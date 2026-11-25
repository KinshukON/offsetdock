import type { ConsumerGroupDetail, KafkaMessage, TopicDetail } from '@shared/types'

export type SignalSeverity = 'healthy' | 'info' | 'warning' | 'critical'

export interface EvidenceSignal {
  id: string
  severity: SignalSeverity
  title: string
  evidence: string
  impact: string
  action: string
}

export interface TopicObservation {
  sampleSize: number
  sampleBytes: number
  eventsPerSecond: number | null
  newestRecordAgeMs: number | null
  payloadP50: number
  payloadP95: number
  tombstonePercent: number
  traceHeaderPercent: number
  partitionSkew: number
  underReplicated: number
  offlinePartitions: number
  formatCounts: Record<string, number>
  partitionCounts: Record<number, number>
  signals: EvidenceSignal[]
}

export interface GroupSnapshot {
  capturedAt: number
  totalLag: bigint
  totalEndOffset: bigint
}

export interface GroupObservation {
  totalLag: bigint
  worstPartitionLag: bigint
  laggingPartitions: number
  partitionSkew: number
  lagDelta: bigint | null
  endOffsetDelta: bigint | null
  inputRate: number | null
  processingRate: number | null
  netDrainRate: number | null
  catchUpSeconds: number | null
  signals: EvidenceSignal[]
}

function percentile(values: number[], fraction: number): number {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)] ?? 0
}

function percent(part: number, total: number): number {
  return total === 0 ? 0 : (part / total) * 100
}

function safeBigInt(value: string): bigint {
  return /^-?\d+$/.test(value) ? BigInt(value) : 0n
}

function bytesFromBase64(value: string): Uint8Array {
  const binary = atob(value)
  return Uint8Array.from(binary, (character) => character.charCodeAt(0))
}

export function detectObservedFormat(message: KafkaMessage): string {
  if (message.value === null) return 'tombstone'
  const bytes = bytesFromBase64(message.value)
  if (bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b) return 'gzip'
  if (bytes.length >= 5 && bytes[0] === 0) return 'schema-framed'

  const text = new TextDecoder('utf-8', { fatal: false }).decode(bytes).trim()
  if (text.startsWith('{') || text.startsWith('[')) {
    try {
      const parsed = JSON.parse(text) as Record<string, unknown>
      if (
        parsed &&
        !Array.isArray(parsed) &&
        typeof parsed === 'object' &&
        typeof parsed.specversion === 'string' &&
        typeof parsed.type === 'string' &&
        typeof parsed.source === 'string'
      ) {
        return 'cloudevent'
      }
      return 'json'
    } catch {
      return 'text'
    }
  }
  if (text.startsWith('<') && text.endsWith('>')) return 'xml'
  const printable = bytes.filter((byte) => byte === 9 || byte === 10 || byte === 13 || (byte >= 32 && byte < 127))
  return bytes.length === 0 || printable.length / bytes.length >= 0.9 ? 'text' : 'binary/encoded'
}

export function observeTopic(
  messages: KafkaMessage[],
  detail: TopicDetail | null,
  now = Date.now()
): TopicObservation {
  const sizes = messages.map((message) => message.valueSize)
  const timestamps = messages
    .map((message) => Date.parse(message.timestamp))
    .filter((timestamp) => Number.isFinite(timestamp))
  const oldest = timestamps.length ? Math.min(...timestamps) : null
  const newest = timestamps.length ? Math.max(...timestamps) : null
  const windowSeconds = oldest !== null && newest !== null ? (newest - oldest) / 1000 : 0
  const byPartition = new Map<number, number>()
  const formatCounts: Record<string, number> = {}
  let tombstones = 0
  let traced = 0

  for (const message of messages) {
    byPartition.set(message.partition, (byPartition.get(message.partition) ?? 0) + 1)
    const format = detectObservedFormat(message)
    formatCounts[format] = (formatCounts[format] ?? 0) + 1
    if (message.value === null) tombstones += 1
    if (message.headers.some((header) => /^(traceparent|tracestate|correlation[-_]?id)$/i.test(header.key))) {
      traced += 1
    }
  }

  if (detail) {
    for (const partition of detail.partitions) {
      if (!byPartition.has(partition.partition)) byPartition.set(partition.partition, 0)
    }
  }
  const observedPartitionCounts = [...byPartition.values()]
  const average = observedPartitionCounts.length
    ? observedPartitionCounts.reduce((sum, count) => sum + count, 0) / observedPartitionCounts.length
    : 0
  const partitionSkew = average ? Math.max(...observedPartitionCounts) / average : 0
  const underReplicated = detail?.partitions.filter((partition) => partition.isr.length < partition.replicas.length).length ?? 0
  const offlinePartitions = detail?.partitions.filter((partition) => partition.leader < 0).length ?? 0
  const tombstonePercent = percent(tombstones, messages.length)
  const traceHeaderPercent = percent(traced, messages.length)
  const payloadP95 = percentile(sizes, 0.95)
  const signals: EvidenceSignal[] = []

  if (offlinePartitions > 0) {
    signals.push({
      id: 'offline-partitions', severity: 'critical', title: 'Partition availability risk',
      evidence: `${offlinePartitions} partition${offlinePartitions === 1 ? '' : 's'} currently have no leader.`,
      impact: 'Reads and writes for those partitions can fail until leadership is restored.',
      action: 'Inspect broker health and replica placement, then restore an in-sync leader.'
    })
  }
  if (underReplicated > 0) {
    signals.push({
      id: 'under-replicated', severity: 'warning', title: 'Replication safety is reduced',
      evidence: `${underReplicated} partition${underReplicated === 1 ? '' : 's'} have fewer in-sync replicas than configured replicas.`,
      impact: 'Another broker failure could reduce availability or durability.',
      action: 'Check broker reachability, replica lag, disk pressure, and ISR recovery.'
    })
  }
  if (messages.length >= 20 && partitionSkew >= 2) {
    signals.push({
      id: 'partition-skew', severity: 'warning', title: 'Sample is partition-skewed',
      evidence: `The busiest sampled partition carries ${partitionSkew.toFixed(1)}× the per-partition average.`,
      impact: 'Hot partitions can cap throughput even when the cluster has spare capacity.',
      action: 'Review record keys and partitioner behavior, then compare broker-level load.'
    })
  }
  if (messages.length >= 10 && traceHeaderPercent < 20) {
    signals.push({
      id: 'trace-coverage', severity: 'info', title: 'Low trace-context coverage',
      evidence: `${traceHeaderPercent.toFixed(0)}% of sampled records include traceparent, tracestate, or a correlation ID header.`,
      impact: 'Cross-system incident reconstruction will depend on payload searches and timestamps.',
      action: 'Propagate W3C trace context or a stable correlation ID in message headers.'
    })
  }
  if (payloadP95 >= 1_000_000) {
    signals.push({
      id: 'large-payloads', severity: 'warning', title: 'Large payload tail',
      evidence: `The sampled p95 payload is ${(payloadP95 / 1_000_000).toFixed(2)} MB.`,
      impact: 'Large records increase broker memory pressure, network cost, and consumer latency.',
      action: 'Move large blobs out of Kafka or review producer, broker, and consumer size limits.'
    })
  }
  if (messages.length > 0 && signals.length === 0) {
    signals.push({
      id: 'sample-healthy', severity: 'healthy', title: 'No immediate risk found in this sample',
      evidence: `${messages.length} records and ${detail?.partitionCount ?? byPartition.size} partitions were evaluated.`,
      impact: 'This is a sample-level signal, not a replacement for broker metrics.',
      action: 'Keep the Observe view open during load changes and compare with infrastructure telemetry.'
    })
  }

  return {
    sampleSize: messages.length,
    sampleBytes: sizes.reduce((sum, size) => sum + size, 0),
    eventsPerSecond: windowSeconds > 0 ? messages.length / windowSeconds : null,
    newestRecordAgeMs: newest === null ? null : Math.max(0, now - newest),
    payloadP50: percentile(sizes, 0.5),
    payloadP95,
    tombstonePercent,
    traceHeaderPercent,
    partitionSkew,
    underReplicated,
    offlinePartitions,
    formatCounts,
    partitionCounts: Object.fromEntries([...byPartition.entries()].sort(([a], [b]) => a - b)),
    signals
  }
}

export function groupSnapshot(detail: ConsumerGroupDetail, capturedAt = Date.now()): GroupSnapshot {
  return {
    capturedAt,
    totalLag: detail.offsets.reduce((sum, row) => sum + safeBigInt(row.lag), 0n),
    totalEndOffset: detail.offsets.reduce((sum, row) => sum + safeBigInt(row.latestOffset), 0n)
  }
}

export function observeGroup(detail: ConsumerGroupDetail, history: GroupSnapshot[]): GroupObservation {
  const lags = detail.offsets.map((row) => safeBigInt(row.lag))
  const totalLag = lags.reduce((sum, lag) => sum + lag, 0n)
  const worstPartitionLag = lags.reduce((worst, lag) => lag > worst ? lag : worst, 0n)
  const laggingPartitions = lags.filter((lag) => lag > 0n).length
  const average = lags.length ? Number(totalLag) / lags.length : 0
  const partitionSkew = average ? Number(worstPartitionLag) / average : 0
  const previous = history.length > 1 ? history.at(-2)! : null
  const latest = history.at(-1) ?? null
  const lagDelta = previous && latest ? latest.totalLag - previous.totalLag : null
  const endOffsetDelta = previous && latest ? latest.totalEndOffset - previous.totalEndOffset : null
  const intervalSeconds = previous && latest ? (latest.capturedAt - previous.capturedAt) / 1000 : 0
  const inputRate = endOffsetDelta !== null && intervalSeconds > 0 ? Number(endOffsetDelta) / intervalSeconds : null
  const processedDelta = endOffsetDelta !== null && lagDelta !== null ? endOffsetDelta - lagDelta : null
  const processingRate = processedDelta !== null && intervalSeconds > 0
    ? Math.max(0, Number(processedDelta) / intervalSeconds)
    : null
  const netDrainRate = lagDelta !== null && intervalSeconds > 0 ? Number(-lagDelta) / intervalSeconds : null
  const catchUpSeconds = netDrainRate !== null && netDrainRate > 0
    ? Number(totalLag) / netDrainRate
    : null
  const signals: EvidenceSignal[] = []

  if (detail.state.toLowerCase() !== 'stable' && detail.memberCount > 0) {
    signals.push({
      id: 'group-state', severity: 'warning', title: 'Consumer group is not stable',
      evidence: `Kafka reports state “${detail.state}” with ${detail.memberCount} active member${detail.memberCount === 1 ? '' : 's'}.`,
      impact: 'Rebalances pause useful work and can amplify processing latency.',
      action: 'Inspect member churn, session timeouts, assignment duration, and deployment activity.'
    })
  }
  if (totalLag > 0n && detail.memberCount === 0) {
    signals.push({
      id: 'orphaned-lag', severity: 'critical', title: 'Lag exists with no active consumers',
      evidence: `${totalLag.toString()} records are outstanding while the group has zero members.`,
      impact: 'The backlog cannot recover until a consumer rejoins.',
      action: 'Restore the consumer workload or confirm that the group is intentionally retired.'
    })
  }
  if (lags.length >= 3 && partitionSkew >= 2) {
    signals.push({
      id: 'lag-skew', severity: 'warning', title: 'Lag is concentrated',
      evidence: `The worst partition holds ${partitionSkew.toFixed(1)}× the average partition lag.`,
      impact: 'One hot or blocked partition can dominate end-to-end freshness.',
      action: 'Inspect the worst partition for key skew, poison records, or a slow assigned member.'
    })
  }
  if (lagDelta !== null && endOffsetDelta !== null && lagDelta >= 0n && endOffsetDelta > 0n) {
    signals.push({
      id: 'falling-behind', severity: 'critical', title: 'Consumers are not catching incoming traffic',
      evidence: `Across the latest observation interval, log end advanced by ${endOffsetDelta.toString()} while lag changed by ${lagDelta.toString()}.`,
      impact: 'End-to-end data freshness will continue to degrade at the current balance.',
      action: 'Check processing latency and errors, then scale consumers up to the partition count if appropriate.'
    })
  } else if (lagDelta !== null && lagDelta < 0n) {
    signals.push({
      id: 'recovering', severity: 'healthy', title: 'The backlog is recovering',
      evidence: `Total lag fell by ${(-lagDelta).toString()} records in the latest observation interval.`,
      impact: 'Consumers are currently reducing the accumulated backlog.',
      action: 'Keep the recorder running until lag reaches the expected steady-state range.'
    })
  }
  if (inputRate !== null && processingRate !== null && inputRate > 0 && processingRate === 0) {
    signals.push({
      id: 'no-throughput', severity: 'critical', title: 'Traffic is arriving but processing is stalled',
      evidence: `The log advanced at ${inputRate.toFixed(1)} records/s while inferred consumer throughput was 0 records/s.`,
      impact: 'Every new record is adding directly to the backlog.',
      action: 'Inspect consumer errors, poison records, paused assignments, and downstream dependencies.'
    })
  }
  if (signals.length === 0) {
    signals.push({
      id: 'group-healthy', severity: 'healthy', title: 'No immediate consumer risk found',
      evidence: `${detail.offsets.length} committed partition offsets were evaluated.`,
      impact: 'Trend confidence improves after at least two recorder samples.',
      action: 'Keep the Observe tab open briefly to establish direction, not just a snapshot.'
    })
  }

  return {
    totalLag, worstPartitionLag, laggingPartitions, partitionSkew, lagDelta, endOffsetDelta,
    inputRate, processingRate, netDrainRate, catchUpSeconds, signals
  }
}
