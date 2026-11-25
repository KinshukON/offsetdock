import type { ConsumerGroupDetail } from '@shared/types'
import type { GroupObservation, GroupSnapshot, SignalSeverity } from './observability'

const STORAGE_PREFIX = 'offsetdock:time-machine:v1'
export const MAX_FLIGHT_SNAPSHOTS = 180

export interface FlightSnapshot extends GroupSnapshot {
  memberCount: number
  state: string
  partitionCount: number
  worstPartitionLag: bigint
}

interface SerializedFlightSnapshot {
  capturedAt: number
  totalLag: string
  totalEndOffset: string
  memberCount: number
  state: string
  partitionCount: number
  worstPartitionLag: string
}

export interface IncidentExplanation {
  severity: SignalSeverity
  title: string
  summary: string
  evidence: string[]
}

export interface RecoveryScenario {
  consumers: number
  throughputMultiplier: number
  inputMultiplier: number
}

export interface RecoveryProjection {
  activeConsumers: number
  projectedInputRate: number | null
  projectedProcessingRate: number | null
  netDrainRate: number | null
  catchUpSeconds: number | null
  constrainedByPartitions: boolean
  verdict: string
}

function safeBigInt(value: string): bigint {
  return /^-?\d+$/.test(value) ? BigInt(value) : 0n
}

export function flightSnapshot(detail: ConsumerGroupDetail, capturedAt = Date.now()): FlightSnapshot {
  const lags = detail.offsets.map((row) => safeBigInt(row.lag))
  return {
    capturedAt,
    totalLag: lags.reduce((sum, lag) => sum + lag, 0n),
    totalEndOffset: detail.offsets.reduce((sum, row) => sum + safeBigInt(row.latestOffset), 0n),
    memberCount: detail.memberCount,
    state: detail.state,
    partitionCount: detail.offsets.length,
    worstPartitionLag: lags.reduce((worst, lag) => lag > worst ? lag : worst, 0n)
  }
}

export function flightStorageKey(profileId: string, groupId: string): string {
  return `${STORAGE_PREFIX}:${encodeURIComponent(profileId)}:${encodeURIComponent(groupId)}`
}

export function loadFlightHistory(storage: Pick<Storage, 'getItem'>, key: string): FlightSnapshot[] {
  try {
    const raw = storage.getItem(key)
    if (!raw) return []
    const parsed = JSON.parse(raw) as SerializedFlightSnapshot[]
    if (!Array.isArray(parsed)) return []
    return parsed.flatMap((item) => {
      if (!Number.isFinite(item.capturedAt) || typeof item.totalLag !== 'string') return []
      return [{
        capturedAt: item.capturedAt,
        totalLag: safeBigInt(item.totalLag),
        totalEndOffset: safeBigInt(item.totalEndOffset),
        memberCount: Number.isFinite(item.memberCount) ? item.memberCount : 0,
        state: typeof item.state === 'string' ? item.state : 'Unknown',
        partitionCount: Number.isFinite(item.partitionCount) ? item.partitionCount : 0,
        worstPartitionLag: safeBigInt(item.worstPartitionLag)
      }]
    }).slice(-MAX_FLIGHT_SNAPSHOTS)
  } catch {
    return []
  }
}

export function saveFlightHistory(storage: Pick<Storage, 'setItem'>, key: string, history: FlightSnapshot[]): void {
  const serialized: SerializedFlightSnapshot[] = history.slice(-MAX_FLIGHT_SNAPSHOTS).map((snapshot) => ({
    capturedAt: snapshot.capturedAt,
    totalLag: snapshot.totalLag.toString(),
    totalEndOffset: snapshot.totalEndOffset.toString(),
    memberCount: snapshot.memberCount,
    state: snapshot.state,
    partitionCount: snapshot.partitionCount,
    worstPartitionLag: snapshot.worstPartitionLag.toString()
  }))
  try {
    storage.setItem(key, JSON.stringify(serialized))
  } catch {
    // Observability must remain usable when local storage is unavailable or full.
  }
}

export function appendFlightSnapshot(history: FlightSnapshot[], snapshot: FlightSnapshot): FlightSnapshot[] {
  const latest = history.at(-1)
  if (latest && snapshot.capturedAt - latest.capturedAt < 1_000) return history
  return [...history, snapshot].slice(-MAX_FLIGHT_SNAPSHOTS)
}

export function explainFlight(history: FlightSnapshot[]): IncidentExplanation {
  const latest = history.at(-1)
  const previous = history.at(-2)
  if (!latest || !previous) {
    return {
      severity: 'info',
      title: 'Establishing an evidence baseline',
      summary: 'A second snapshot is needed before OffsetDock can explain what changed.',
      evidence: latest ? [`Baseline captured with ${latest.totalLag.toString()} total lag.`] : []
    }
  }

  const seconds = Math.max(0.001, (latest.capturedAt - previous.capturedAt) / 1_000)
  const lagDelta = latest.totalLag - previous.totalLag
  const endDelta = latest.totalEndOffset - previous.totalEndOffset
  const inputRate = Number(endDelta) / seconds
  const evidence = [
    `Lag ${lagDelta > 0n ? 'increased' : lagDelta < 0n ? 'decreased' : 'held'} by ${lagDelta < 0n ? (-lagDelta).toString() : lagDelta.toString()} records over ${seconds.toFixed(1)} seconds.`,
    `The log end advanced by ${endDelta.toString()} records (${inputRate.toFixed(1)}/s).`
  ]

  if (latest.memberCount !== previous.memberCount) {
    evidence.push(`Active membership changed from ${previous.memberCount} to ${latest.memberCount}.`)
  }
  if (latest.state !== previous.state) {
    evidence.push(`Kafka group state changed from ${previous.state} to ${latest.state}.`)
  }
  if (latest.worstPartitionLag > 0n && latest.totalLag > 0n) {
    const share = Number((latest.worstPartitionLag * 1000n) / latest.totalLag) / 10
    evidence.push(`The worst partition holds ${share.toFixed(1)}% of total lag.`)
  }

  if (latest.memberCount === 0 && latest.totalLag > 0n) {
    return {
      severity: 'critical', title: 'Backlog has no active recovery path',
      summary: 'Lag remains, but no consumer is currently assigned to process it.', evidence
    }
  }
  if (lagDelta > 0n && latest.memberCount < previous.memberCount) {
    return {
      severity: 'critical', title: 'Capacity loss coincides with rising lag',
      summary: 'Consumer membership fell during the same interval in which the backlog grew.', evidence
    }
  }
  if (lagDelta > 0n && endDelta > 0n) {
    return {
      severity: 'warning', title: 'Incoming traffic is outrunning consumers',
      summary: 'The backlog grew while producers continued advancing the log end.', evidence
    }
  }
  if (lagDelta < 0n) {
    return {
      severity: 'healthy', title: 'Recovery is supported by measured drain',
      summary: 'Consumers reduced the backlog during the latest recorded interval.', evidence
    }
  }
  return {
    severity: latest.totalLag > 0n ? 'info' : 'healthy',
    title: latest.totalLag > 0n ? 'Backlog is stable' : 'Group is caught up',
    summary: latest.totalLag > 0n ? 'Lag did not materially change in the latest interval.' : 'No outstanding lag is recorded.',
    evidence
  }
}

export function simulateRecovery(
  observation: GroupObservation,
  currentMembers: number,
  partitionCount: number,
  scenario: RecoveryScenario
): RecoveryProjection {
  const requestedConsumers = Math.max(0, Math.floor(scenario.consumers))
  const activeConsumers = Math.min(requestedConsumers, Math.max(0, partitionCount))
  const constrainedByPartitions = requestedConsumers > partitionCount
  const projectedInputRate = observation.inputRate === null
    ? null
    : observation.inputRate * Math.max(0, scenario.inputMultiplier)
  const perConsumerRate = observation.processingRate === null || currentMembers <= 0
    ? null
    : observation.processingRate / currentMembers
  const projectedProcessingRate = perConsumerRate === null
    ? null
    : perConsumerRate * activeConsumers * Math.max(0, scenario.throughputMultiplier)
  const netDrainRate = projectedInputRate === null || projectedProcessingRate === null
    ? null
    : projectedProcessingRate - projectedInputRate
  const catchUpSeconds = netDrainRate !== null && netDrainRate > 0
    ? Number(observation.totalLag) / netDrainRate
    : observation.totalLag === 0n ? 0 : null

  let verdict = 'Record at least two active-consumer snapshots to project recovery.'
  if (observation.totalLag === 0n) verdict = 'There is no current backlog to recover.'
  else if (projectedProcessingRate !== null && activeConsumers === 0) verdict = 'No recovery is possible with zero active consumers.'
  else if (netDrainRate !== null && netDrainRate <= 0) verdict = 'This scenario does not process faster than incoming traffic.'
  else if (catchUpSeconds !== null) verdict = `This scenario clears the measured backlog in about ${formatDuration(catchUpSeconds)}.`
  if (constrainedByPartitions) verdict += ` Only ${partitionCount} consumers can receive assignments.`

  return {
    activeConsumers, projectedInputRate, projectedProcessingRate, netDrainRate,
    catchUpSeconds, constrainedByPartitions, verdict
  }
}

export function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds)) return 'unknown'
  if (seconds < 60) return `${Math.max(0, Math.ceil(seconds))}s`
  if (seconds < 3_600) return `${Math.ceil(seconds / 60)}m`
  return `${(seconds / 3_600).toFixed(1)}h`
}
