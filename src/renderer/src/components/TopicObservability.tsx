import { useMemo } from 'react'
import { useStore } from '../state/store'
import { observeTopic } from '../lib/observability'
import { EvidenceLens, formatAge, formatBytes, MetricCard } from './Observability'

export default function TopicObservability(): React.JSX.Element {
  const topicDetail = useStore((state) => state.topicDetail)
  const fetchResult = useStore((state) => state.fetchResult)
  const fetchMessages = useStore((state) => state.fetchMessages)
  const fetching = useStore((state) => state.fetching)
  const observation = useMemo(
    () => observeTopic(fetchResult?.messages ?? [], topicDetail),
    [fetchResult, topicDetail]
  )
  const formats = Object.entries(observation.formatCounts).sort((a, b) => b[1] - a[1])
  const partitions = Object.entries(observation.partitionCounts)
  const maxPartitionCount = Math.max(1, ...partitions.map(([, count]) => count))
  const replicationTone = observation.offlinePartitions > 0
    ? 'critical'
    : observation.underReplicated > 0 ? 'warning' : 'healthy'

  return (
    <div className="observe-view">
      <div className="observe-hero">
        <div>
          <span className="observe-eyebrow">TOPIC TELEMETRY</span>
          <h3>What is happening in this stream?</h3>
          <p>OffsetDock derives operational signals from the fetched record sample and Kafka metadata. No telemetry leaves this device.</p>
        </div>
        <button className="btn small" disabled={fetching} onClick={() => void fetchMessages()}>
          {fetching ? <><span className="spin" /> Sampling…</> : 'Refresh sample'}
        </button>
      </div>

      <div className="observe-metrics">
        <MetricCard label="Sample" value={observation.sampleSize.toLocaleString()} detail={formatBytes(observation.sampleBytes)} />
        <MetricCard label="Observed rate" value={observation.eventsPerSecond === null ? 'n/a' : `${observation.eventsPerSecond.toFixed(1)}/s`} detail="within sample window" />
        <MetricCard label="Payload p95" value={formatBytes(observation.payloadP95)} detail={`p50 ${formatBytes(observation.payloadP50)}`} />
        <MetricCard label="Newest record" value={formatAge(observation.newestRecordAgeMs)} detail="age at analysis time" />
        <MetricCard label="Trace context" value={`${observation.traceHeaderPercent.toFixed(0)}%`} detail="sampled records" tone={observation.traceHeaderPercent < 20 && observation.sampleSize >= 10 ? 'info' : undefined} />
        <MetricCard label="Replication" value={observation.offlinePartitions > 0 ? `${observation.offlinePartitions} offline` : observation.underReplicated > 0 ? `${observation.underReplicated} under ISR` : 'healthy'} detail={`${topicDetail?.partitionCount ?? 0} partitions`} tone={replicationTone} />
      </div>

      <div className="observe-columns">
        <section className="observe-section">
          <div className="observe-section-title"><div><h3>Wire-format fingerprint</h3><p>Detected without changing the original bytes.</p></div></div>
          {formats.length === 0 ? <div className="observe-empty">Fetch a record sample to fingerprint the stream.</div> : (
            <div className="observe-bars">
              {formats.map(([format, count]) => (
                <div className="observe-bar-row" key={format}>
                  <span>{format}</span>
                  <div className="observe-bar-track"><i style={{ width: `${(count / observation.sampleSize) * 100}%` }} /></div>
                  <b>{count}</b>
                </div>
              ))}
            </div>
          )}
        </section>
        <section className="observe-section">
          <div className="observe-section-title"><div><h3>Partition heat</h3><p>Record distribution inside the current sample.</p></div><span className="observe-ratio">{observation.partitionSkew.toFixed(1)}× skew</span></div>
          {partitions.length === 0 ? <div className="observe-empty">Partition activity appears after sampling.</div> : (
            <div className="observe-partitions">
              {partitions.map(([partition, count]) => (
                <div className="observe-partition" key={partition} title={`Partition ${partition}: ${count} sampled records`}>
                  <i style={{ height: `${Math.max(4, (count / maxPartitionCount) * 100)}%` }} />
                  <span>P{partition}</span><b>{count}</b>
                </div>
              ))}
            </div>
          )}
        </section>
      </div>
      <EvidenceLens signals={observation.signals} />
      <p className="observe-footnote">Sample-derived rates and distributions are directional. Pair them with broker metrics for capacity decisions.</p>
    </div>
  )
}
