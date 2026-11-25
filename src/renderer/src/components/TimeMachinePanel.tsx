import { useEffect, useMemo, useState } from 'react'
import type { ConsumerGroupDetail } from '@shared/types'
import type { GroupObservation } from '../lib/observability'
import {
  explainFlight, formatDuration, simulateRecovery, type FlightSnapshot
} from '../lib/timeMachine'
import { formatCount, MetricCard } from './Observability'

interface Props {
  detail: ConsumerGroupDetail
  history: FlightSnapshot[]
  observation: GroupObservation
}

function formatRate(value: number | null): string {
  return value === null ? 'learning' : `${value.toFixed(1)}/s`
}

export default function TimeMachinePanel({ detail, history, observation }: Props): React.JSX.Element {
  const partitionCount = detail.offsets.length
  const [selectedIndex, setSelectedIndex] = useState(Math.max(0, history.length - 1))
  const [consumers, setConsumers] = useState(Math.max(1, detail.memberCount))
  const [throughputMultiplier, setThroughputMultiplier] = useState(1)
  const [inputMultiplier, setInputMultiplier] = useState(1)

  useEffect(() => setSelectedIndex(Math.max(0, history.length - 1)), [history.length])
  useEffect(() => setConsumers(Math.max(1, detail.memberCount)), [detail.groupId, detail.memberCount])

  const explanation = useMemo(() => explainFlight(history), [history])
  const projection = useMemo(() => simulateRecovery(
    observation,
    detail.memberCount,
    partitionCount,
    { consumers, throughputMultiplier, inputMultiplier }
  ), [observation, detail.memberCount, partitionCount, consumers, throughputMultiplier, inputMultiplier])
  const selected = history[selectedIndex] ?? history.at(-1)

  return (
    <section className="observe-section time-machine">
      <div className="observe-section-title">
        <div>
          <span className="observe-eyebrow">OFFSETDOCK TIME MACHINE</span>
          <h3>Reconstruct, explain, then simulate</h3>
          <p>Privacy-safe offset metadata stays on this device. Simulations never issue Kafka commands.</p>
        </div>
        <span className="observe-local-chip">LOCAL · {history.length} SNAPSHOTS</span>
      </div>

      <div className={`time-machine-explanation ${explanation.severity}`}>
        <div className="time-machine-heading">
          <span className="observe-signal-dot" />
          <strong>{explanation.title}</strong>
          <span>{explanation.severity}</span>
        </div>
        <p>{explanation.summary}</p>
        {explanation.evidence.length > 0 && (
          <ul>{explanation.evidence.map((item) => <li key={item}>{item}</li>)}</ul>
        )}
      </div>

      <div className="time-machine-scrubber">
        <div className="time-machine-scrubber-label">
          <span>Recorded state</span>
          <b>{selected ? new Date(selected.capturedAt).toLocaleTimeString() : 'waiting for baseline'}</b>
        </div>
        <input
          aria-label="Recorded snapshot"
          type="range"
          min={0}
          max={Math.max(0, history.length - 1)}
          value={Math.min(selectedIndex, Math.max(0, history.length - 1))}
          disabled={history.length < 2}
          onChange={(event) => setSelectedIndex(Number(event.target.value))}
        />
        <div className="time-machine-state">
          <span><small>Total lag</small><b>{selected ? formatCount(selected.totalLag) : 'n/a'}</b></span>
          <span><small>Members</small><b>{selected?.memberCount ?? 'n/a'}</b></span>
          <span><small>State</small><b>{selected?.state ?? 'n/a'}</b></span>
          <span><small>Worst partition</small><b>{selected ? formatCount(selected.worstPartitionLag) : 'n/a'}</b></span>
        </div>
      </div>

      <div className="time-machine-simulator">
        <div className="time-machine-controls">
          <div className="observe-section-title">
            <div><h3>Counterfactual recovery</h3><p>Change assumptions to test a recovery plan before touching production.</p></div>
          </div>
          <label>
            <span>Requested consumers <b>{consumers}</b></span>
            <input type="range" min={0} max={Math.max(8, partitionCount * 2)} value={consumers} onChange={(event) => setConsumers(Number(event.target.value))} />
          </label>
          <label>
            <span>Per-consumer throughput <b>{throughputMultiplier.toFixed(1)}×</b></span>
            <input type="range" min={0.25} max={3} step={0.25} value={throughputMultiplier} onChange={(event) => setThroughputMultiplier(Number(event.target.value))} />
          </label>
          <label>
            <span>Incoming traffic <b>{inputMultiplier.toFixed(1)}×</b></span>
            <input type="range" min={0} max={3} step={0.25} value={inputMultiplier} onChange={(event) => setInputMultiplier(Number(event.target.value))} />
          </label>
        </div>
        <div className="time-machine-projection">
          <div className="observe-metrics">
            <MetricCard label="Active consumers" value={projection.activeConsumers.toString()} detail={`${partitionCount} assignable partitions`} tone={projection.constrainedByPartitions ? 'warning' : undefined} />
            <MetricCard label="Projected input" value={formatRate(projection.projectedInputRate)} detail={`${inputMultiplier.toFixed(1)}× observed`} />
            <MetricCard label="Projected processing" value={formatRate(projection.projectedProcessingRate)} detail={`${throughputMultiplier.toFixed(1)}× per consumer`} />
            <MetricCard label="Net drain" value={formatRate(projection.netDrainRate)} detail="processing minus input" tone={projection.netDrainRate !== null && projection.netDrainRate <= 0 ? 'critical' : 'healthy'} />
            <MetricCard label="Catch-up ETA" value={projection.catchUpSeconds === null ? 'not clearing' : formatDuration(projection.catchUpSeconds)} detail={formatCount(observation.totalLag) + ' current lag'} tone={projection.catchUpSeconds === null && observation.totalLag > 0n ? 'critical' : 'healthy'} />
          </div>
          <p className={`time-machine-verdict${projection.catchUpSeconds === null && observation.totalLag > 0n ? ' critical' : ''}`}>{projection.verdict}</p>
        </div>
      </div>
    </section>
  )
}
