import { useEffect, useMemo, useState } from 'react'
import type { ConsumerGroupDetail } from '@shared/types'
import { observeGroup } from '../lib/observability'
import {
  appendFlightSnapshot, flightSnapshot, flightStorageKey, loadFlightHistory,
  saveFlightHistory, type FlightSnapshot
} from '../lib/timeMachine'
import { useStore } from '../state/store'
import { EvidenceLens, formatCount, MetricCard } from './Observability'
import TimeMachinePanel from './TimeMachinePanel'

const POLL_MS = 10_000

export default function GroupObservability({ detail }: { detail: ConsumerGroupDetail }): React.JSX.Element {
  const refreshSelection = useStore((state) => state.refreshSelection)
  const detailLoading = useStore((state) => state.detailLoading)
  const activeProfileId = useStore((state) => state.activeProfileId)
  const storageKey = useMemo(
    () => flightStorageKey(activeProfileId ?? 'unknown', detail.groupId),
    [activeProfileId, detail.groupId]
  )
  const [history, setHistory] = useState<FlightSnapshot[]>([])
  const [recording, setRecording] = useState(true)

  useEffect(() => {
    const loaded = loadFlightHistory(window.localStorage, storageKey)
    const next = appendFlightSnapshot(loaded, flightSnapshot(detail))
    saveFlightHistory(window.localStorage, storageKey, next)
    setHistory(next)
  }, [storageKey])

  useEffect(() => {
    const snapshot = flightSnapshot(detail)
    setHistory((current) => {
      const next = appendFlightSnapshot(current, snapshot)
      if (next !== current) saveFlightHistory(window.localStorage, storageKey, next)
      return next
    })
  }, [detail, storageKey])

  useEffect(() => {
    if (!recording) return
    const timer = window.setInterval(() => void refreshSelection(), POLL_MS)
    return () => window.clearInterval(timer)
  }, [recording, refreshSelection])

  const observation = useMemo(() => observeGroup(detail, history), [detail, history])
  const maxLag = history.reduce((max, snapshot) => snapshot.totalLag > max ? snapshot.totalLag : max, 1n)
  const trend = observation.lagDelta === null ? 'establishing baseline'
    : observation.lagDelta < 0n ? `down ${formatCount(-observation.lagDelta)}`
      : observation.lagDelta > 0n ? `up ${formatCount(observation.lagDelta)}` : 'flat'
  const catchUp = observation.totalLag === 0n ? 'clear'
    : observation.catchUpSeconds === null ? 'not clearing'
      : observation.catchUpSeconds < 60 ? `${Math.ceil(observation.catchUpSeconds)}s`
        : observation.catchUpSeconds < 3_600 ? `${Math.ceil(observation.catchUpSeconds / 60)}m`
          : `${(observation.catchUpSeconds / 3_600).toFixed(1)}h`

  return (
    <div className="observe-view">
      <div className="observe-hero">
        <div>
          <span className="observe-eyebrow">CONSUMER FLIGHT RECORDER</span>
          <h3>Is this group keeping up?</h3>
          <p>A local lag history separates a backlog snapshot from a worsening trend and survives navigation. Payloads are never recorded.</p>
        </div>
        <button className={`btn small${recording ? '' : ' ghost'}`} onClick={() => setRecording((value) => !value)}>
          <span className={`observe-live-dot${recording ? ' active' : ''}`} />
          {recording ? 'Recording every 10s' : 'Resume recorder'}
        </button>
      </div>

      <div className="observe-metrics">
        <MetricCard label="Total lag" value={formatCount(observation.totalLag)} detail={trend} tone={observation.totalLag > 0n ? 'warning' : 'healthy'} />
        <MetricCard label="Input rate" value={observation.inputRate === null ? 'learning' : `${observation.inputRate.toFixed(1)}/s`} detail="inferred from log end" />
        <MetricCard label="Consumer rate" value={observation.processingRate === null ? 'learning' : `${observation.processingRate.toFixed(1)}/s`} detail="inferred from lag balance" />
        <MetricCard label="Catch-up ETA" value={catchUp} detail={observation.netDrainRate === null ? 'needs two samples' : `${observation.netDrainRate.toFixed(1)}/s net drain`} tone={observation.totalLag > 0n && observation.catchUpSeconds === null ? 'critical' : undefined} />
        <MetricCard label="Worst partition" value={formatCount(observation.worstPartitionLag)} detail={`${observation.laggingPartitions} of ${detail.offsets.length} lagging`} />
        <MetricCard label="Lag skew" value={`${observation.partitionSkew.toFixed(1)}×`} detail="worst versus average" tone={observation.partitionSkew >= 2 ? 'warning' : undefined} />
        <MetricCard label="Group state" value={detail.state} detail={`${detail.memberCount} active members`} tone={detail.state.toLowerCase() === 'stable' ? 'healthy' : 'warning'} />
      </div>

      <section className="observe-section observe-recorder">
        <div className="observe-section-title">
          <div><h3>Lag timeline</h3><p>Recent local observations retained by OffsetDock Time Machine.</p></div>
          <span className="observe-ratio">{trend}</span>
        </div>
        <div className="observe-trend" aria-label="Recent total lag observations">
          {history.map((snapshot) => {
            const percent = Number((snapshot.totalLag * 100n) / maxLag)
            return <i key={snapshot.capturedAt} style={{ height: `${Math.max(3, percent)}%` }} title={`${formatCount(snapshot.totalLag)} lag`} />
          })}
        </div>
        <div className="observe-timeline-labels"><span>older</span><span>now</span></div>
      </section>
      <TimeMachinePanel detail={detail} history={history} observation={observation} />
      <EvidenceLens signals={observation.signals} />
      <p className="observe-footnote">Evidence Lens uses transparent rules, not an opaque health score. Only offset metadata is retained locally, and nothing is uploaded.</p>
    </div>
  )
}
