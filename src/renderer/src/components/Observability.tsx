import type { EvidenceSignal, SignalSeverity } from '../lib/observability'

export function formatCount(value: bigint | number): string {
  return value.toLocaleString('en-US')
}

export function formatBytes(value: number): string {
  if (value < 1_000) return `${value} B`
  if (value < 1_000_000) return `${(value / 1_000).toFixed(value < 10_000 ? 1 : 0)} KB`
  return `${(value / 1_000_000).toFixed(1)} MB`
}

export function formatAge(value: number | null): string {
  if (value === null) return 'no sample'
  if (value < 1_000) return 'just now'
  if (value < 60_000) return `${Math.floor(value / 1_000)}s`
  if (value < 3_600_000) return `${Math.floor(value / 60_000)}m`
  return `${Math.floor(value / 3_600_000)}h`
}

export function MetricCard({ label, value, detail, tone }: {
  label: string
  value: string
  detail: string
  tone?: SignalSeverity
}): React.JSX.Element {
  return (
    <div className={`observe-metric${tone ? ` ${tone}` : ''}`}>
      <span className="observe-metric-label">{label}</span>
      <strong>{value}</strong>
      <span className="observe-metric-detail">{detail}</span>
    </div>
  )
}

export function EvidenceLens({ signals }: { signals: EvidenceSignal[] }): React.JSX.Element {
  return (
    <section className="observe-section">
      <div className="observe-section-title">
        <div>
          <h3>Evidence Lens</h3>
          <p>Explainable signals built from the values visible in this session.</p>
        </div>
        <span className="observe-local-chip">LOCAL ANALYSIS</span>
      </div>
      <div className="observe-signals">
        {signals.map((signal) => (
          <article className={`observe-signal ${signal.severity}`} key={signal.id}>
            <div className="observe-signal-heading">
              <span className="observe-signal-dot" />
              <strong>{signal.title}</strong>
              <span className="observe-severity">{signal.severity}</span>
            </div>
            <dl>
              <div><dt>Evidence</dt><dd>{signal.evidence}</dd></div>
              <div><dt>Why it matters</dt><dd>{signal.impact}</dd></div>
              <div><dt>Next action</dt><dd>{signal.action}</dd></div>
            </dl>
          </article>
        ))}
      </div>
    </section>
  )
}
