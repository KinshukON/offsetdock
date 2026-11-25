import { Fragment, useEffect, useMemo, useState } from 'react'
import Editor from '@monaco-editor/react'
import type { DecodeFormat, DecodedPayload } from '@shared/types'
import { useStore } from '../state/store'
import { b64ToUtf8, formatBytes, formatTimestamp, renderPayload } from '../lib/bytes'

const DECODE_FORMATS: { value: DecodeFormat; label: string }[] = [
  { value: 'auto', label: 'Auto detect' },
  { value: 'json', label: 'JSON' },
  { value: 'xml', label: 'XML' },
  { value: 'text', label: 'Text' },
  { value: 'hex', label: 'Hex' },
  { value: 'avro', label: 'Avro + Registry' },
  { value: 'protobuf', label: 'Protobuf + Registry' },
  { value: 'json-schema', label: 'JSON Schema + Registry' },
  { value: 'messagepack', label: 'MessagePack' },
  { value: 'cbor', label: 'CBOR' },
  { value: 'bson', label: 'BSON' },
  { value: 'gzip', label: 'Gzip' }
]

type InspectorTab = 'structured' | 'raw' | 'schema' | 'metadata' | 'diff'

export default function DetailsPanel(): React.JSX.Element {
  const selectedMessage = useStore((s) => s.selectedMessage)
  const activeProfileId = useStore((s) => s.activeProfileId)
  const selection = useStore((s) => s.selection)
  const topicDetail = useStore((s) => s.topicDetail)
  const theme = useStore((s) => s.settings.theme)
  const saveMessagePart = useStore((s) => s.saveMessagePart)
  const [requestedFormat, setRequestedFormat] = useState<DecodeFormat>('auto')
  const [tab, setTab] = useState<InspectorTab>('structured')
  const [showKeyHex, setShowKeyHex] = useState(false)
  const [revealSensitive, setRevealSensitive] = useState(false)
  const [decoded, setDecoded] = useState<DecodedPayload | null>(null)
  const [decodeError, setDecodeError] = useState<string | null>(null)
  const [decoding, setDecoding] = useState(false)
  const [comparison, setComparison] = useState<{ label: string; payload: DecodedPayload } | null>(null)

  const m = selectedMessage
  const rawPayload = useMemo(
    () => (m ? renderPayload(m.value, requestedFormat === 'hex' ? 'hex' : 'auto') : null),
    [m, requestedFormat]
  )
  const keyPayload = useMemo(
    () => (m ? renderPayload(m.key, showKeyHex ? 'hex' : 'auto') : null),
    [m, showKeyHex]
  )

  useEffect(() => {
    if (!m || !activeProfileId) {
      setDecoded(null)
      setDecodeError(null)
      return
    }
    let cancelled = false
    setDecoding(true)
    setDecodeError(null)
    void window.api
      .invoke('messages:decode', {
        profileId: activeProfileId,
        topic: m.topic,
        contentBase64: m.value,
        headers: m.headers,
        requestedFormat,
        redactSensitive: !revealSensitive
      })
      .then((payload) => {
        if (!cancelled) setDecoded(payload)
      })
      .catch((error) => {
        if (!cancelled) setDecodeError(error instanceof Error ? error.message : String(error))
      })
      .finally(() => {
        if (!cancelled) setDecoding(false)
      })
    return () => { cancelled = true }
  }, [activeProfileId, m, requestedFormat, revealSensitive])

  if (!m) {
    return (
      <div className="details">
        {selection.kind === 'topic' && topicDetail ? (
          <section>
            <h3>Topic</h3>
            <dl className="meta-grid">
              <dt>name</dt><dd>{topicDetail.name}</dd>
              <dt>partitions</dt><dd>{topicDetail.partitionCount}</dd>
              <dt>replication</dt><dd>{topicDetail.replicationFactor}</dd>
              <dt>messages</dt><dd>~{topicDetail.messageCount}</dd>
            </dl>
          </section>
        ) : <div className="details-empty">Select a message to inspect it here.</div>}
      </div>
    )
  }

  const editorLanguage = decoded?.format === 'xml' ? 'xml' : decoded?.value !== undefined ? 'json' : 'plaintext'

  return (
    <div className="details" style={{ display: 'flex' }}>
      <section>
        <h3>
          Message
          <span style={{ display: 'flex', gap: 4 }}>
            <button className="btn small ghost" onClick={() => void saveMessagePart('key')} disabled={m.key === null}>save key</button>
            <button className="btn small ghost" onClick={() => void saveMessagePart('value')} disabled={m.value === null}>save value</button>
          </span>
        </h3>
        <dl className="meta-grid">
          <dt>topic</dt><dd>{m.topic}</dd>
          <dt>partition</dt><dd>{m.partition}</dd>
          <dt>offset</dt><dd>{m.offset}</dd>
          <dt>timestamp</dt><dd>{formatTimestamp(m.timestamp)}</dd>
          <dt>key size</dt><dd>{formatBytes(m.keySize)}</dd>
          <dt>value size</dt><dd>{formatBytes(m.valueSize)}</dd>
          <dt>detected</dt><dd>{decoding ? 'detecting…' : decoded?.format ?? 'unknown'}</dd>
          {decoded?.schema?.id !== undefined && <><dt>schema ID</dt><dd>{decoded.schema.id}</dd></>}
        </dl>
      </section>

      <section>
        <h3>Key<button className="btn small ghost" onClick={() => setShowKeyHex((value) => !value)}>{showKeyHex ? 'text' : 'hex'}</button></h3>
        <div className="key-preview" data-null={m.key === null}>{keyPayload?.text}</div>
      </section>

      {m.headers.length > 0 && (
        <section>
          <h3>Headers ({m.headers.length})</h3>
          {m.headers.map((header, index) => (
            <div key={`${header.key}-${index}`} className="header-item"><span className="hk">{header.key}</span><span className="hv">{b64ToUtf8(header.value)}</span></div>
          ))}
        </section>
      )}

      <section className="grow inspector-section">
        <div className="inspector-controls">
          <select className="select compact" value={requestedFormat} onChange={(event) => setRequestedFormat(event.target.value as DecodeFormat)}>
            {DECODE_FORMATS.map((format) => <option key={format.value} value={format.value}>{format.label}</option>)}
          </select>
          <label className="checkbox-row compact" title="Potential secrets are hidden in structured views by default">
            <input type="checkbox" checked={revealSensitive} onChange={(event) => setRevealSensitive(event.target.checked)} /> reveal sensitive
          </label>
          <button className="btn small ghost" disabled={!decoded} onClick={() => decoded && setComparison({ label: `${m.partition}@${m.offset}`, payload: decoded })}>pin for diff</button>
        </div>
        <div className="inspector-tabs">
          {(['structured', 'raw', 'schema', 'metadata', 'diff'] as InspectorTab[]).map((name) => (
            <button key={name} className={tab === name ? 'active' : ''} onClick={() => setTab(name)}>{name}</button>
          ))}
        </div>
        {decodeError && <div className="decode-diagnostic error">{decodeError}</div>}
        {decoded?.diagnostics.map((diagnostic, index) => <div key={index} className={`decode-diagnostic ${decoded.status === 'error' ? 'error' : ''}`}>{diagnostic}</div>)}

        <div className="payload-editor rich">
          {decoding ? <div className="details-empty"><span className="spin" /> decoding payload…</div>
            : tab === 'structured' && decoded?.value !== undefined && typeof decoded.value === 'object' ? <JsonTree value={decoded.value} />
              : tab === 'schema' ? decoded?.schema?.text ? <Editor height="100%" language={decoded.schema.type === 'PROTOBUF' ? 'plaintext' : 'json'} value={prettySchema(decoded.schema.text)} theme={theme === 'dark' ? 'koe-dark' : 'koe-light'} options={editorOptions(false)} /> : <div className="details-empty">No schema is associated with this payload.</div>
                : tab === 'metadata' ? <MetadataView payload={decoded} />
                  : tab === 'diff' ? comparison && decoded ? <JsonDiff before={comparison.payload.value} after={decoded.value} label={comparison.label} /> : <div className="details-empty">Pin a decoded message, then select another message to compare.</div>
                    : <Editor height="100%" language={tab === 'raw' ? rawPayload?.language ?? 'plaintext' : editorLanguage} value={tab === 'raw' ? rawPayload?.text ?? '' : decoded?.text ?? ''} theme={theme === 'dark' ? 'koe-dark' : 'koe-light'} options={editorOptions(requestedFormat === 'hex' || decoded?.format === 'binary')} />}
        </div>
      </section>
    </div>
  )
}

function editorOptions(noWrap: boolean): Record<string, unknown> {
  return { readOnly: true, minimap: { enabled: false }, lineNumbers: 'on', wordWrap: noWrap ? 'off' : 'on', scrollBeyondLastLine: false, fontSize: 11.5, fontFamily: 'var(--font-mono)', renderLineHighlight: 'none', folding: true, contextmenu: true, automaticLayout: true }
}

function prettySchema(schema: string): string { try { return JSON.stringify(JSON.parse(schema), null, 2) } catch { return schema } }

function JsonTree({ value, name = '$', depth = 0 }: { value: unknown; name?: string; depth?: number }): React.JSX.Element {
  const [open, setOpen] = useState(depth < 2)
  const isObject = value !== null && typeof value === 'object'
  if (!isObject) return <div className="json-leaf" style={{ paddingLeft: depth * 14 }}><span className="json-key">{name}</span><span className={`json-${typeof value}`}>{formatScalar(value)}</span></div>
  const entries = Array.isArray(value) ? value.map((item, index) => [String(index), item] as const) : Object.entries(value as Record<string, unknown>)
  return <div className="json-node"><button className="json-toggle" style={{ paddingLeft: depth * 14 }} onClick={() => setOpen((current) => !current)}><span>{open ? '▾' : '▸'}</span><span className="json-key">{name}</span><span className="json-count">{Array.isArray(value) ? `[${entries.length}]` : `{${entries.length}}`}</span></button>{open && entries.map(([key, nested]) => <JsonTree key={key} name={key} value={nested} depth={depth + 1} />)}</div>
}

function formatScalar(value: unknown): string { if (value === null) return 'null'; if (typeof value === 'string') return JSON.stringify(value); return String(value) }

function MetadataView({ payload }: { payload: DecodedPayload | null }): React.JSX.Element {
  if (!payload) return <div className="details-empty">No decode metadata available.</div>
  return <div className="metadata-view"><dl className="meta-grid"><dt>format</dt><dd>{payload.format}</dd><dt>status</dt><dd>{payload.status}</dd>{Object.entries(payload.metadata).map(([key, value]) => <Fragment key={key}><dt>{key}</dt><dd>{String(value)}</dd></Fragment>)}{payload.schema && <><dt>schema type</dt><dd>{payload.schema.type}</dd><dt>schema source</dt><dd>{payload.schema.source}</dd></>}</dl></div>
}

function JsonDiff({ before, after, label }: { before: unknown; after: unknown; label: string }): React.JSX.Element {
  const rows = diffValues(before, after)
  if (rows.length === 0) return <div className="details-empty">No structured differences from pinned message {label}.</div>
  return <div className="json-diff"><div className="diff-caption">Compared with {label}</div>{rows.map((row) => <div key={row.path} className={`diff-row ${row.kind}`}><span className="diff-kind">{row.kind === 'added' ? '+' : row.kind === 'removed' ? '−' : '±'}</span><span className="diff-path">{row.path}</span>{row.kind !== 'added' && <code>{formatScalar(row.before)}</code>}{row.kind === 'changed' && <span>→</span>}{row.kind !== 'removed' && <code>{formatScalar(row.after)}</code>}</div>)}</div>
}

interface DiffRow { path: string; kind: 'added' | 'removed' | 'changed'; before?: unknown; after?: unknown }
function diffValues(before: unknown, after: unknown, path = '$'): DiffRow[] {
  if (Object.is(before, after)) return []
  const beforeObject = before !== null && typeof before === 'object'
  const afterObject = after !== null && typeof after === 'object'
  if (!beforeObject || !afterObject || Array.isArray(before) !== Array.isArray(after)) return [{ path, kind: 'changed', before, after }]
  const left = before as Record<string, unknown>; const right = after as Record<string, unknown>
  return [...new Set([...Object.keys(left), ...Object.keys(right)])].flatMap((key) => {
    const nestedPath = Array.isArray(before) ? `${path}[${key}]` : `${path}.${key}`
    if (!(key in left)) return [{ path: nestedPath, kind: 'added' as const, after: right[key] }]
    if (!(key in right)) return [{ path: nestedPath, kind: 'removed' as const, before: left[key] }]
    return diffValues(left[key], right[key], nestedPath)
  })
}
