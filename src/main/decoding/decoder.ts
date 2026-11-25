import { gunzipSync } from 'node:zlib'
import { Type as AvroType } from 'avsc'
import Ajv from 'ajv'
import { deserialize as deserializeBson } from 'bson'
import { decode as decodeCbor } from 'cbor-x'
import { XMLBuilder, XMLParser } from 'fast-xml-parser'
import { decode as decodeMessagePack } from '@msgpack/msgpack'
import * as protobuf from 'protobufjs'
import type {
  ClusterProfile,
  DecodeFormat,
  DecodeMessageRequest,
  DecodedPayload,
  DecodedSchema,
  DetectedFormat,
  MessageHeader
} from '@shared/types'
import { SchemaRegistryClient, type RegistrySchema } from './schemaRegistry'

const MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
const SENSITIVE_FIELD = /(^|[_-])(pass(word)?|secret|token|authorization|api[_-]?key|credential|ssn|social[_-]?security)([_-]|$)/i

export interface DecodeContext {
  profile: ClusterProfile
  registrySecret: string | null
}

export async function decodeMessage(
  request: DecodeMessageRequest,
  context: DecodeContext
): Promise<DecodedPayload> {
  if (request.contentBase64 === null) {
    return result('text', 'decoded', '∅ null (tombstone)', null, { tombstone: true })
  }
  const bytes = Buffer.from(request.contentBase64, 'base64')
  if (bytes.length > MAX_PAYLOAD_BYTES) {
    return result(
      'binary',
      'error',
      hexDump(bytes.subarray(0, 4096)),
      undefined,
      { bytes: bytes.length, truncated: true },
      [`Payload exceeds the ${MAX_PAYLOAD_BYTES / 1024 / 1024} MiB interactive decode limit.`]
    )
  }
  try {
    return await decodeBytes(bytes, request, context, 0)
  } catch (error) {
    return result(
      request.requestedFormat === 'auto' || !request.requestedFormat ? 'binary' : request.requestedFormat,
      'error',
      hexDump(bytes),
      undefined,
      { bytes: bytes.length },
      [error instanceof Error ? error.message : String(error)]
    )
  }
}

async function decodeBytes(
  bytes: Buffer,
  request: DecodeMessageRequest,
  context: DecodeContext,
  depth: number
): Promise<DecodedPayload> {
  if (depth > 2) throw new Error('Nested compression limit exceeded')
  const forced = request.requestedFormat ?? 'auto'
  const headers = headerMap(request.headers)
  const contentType = headers['content-type'] ?? headers['contenttype'] ?? ''

  if ((forced === 'gzip' || (bytes[0] === 0x1f && bytes[1] === 0x8b)) && bytes.length > 2) {
    const inflated = gunzipSync(bytes)
    const nested = await decodeBytes(inflated, { ...request, requestedFormat: 'auto' }, context, depth + 1)
    return {
      ...nested,
      metadata: { ...nested.metadata, compression: 'gzip', compressedBytes: bytes.length, bytes: inflated.length },
      diagnostics: ['Gzip payload decompressed before decoding.', ...nested.diagnostics]
    }
  }

  if ((forced === 'auto' || forced === 'avro' || forced === 'protobuf' || forced === 'json-schema') &&
      bytes.length >= 5 && bytes[0] === 0) {
    const registryConfig = context.profile.schemaRegistry
    if (registryConfig?.enabled) {
      const schemaId = bytes.readUInt32BE(1)
      const client = new SchemaRegistryClient(registryConfig, context.registrySecret)
      const schema = await client.schemaById(schemaId)
      return decodeConfluent(bytes, schema, client, request.redactSensitive !== false)
    }
    if (forced !== 'auto') throw new Error('This is Confluent-framed data, but no Schema Registry is configured.')
  }

  if (forced === 'hex') return result('hex', 'raw', hexDump(bytes), undefined, { bytes: bytes.length })
  if (forced === 'bson' || (forced === 'auto' && isBson(bytes, contentType))) {
    return structured('bson', deserializeBson(bytes), request, { bytes: bytes.length })
  }
  if (forced === 'messagepack' || (forced === 'auto' && /msgpack|messagepack/i.test(contentType))) {
    return structured('messagepack', decodeMessagePack(bytes), request, { bytes: bytes.length })
  }
  if (forced === 'cbor' || (forced === 'auto' && /cbor/i.test(contentType))) {
    return structured('cbor', decodeCbor(bytes), request, { bytes: bytes.length })
  }

  const text = bytes.toString('utf8')
  if (forced === 'json' || forced === 'json-schema' || (forced === 'auto' && looksJson(text))) {
    const value = JSON.parse(text)
    const cloudEvent = isStructuredCloudEvent(value)
    return structured(cloudEvent ? 'cloudevents' : forced === 'json-schema' ? 'json-schema' : 'json', value, request, {
      bytes: bytes.length,
      ...(cloudEvent ? cloudEventMetadata(value as Record<string, unknown>) : {})
    })
  }
  if (forced === 'xml' || (forced === 'auto' && /^\s*</.test(text))) {
    const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_' })
    const value = parser.parse(text)
    const formatted = new XMLBuilder({ ignoreAttributes: false, attributeNamePrefix: '@_', format: true }).build(value)
    return {
      ...structured('xml', value, request, { bytes: bytes.length }),
      text: formatted.trim()
    }
  }
  if (forced === 'text' || (forced === 'auto' && isReadableText(bytes, text))) {
    const binaryCloudEvent = binaryCloudEventMetadata(headers)
    return result(
      binaryCloudEvent ? 'cloudevents' : 'text',
      'decoded',
      text,
      text,
      { bytes: bytes.length, ...(binaryCloudEvent ?? {}) }
    )
  }
  return result('binary', 'raw', hexDump(bytes), undefined, { bytes: bytes.length }, [
    'No safe decoder matched. Showing lossless hexadecimal bytes.'
  ])
}

async function decodeConfluent(
  bytes: Buffer,
  schema: RegistrySchema,
  client: SchemaRegistryClient,
  redact: boolean
): Promise<DecodedPayload> {
  const baseMetadata = { bytes: bytes.length, schemaId: schema.id, framing: 'confluent-v0' }
  if (schema.type === 'AVRO') {
    const registry: Record<string, AvroType> = {}
    for (const reference of schema.references ?? []) {
      const ref = await client.reference(reference.subject, reference.version)
      AvroType.forSchema(JSON.parse(ref.schema), { registry })
    }
    const type = AvroType.forSchema(JSON.parse(schema.schema), { registry })
    return structuredValue('avro', type.fromBuffer(bytes.subarray(5)), redact, baseMetadata, schema)
  }
  if (schema.type === 'PROTOBUF') {
    const { indexes, offset } = readMessageIndexes(bytes, 5)
    const root = new protobuf.Root()
    for (const reference of schema.references ?? []) {
      const ref = await client.reference(reference.subject, reference.version)
      protobuf.parse(ref.schema, root, { keepCase: true })
    }
    protobuf.parse(schema.schema, root, { keepCase: true })
    root.resolveAll()
    const type = protobufTypeAt(root, indexes)
    const message = type.decode(bytes.subarray(offset))
    const value = type.toObject(message, {
      longs: String,
      enums: String,
      bytes: String,
      defaults: true,
      arrays: true,
      objects: true
    })
    return structuredValue('protobuf', value, redact, { ...baseMetadata, messageType: type.fullName }, schema)
  }
  if (schema.type === 'JSON') {
    const value = JSON.parse(bytes.subarray(5).toString('utf8'))
    const validate = new Ajv({ allErrors: true, strict: false }).compile(JSON.parse(schema.schema))
    const valid = validate(value)
    const diagnostics = valid ? [] : (validate.errors ?? []).map((e) => `${e.instancePath || '/'} ${e.message ?? 'is invalid'}`)
    const decoded = structuredValue('json-schema', value, redact, { ...baseMetadata, schemaValid: valid }, schema)
    decoded.diagnostics.push(...diagnostics)
    return decoded
  }
  throw new Error(`Unsupported Schema Registry type: ${schema.type}`)
}

function structured(
  format: DetectedFormat,
  value: unknown,
  request: DecodeMessageRequest,
  metadata: Record<string, string | number | boolean>
): DecodedPayload {
  return structuredValue(format, value, request.redactSensitive !== false, metadata)
}

function structuredValue(
  format: DetectedFormat,
  value: unknown,
  redact: boolean,
  metadata: Record<string, string | number | boolean>,
  schema?: DecodedSchema
): DecodedPayload {
  const normalized = normalize(value)
  const redacted = redactValue(normalized, redact)
  return {
    format,
    status: 'decoded',
    text: JSON.stringify(redacted.value, null, 2),
    value: redacted.value,
    schema,
    metadata: { ...metadata, ...(redacted.count > 0 ? { redactedFields: redacted.count } : {}) },
    diagnostics: redacted.count > 0 ? [`Redacted ${redacted.count} potentially sensitive field(s).`] : []
  }
}

function result(
  format: DetectedFormat,
  status: DecodedPayload['status'],
  text: string,
  value: unknown,
  metadata: Record<string, string | number | boolean>,
  diagnostics: string[] = []
): DecodedPayload {
  return { format, status, text, value, metadata, diagnostics }
}

function normalize(value: unknown, seen = new WeakSet<object>()): unknown {
  if (typeof value === 'bigint') return value.toString()
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return { $binary: Buffer.from(value).toString('base64') }
  if (value instanceof Date) return value.toISOString()
  if (value === null || typeof value !== 'object') return value
  if (seen.has(value)) return '[Circular]'
  seen.add(value)
  if (Array.isArray(value)) return value.map((item) => normalize(item, seen))
  return Object.fromEntries(Object.entries(value as Record<string, unknown>).map(([k, v]) => [k, normalize(v, seen)]))
}

function redactValue(value: unknown, enabled: boolean): { value: unknown; count: number } {
  if (!enabled || value === null || typeof value !== 'object') return { value, count: 0 }
  if (Array.isArray(value)) {
    let count = 0
    const out = value.map((item) => {
      const nested = redactValue(item, true)
      count += nested.count
      return nested.value
    })
    return { value: out, count }
  }
  let count = 0
  const out: Record<string, unknown> = {}
  for (const [key, nestedValue] of Object.entries(value as Record<string, unknown>)) {
    if (SENSITIVE_FIELD.test(key)) {
      out[key] = '••••••••'
      count++
    } else {
      const nested = redactValue(nestedValue, true)
      out[key] = nested.value
      count += nested.count
    }
  }
  return { value: out, count }
}

function readMessageIndexes(bytes: Buffer, start: number): { indexes: number[]; offset: number } {
  if (bytes[start] === 0) return { indexes: [0], offset: start + 1 }
  let cursor = start
  const length = readZigZagVarint(bytes, cursor)
  cursor = length.offset
  if (length.value < 1 || length.value > 64) throw new Error('Invalid Protobuf message-index path')
  const indexes: number[] = []
  for (let i = 0; i < length.value; i++) {
    const decoded = readZigZagVarint(bytes, cursor)
    indexes.push(decoded.value)
    cursor = decoded.offset
  }
  return { indexes, offset: cursor }
}

function readZigZagVarint(bytes: Buffer, start: number): { value: number; offset: number } {
  let raw = 0
  let shift = 0
  let cursor = start
  while (cursor < bytes.length && shift < 35) {
    const byte = bytes[cursor++]
    raw |= (byte & 0x7f) << shift
    if ((byte & 0x80) === 0) return { value: (raw >>> 1) ^ -(raw & 1), offset: cursor }
    shift += 7
  }
  throw new Error('Invalid Protobuf message-index varint')
}

function protobufTypeAt(root: protobuf.Root, indexes: number[]): protobuf.Type {
  let namespace: protobuf.NamespaceBase = root
  let type: protobuf.Type | undefined
  for (const index of indexes) {
    const types = Object.values(namespace.nested ?? {}).filter((item): item is protobuf.Type => item instanceof protobuf.Type)
    type = types[index]
    if (!type) throw new Error(`Protobuf message index ${index} is not present in the registered schema`)
    namespace = type
  }
  if (!type) throw new Error('Registered Protobuf schema has no message type')
  return type
}

function headerMap(headers: MessageHeader[]): Record<string, string> {
  return Object.fromEntries(
    headers.map((header) => [header.key.toLowerCase(), Buffer.from(header.value, 'base64').toString('utf8')])
  )
}

function looksJson(text: string): boolean {
  const trimmed = text.trim()
  return trimmed.startsWith('{') || trimmed.startsWith('[')
}

function isReadableText(bytes: Buffer, text: string): boolean {
  if (text.includes('\uFFFD')) return false
  if (bytes.length === 0) return true
  let readable = 0
  for (const byte of bytes) if (byte === 9 || byte === 10 || byte === 13 || byte >= 32) readable++
  return readable / bytes.length > 0.9
}

function isBson(bytes: Buffer, contentType: string): boolean {
  return /bson/i.test(contentType) || (bytes.length >= 5 && bytes.readInt32LE(0) === bytes.length && bytes.at(-1) === 0)
}

function isStructuredCloudEvent(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const record = value as Record<string, unknown>
  return typeof record.specversion === 'string' && typeof record.id === 'string' && typeof record.source === 'string' && typeof record.type === 'string'
}

function cloudEventMetadata(value: Record<string, unknown>): Record<string, string> {
  return Object.fromEntries(
    ['specversion', 'id', 'source', 'type', 'subject', 'time', 'datacontenttype']
      .filter((key) => typeof value[key] === 'string')
      .map((key) => [`cloudevent.${key}`, value[key] as string])
  )
}

function binaryCloudEventMetadata(headers: Record<string, string>): Record<string, string> | null {
  if (!headers['ce-specversion'] || !headers['ce-id'] || !headers['ce-source'] || !headers['ce-type']) return null
  return Object.fromEntries(
    Object.entries(headers).filter(([key]) => key.startsWith('ce-')).map(([key, value]) => [`cloudevent.${key.slice(3)}`, value])
  )
}

function hexDump(bytes: Buffer): string {
  const lines: string[] = []
  for (let i = 0; i < bytes.length; i += 16) {
    const chunk = bytes.subarray(i, i + 16)
    const hex = [...chunk].map((byte) => byte.toString(16).padStart(2, '0')).join(' ')
    const ascii = [...chunk].map((byte) => (byte >= 32 && byte < 127 ? String.fromCharCode(byte) : '.')).join('')
    lines.push(`${i.toString(16).padStart(8, '0')}  ${hex.padEnd(47)}  ${ascii}`)
  }
  return lines.join('\n')
}
