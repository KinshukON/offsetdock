import { gzipSync } from 'node:zlib'
import { beforeAll, describe, expect, it, vi } from 'vitest'
import { Type as AvroType } from 'avsc'
import { serialize as serializeBson } from 'bson'
import { encode as encodeCbor } from 'cbor-x'
import { encode as encodeMessagePack } from '@msgpack/msgpack'
import * as protobuf from 'protobufjs'
import type { ClusterProfile, DecodeMessageRequest, MessageHeader } from '@shared/types'
import { decodeMessage } from './decoder'

const schemas = new Map<number, { schema: string; schemaType: string }>()
const registryUrl = 'http://schema-registry.test'

beforeAll(() => {
  vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
    const id = Number(url.match(/\/schemas\/ids\/(\d+)/)?.[1])
    const schema = schemas.get(id)
    if (!schema) {
      return new Response(JSON.stringify({ error_code: 40403, message: 'Schema not found' }), {
        status: 404,
        headers: { 'content-type': 'application/json' }
      })
    }
    return new Response(JSON.stringify(schema), { headers: { 'content-type': 'application/json' } })
  }))
})

const profile = (): ClusterProfile => ({
  id: 'test',
  name: 'test',
  bootstrapServers: 'localhost:9092',
  securityProtocol: 'PLAINTEXT',
  requestTimeoutMs: 30_000,
  color: 'blue',
  isDemo: false,
  createdAt: new Date(0).toISOString(),
  schemaRegistry: {
    enabled: true,
    provider: 'confluent',
    url: registryUrl,
    auth: 'none',
    hasSecret: false
  }
})

const request = (
  bytes: Uint8Array | Buffer,
  headers: MessageHeader[] = [],
  patch: Partial<DecodeMessageRequest> = {}
): DecodeMessageRequest => ({
  profileId: 'test',
  topic: 'events',
  contentBase64: Buffer.from(bytes).toString('base64'),
  headers,
  ...patch
})

const header = (key: string, value: string): MessageHeader => ({
  key,
  value: Buffer.from(value).toString('base64')
})

function frame(schemaId: number, payload: Uint8Array, protobufIndexes?: Uint8Array): Buffer {
  const prefix = Buffer.alloc(5)
  prefix[0] = 0
  prefix.writeUInt32BE(schemaId, 1)
  return Buffer.concat([prefix, ...(protobufIndexes ? [Buffer.from(protobufIndexes)] : []), Buffer.from(payload)])
}

describe('decodeMessage', () => {
  it('pretty prints JSON and redacts sensitive fields by default', async () => {
    const decoded = await decodeMessage(
      request(Buffer.from(JSON.stringify({ user: 'ada', api_key: 'secret' }))),
      { profile: profile(), registrySecret: null }
    )
    expect(decoded.format).toBe('json')
    expect(decoded.value).toEqual({ user: 'ada', api_key: '••••••••' })
    expect(decoded.metadata.redactedFields).toBe(1)
  })

  it('recognizes structured CloudEvents', async () => {
    const value = { specversion: '1.0', id: '42', source: '/orders', type: 'order.created', data: { id: 9 } }
    const decoded = await decodeMessage(request(Buffer.from(JSON.stringify(value))), {
      profile: profile(),
      registrySecret: null
    })
    expect(decoded.format).toBe('cloudevents')
    expect(decoded.metadata['cloudevent.type']).toBe('order.created')
  })

  it('recursively decompresses gzip payloads', async () => {
    const decoded = await decodeMessage(request(gzipSync(JSON.stringify({ ok: true }))), {
      profile: profile(),
      registrySecret: null
    })
    expect(decoded.format).toBe('json')
    expect(decoded.value).toEqual({ ok: true })
    expect(decoded.metadata.compression).toBe('gzip')
  })

  it('decodes MessagePack, CBOR, and BSON when self-described', async () => {
    const cases = [
      [encodeMessagePack({ format: 'msgpack' }), header('content-type', 'application/msgpack'), 'messagepack', 'msgpack'],
      [encodeCbor({ format: 'cbor' }), header('content-type', 'application/cbor'), 'cbor', 'cbor'],
      [serializeBson({ format: 'bson' }), header('content-type', 'application/bson'), 'bson', 'bson']
    ] as const
    for (const [bytes, contentType, format, valueFormat] of cases) {
      const decoded = await decodeMessage(request(bytes, [contentType]), {
        profile: profile(),
        registrySecret: null
      })
      expect(decoded.format).toBe(format)
      expect(decoded.value).toEqual({ format: valueFormat })
    }
  })

  it('decodes Confluent-framed Avro', async () => {
    const schema = {
      type: 'record',
      name: 'Order',
      fields: [
        { name: 'id', type: 'long' },
        { name: 'status', type: 'string' }
      ]
    }
    schemas.set(11, { schema: JSON.stringify(schema), schemaType: 'AVRO' })
    const payload = AvroType.forSchema(schema as Parameters<typeof AvroType.forSchema>[0]).toBuffer({ id: 7, status: 'CREATED' })
    const decoded = await decodeMessage(request(frame(11, payload)), {
      profile: profile(),
      registrySecret: null
    })
    expect(decoded.format).toBe('avro')
    expect(decoded.value).toEqual({ id: 7, status: 'CREATED' })
    expect(decoded.schema?.id).toBe(11)
  })

  it('decodes Confluent-framed Protobuf with the optimized [0] message index', async () => {
    const schema = 'syntax = "proto3"; message Order { int64 id = 1; string status = 2; }'
    schemas.set(12, { schema, schemaType: 'PROTOBUF' })
    const root = protobuf.parse(schema, { keepCase: true }).root
    const type = root.lookupType('Order')
    const payload = type.encode(type.create({ id: 9, status: 'SHIPPED' })).finish()
    const decoded = await decodeMessage(request(frame(12, payload, Uint8Array.of(0))), {
      profile: profile(),
      registrySecret: null
    })
    expect(decoded.format).toBe('protobuf')
    expect(decoded.value).toEqual({ id: '9', status: 'SHIPPED' })
    expect(decoded.metadata.messageType).toBe('.Order')
  })

  it('validates Confluent-framed JSON Schema payloads', async () => {
    schemas.set(13, {
      schema: JSON.stringify({ type: 'object', required: ['id'], properties: { id: { type: 'integer' } } }),
      schemaType: 'JSON'
    })
    const decoded = await decodeMessage(request(frame(13, Buffer.from('{"name":"missing id"}'))), {
      profile: profile(),
      registrySecret: null
    })
    expect(decoded.format).toBe('json-schema')
    expect(decoded.metadata.schemaValid).toBe(false)
    expect(decoded.diagnostics.join(' ')).toContain('required property')
  })

  it('falls back to a lossless hex dump for unknown binary', async () => {
    const decoded = await decodeMessage(request(Uint8Array.of(0xff, 0x00, 0x81, 0x02)), {
      profile: { ...profile(), schemaRegistry: undefined },
      registrySecret: null
    })
    expect(decoded.format).toBe('binary')
    expect(decoded.status).toBe('raw')
    expect(decoded.text).toContain('ff 00 81 02')
  })
})
