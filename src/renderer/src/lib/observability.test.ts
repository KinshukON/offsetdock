import { describe, expect, it } from 'vitest'
import type { ConsumerGroupDetail, KafkaMessage, TopicDetail } from '@shared/types'
import { detectObservedFormat, groupSnapshot, observeGroup, observeTopic } from './observability'

const encode = (text: string): string => btoa(text)
const encodeBytes = (bytes: number[]): string => btoa(String.fromCharCode(...bytes))
const message = (overrides: Partial<KafkaMessage> = {}): KafkaMessage => ({
  topic: 'events', partition: 0, offset: '1', timestamp: '2026-11-24T12:00:00.000Z',
  key: null, value: encode('{"ok":true}'), headers: [], keySize: 0, valueSize: 11, ...overrides
})

describe('topic observability', () => {
  it('detects JSON, CloudEvents, schema frames, gzip, and tombstones', () => {
    expect(detectObservedFormat(message())).toBe('json')
    expect(detectObservedFormat(message({ value: encode('{"specversion":"1.0","type":"x","source":"test"}') }))).toBe('cloudevent')
    expect(detectObservedFormat(message({ value: encodeBytes([0, 0, 0, 0, 3, 1]) }))).toBe('schema-framed')
    expect(detectObservedFormat(message({ value: encodeBytes([0x1f, 0x8b, 1]) }))).toBe('gzip')
    expect(detectObservedFormat(message({ value: null }))).toBe('tombstone')
  })

  it('flags under-replication, partition skew, and missing trace context with evidence', () => {
    const messages = Array.from({ length: 20 }, (_, index) => message({ partition: index < 18 ? 0 : index }))
    const detail: TopicDetail = {
      name: 'events', internal: false, partitionCount: 3, replicationFactor: 3, messageCount: '20',
      partitions: [{ partition: 0, leader: 1, replicas: [1, 2, 3], isr: [1, 2], earliestOffset: '0', latestOffset: '20' }]
    }
    const result = observeTopic(messages, detail, Date.parse('2026-11-24T12:01:00.000Z'))
    expect(result.underReplicated).toBe(1)
    expect(result.signals.map((signal) => signal.id)).toEqual(expect.arrayContaining(['under-replicated', 'partition-skew', 'trace-coverage']))
  })
})

describe('consumer observability', () => {
  const detail: ConsumerGroupDetail = {
    groupId: 'workers', state: 'Stable', protocolType: 'consumer', memberCount: 1, members: [],
    offsets: [
      { topic: 'events', partition: 0, committedOffset: '80', earliestOffset: '0', latestOffset: '100', lag: '20' },
      { topic: 'events', partition: 1, committedOffset: '99', earliestOffset: '0', latestOffset: '100', lag: '1' },
      { topic: 'events', partition: 2, committedOffset: '100', earliestOffset: '0', latestOffset: '100', lag: '0' }
    ]
  }

  it('detects concentrated lag', () => {
    expect(observeGroup(detail, [groupSnapshot(detail, 1)]).signals.map((signal) => signal.id)).toContain('lag-skew')
  })

  it('uses consecutive snapshots to explain a recovering backlog', () => {
    const previous = { capturedAt: 1, totalLag: 40n, totalEndOffset: 280n }
    const current = groupSnapshot(detail, 2)
    expect(observeGroup(detail, [previous, current]).signals.map((signal) => signal.id)).toContain('recovering')
  })

  it('infers input, processing, and catch-up rates from offset movement', () => {
    const result = observeGroup(detail, [
      { capturedAt: 0, totalLag: 31n, totalEndOffset: 270n },
      { capturedAt: 10_000, totalLag: 21n, totalEndOffset: 300n }
    ])
    expect(result.inputRate).toBe(3)
    expect(result.processingRate).toBe(4)
    expect(result.netDrainRate).toBe(1)
    expect(result.catchUpSeconds).toBe(21)
  })
})
