import { describe, expect, it } from 'vitest'
import type { ConsumerGroupDetail } from '@shared/types'
import type { GroupObservation } from './observability'
import {
  appendFlightSnapshot, explainFlight, flightSnapshot, loadFlightHistory,
  saveFlightHistory, simulateRecovery
} from './timeMachine'

const detail = (memberCount = 2): ConsumerGroupDetail => ({
  groupId: 'workers', state: 'Stable', protocolType: 'consumer', memberCount, members: [],
  offsets: [
    { topic: 'events', partition: 0, committedOffset: '80', earliestOffset: '0', latestOffset: '100', lag: '20' },
    { topic: 'events', partition: 1, committedOffset: '95', earliestOffset: '0', latestOffset: '100', lag: '5' }
  ]
})

describe('time machine history', () => {
  it('round-trips bigint snapshots through local storage', () => {
    let value: string | null = null
    const storage = { getItem: () => value, setItem: (_key: string, next: string) => { value = next } }
    const snapshot = flightSnapshot(detail(), 1000)
    saveFlightHistory(storage, 'history', [snapshot])
    expect(loadFlightHistory(storage, 'history')).toEqual([snapshot])
  })

  it('deduplicates snapshots captured within one second', () => {
    const first = flightSnapshot(detail(), 1000)
    expect(appendFlightSnapshot([first], flightSnapshot(detail(), 1500))).toHaveLength(1)
    expect(appendFlightSnapshot([first], flightSnapshot(detail(), 2500))).toHaveLength(2)
  })

  it('connects membership loss with rising lag using explicit evidence', () => {
    const before = { ...flightSnapshot(detail(4), 0), totalLag: 10n, totalEndOffset: 100n }
    const after = { ...flightSnapshot(detail(2), 10_000), totalLag: 30n, totalEndOffset: 140n }
    const explanation = explainFlight([before, after])
    expect(explanation.title).toBe('Capacity loss coincides with rising lag')
    expect(explanation.evidence.join(' ')).toContain('membership changed from 4 to 2')
  })
})

describe('counterfactual recovery simulator', () => {
  const observation: GroupObservation = {
    totalLag: 600n, worstPartitionLag: 200n, laggingPartitions: 3, partitionSkew: 1,
    lagDelta: 0n, endOffsetDelta: 100n, inputRate: 10, processingRate: 20,
    netDrainRate: 10, catchUpSeconds: 60, signals: []
  }

  it('projects catch-up and caps active consumers at partition count', () => {
    const result = simulateRecovery(observation, 2, 4, {
      consumers: 8, throughputMultiplier: 1, inputMultiplier: 1
    })
    expect(result.activeConsumers).toBe(4)
    expect(result.projectedProcessingRate).toBe(40)
    expect(result.catchUpSeconds).toBe(20)
    expect(result.constrainedByPartitions).toBe(true)
  })

  it('rejects a scenario that cannot outrun incoming traffic', () => {
    const result = simulateRecovery(observation, 2, 4, {
      consumers: 1, throughputMultiplier: 1, inputMultiplier: 1
    })
    expect(result.catchUpSeconds).toBeNull()
    expect(result.verdict).toContain('does not process faster')
  })
})
