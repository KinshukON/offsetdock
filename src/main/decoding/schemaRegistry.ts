import type { ClusterProfile, DecodedSchema } from '@shared/types'

export interface RegistrySchema extends DecodedSchema {
  id: number
  schema: string
}

interface RegistryResponse {
  schema: string
  schemaType?: string
  references?: { name: string; subject: string; version: number }[]
}

interface SubjectVersionResponse extends RegistryResponse {
  id: number
  subject?: string
  version?: number
}

/** Small Confluent-compatible client. Apicurio exposes the same compatibility API. */
export class SchemaRegistryClient {
  private readonly cache = new Map<number, Promise<RegistrySchema>>()
  private readonly referenceCache = new Map<string, Promise<SubjectVersionResponse>>()

  constructor(
    private readonly config: NonNullable<ClusterProfile['schemaRegistry']>,
    private readonly secret: string | null
  ) {}

  async schemaById(id: number): Promise<RegistrySchema> {
    let pending = this.cache.get(id)
    if (!pending) {
      pending = this.fetchJson<RegistryResponse>(`/schemas/ids/${id}`).then((result) => ({
        id,
        type: normalizeType(result.schemaType),
        source: 'schema-registry' as const,
        text: result.schema,
        schema: result.schema,
        references: result.references ?? []
      }))
      this.cache.set(id, pending)
    }
    return pending
  }

  async reference(subject: string, version: number): Promise<SubjectVersionResponse> {
    const key = `${subject}@${version}`
    let pending = this.referenceCache.get(key)
    if (!pending) {
      pending = this.fetchJson<SubjectVersionResponse>(
        `/subjects/${encodeURIComponent(subject)}/versions/${version}`
      )
      this.referenceCache.set(key, pending)
    }
    return pending
  }

  private async fetchJson<T>(path: string): Promise<T> {
    const base = this.config.url.replace(/\/+$/, '')
    const headers: Record<string, string> = { Accept: 'application/vnd.schemaregistry.v1+json, application/json' }
    if (this.config.auth === 'basic') {
      if (!this.config.username || !this.secret) throw new Error('Schema Registry username and password are required')
      headers.Authorization = `Basic ${Buffer.from(`${this.config.username}:${this.secret}`).toString('base64')}`
    } else if (this.config.auth === 'bearer') {
      if (!this.secret) throw new Error('Schema Registry bearer token is required')
      headers.Authorization = `Bearer ${this.secret}`
    }
    const response = await fetch(base + path, {
      headers,
      signal: AbortSignal.timeout(10_000)
    })
    if (!response.ok) {
      const body = (await response.text()).slice(0, 500)
      throw new Error(`Schema Registry ${response.status}: ${body || response.statusText}`)
    }
    return (await response.json()) as T
  }
}

function normalizeType(value: string | undefined): RegistrySchema['type'] {
  const type = (value ?? 'AVRO').toUpperCase()
  if (type === 'AVRO' || type === 'PROTOBUF' || type === 'JSON') return type
  return 'UNKNOWN'
}
