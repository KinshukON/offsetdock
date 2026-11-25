# OffsetDock 0.2.0

OffsetDock 0.2.0 introduces **OffsetDock Time Machine**, an evidence-based
Kafka incident recorder and counterfactual recovery simulator that remains
local to the operator's device.

## OffsetDock Time Machine

- Retain up to 180 snapshots of consumer lag, log-end movement, group state,
  membership, partition count, and worst-partition lag.
- Scrub through recorded states after navigating away from the consumer group.
- Explain whether lag growth coincides with traffic, membership loss, group
  state changes, or partition concentration.
- Show every conclusion with the measurements that support it instead of an
  opaque health score.
- Simulate changes to consumer count, per-consumer throughput, and incoming
  traffic before changing production.
- Respect Kafka's partition assignment ceiling and estimate whether, and when,
  a proposed scenario would clear the backlog.

## Privacy and safety

- Time Machine stores only operational offset metadata in local application
  storage. It never records message payloads, keys, headers, schemas, or
  credentials.
- Recovery simulations are read-only and never issue Kafka commands.
- No account, cloud relay, analytics, or telemetry is required.

## Existing observability

- Profile observed topic traffic by rate, payload size, wire format, trace
  coverage, partition heat, and replication health.
- Infer producer rate, consumer throughput, backlog drain, and catch-up ETA
  from consecutive offset observations.
- Inspect rich JSON, XML, text, gzip, MessagePack, CBOR, BSON, CloudEvents,
  Avro, Protobuf, and JSON Schema payloads.

## Downloads

Windows x64 builds are available as an installer, portable executable, and ZIP
archive. Linux builds are available as an x64 AppImage and Debian package, plus
an ARM64 AppImage. Public macOS distribution remains gated on Developer ID
signing and notarization; source builds and the verified local development
installation remain available.
