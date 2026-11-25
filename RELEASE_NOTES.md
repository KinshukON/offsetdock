# OffsetDock 0.1.0

The first public macOS release of OffsetDock, a local-first Kafka operations cockpit by Data4AI.

## Highlights

- Browse brokers, topics, partitions, and consumer groups.
- Inspect and search messages by offset or timestamp.
- Diagnose consumer lag and group assignments.
- Produce test events and manage topic configuration.
- Connect with TLS, mTLS, SASL PLAIN/SCRAM, or AWS MSK IAM.
- Explore safely with the built-in demo cluster.
- No account, cloud relay, analytics, or telemetry.

## Payload intelligence

- Auto-detect and inspect JSON, XML, text, gzip, MessagePack, CBOR, BSON, and
  structured or binary CloudEvents.
- Decode Confluent-framed Avro, Protobuf, and JSON Schema payloads through a
  Confluent-compatible or Apicurio Schema Registry.
- Browse structured values, raw bytes, schemas, metadata, and field-level diffs.
- Redact common sensitive fields by default and preserve malformed payloads as
  lossless hex.

## macOS installation

Apple silicon and Intel builds are available. Both are currently unsigned and not notarized. After copying OffsetDock to Applications, Control-click the app and choose **Open** on first launch.

## Windows and Linux installation

Windows x64 builds are available as an installer, portable executable, and ZIP archive. Linux builds are available as an x64 AppImage and Debian package, plus an ARM64 AppImage. These community builds are unsigned; verify the published checksums before installation.
