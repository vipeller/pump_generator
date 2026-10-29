# Pump generator

A Rust CLI that generates a simulated pump history and publishes complete JSON snapshots over **MQTT 5**. Virtual sampling time is independent of publishing speed: by default, a day of one-second samples is sent as fast as the broker permits, not over a real day.

## Run

Install a current stable Rust toolchain, then build and test:

```text
cargo build --release --locked
cargo test --locked
```

Generate a day without a broker (newline-delimited JSON on stdout, progress on stderr):

```text
cargo run --release --locked -- --dry-run --start-time 2026-10-28T00:00:00Z > telemetry.ndjson
```

Publish to a local MQTT 5 broker:

```text
cargo run --release --locked -- --broker-host localhost --model-reference urn:pump-generator:model:pump-telemetry:1.0.0 --asset-id pump-001 --start-time 2026-10-28T00:00:00Z
```

The URN in this example is the bundled TD's identifier, **not a downloadable URL**. Register the model with the consumer's registry, or host the TD and pass its actual URL instead. This application does not host, register, retrieve, or resolve the model.

### Configuration

| Argument | Default | Meaning |
| --- | --- | --- |
| `--model-reference` | Required when publishing | Model URL or registry identifier placed on every publication |
| `--asset-id` | Required when publishing | Asset identifier placed on every publication as an MQTT User Property |
| `--start-time` | Yesterday, 00:00:00 UTC | First event time, RFC 3339; offsets are normalized to UTC |
| `--duration-hours` | `24` | Virtual duration, at least 12 hours |
| `--sample-interval-ms` | `1000` | Virtual distance between samples |
| `--seed` | `42` | Repeatable measurement variation |
| `--max-messages-per-second` | Unlimited | Optional real-time send-rate cap, positive integer |
| `--broker-host` | `localhost` | Hostname/IP without scheme or port |
| `--broker-port` | `1883`, or `8883` with TLS | MQTT TCP port |
| `--topic` | `pumps/pump-001/telemetry` | Publication topic; no wildcards |
| `--client-id` | `pump-generator-<process-id>` | Override for broker ACLs or concurrent runs |
| `--tls` | Disabled | TLS with certificate and hostname verification |
| `--ca-cert` | System trust roots | PEM CA bundle; requires TLS |
| `--client-cert`, `--client-key` | None | PEM certificate chain/key for mutual TLS; requires TLS and a CA bundle |
| `--username` | `PUMP_MQTT_USERNAME` if set | Optional broker username |
| `--password` | `PUMP_MQTT_PASSWORD` if set | Optional broker password; requires a username |
| `--progress-timeout-seconds` | `30` | Maximum time without connection/delivery progress; paced runs allow one extra send interval |
| `--dry-run` | Disabled | Emit payloads without MQTT; rate limit and MQTT settings do not affect this mode |

Use `--help` for the complete CLI reference. Start times may have at most millisecond precision. Sampling includes the start but excludes the end; intervals too coarse to capture the final bearing-temperature anomaly are rejected.

Prefer the password environment variable to a CLI password, which can be exposed in shell history and process listings. Credentials are not logged. Avoid plaintext MQTT outside trusted local development networks.

For the endpoint from the original TD, the connection arguments would be:

```text
--tls --broker-host aio-broker.azure-iot-operations --broker-port 18883 --topic azure-iot-operations/data/pump-machines-pump-1/Machines.Pump_1
```

That hostname must be reachable from the generator (it may be cluster-internal); supply the appropriate CA and authentication for your deployment. No deployment-specific certificates or credentials are bundled.

## Payload and WoT model

[`models/pump-telemetry.td.json`](models/pump-telemetry.td.json) describes every emitted JSON key. Its property keys match the abbreviated wire keys directly; the `title` supplies the original long name.

```json
{
  "Timestamp": "2026-10-28T23:00:00.000Z",
  "BT": 432.14,
  "DP": 234000.0,
  "FT": 317.12,
  "LV": 2.98,
  "MF": 7.7,
  "NS": 86,
  "PE": 71.0,
  "PPI": 2537.75,
  "FC": false,
  "MO": false
}
```

This is an illustrative snapshot, not an exact seed fixture.

| Key | Measurement | Unit | Normal operating range |
| --- | --- | --- | --- |
| `Timestamp` | Virtual event time | UTC RFC 3339 | — |
| `BT` | Bearing temperature | Kelvin | 273–423 |
| `DP` | Differential pressure | Pascal | 0–400,000 |
| `FT` | Fluid temperature | Kelvin | 263–395 |
| `LV` | Level | meter | 0–5 |
| `MF` | Mass flow | kg/s | 0–10 |
| `NS` | Number of starts | Count | Not specified |
| `PE` | Pump efficiency | Percent | 0–100 |
| `PPI` | Pump power input | Watt | 0–4,000 |
| `FC` | Fluid cavitation | Boolean state | — |
| `MO` | Motor overheat | Boolean state | — |

The TD preserves the supplied `dov:unit`, `dov:normalOperatingMinimum`, and `dov:normalOperatingMaximum` annotations. Normal ranges are **not JSON Schema validation bounds**: a bearing reading above 423 K must remain valid telemetry.

The TD's MQTT forms describe the default local endpoint, topic, and unauthenticated security configuration. If deploying elsewhere, update the forms/security to match that deployment and publish/register the revised TD. The CLI does not rewrite the bundled model. Subscription forms use `mqv:filter` and string QoS `"1"`, as specified by the WoT MQTT binding; runtime delivery uses QoS level 1. Per-property forms use the same aggregate JSON stream; the abbreviated key identifies the value in each snapshot.

### MQTT User Properties

Every PUBLISH carries two MQTT 5 User Properties:

```text
model-reference = <value supplied with --model-reference>
asset-id        = <value supplied with --asset-id>
```

Both values are required when publishing and remain constant throughout a run. Their names are fixed in the application, not CLI-configurable. Values must be nonblank, contain no NUL characters, and fit MQTT's 65,535-byte UTF-8 string limit. These properties are message metadata, not JSON payload fields, so the TD's measurement keys are unchanged. Dry-run mode requires neither value.

**`model-reference` is a provisional key**, not the final integration name. Replace only `MODEL_REFERENCE_PROPERTY_NAME` in [`src/model.rs`](src/model.rs) when the fixed name is known, and update this documentation. The asset property name is `asset-id`.

Publications also set the standard MQTT content type `application/json` and UTF-8 payload-format indicator. These are standard protocol properties, not additional User Properties.

The consumer resolves the reference and looks up, for example:

```text
payload["BT"] -> TD.properties["BT"] -> title, unit, normal operating limits
```

Use a version-specific model reference so changes to a model do not reinterpret historical data.

### Original OPC UA address-space references

The TD preserves the source model's OPC UA references in `pg:opcUaSource`, a **project-specific metadata extension**, not a standard OPC UA protocol binding. The `pg` prefix is declared as `urn:pump-generator:metadata:` in `@context`.

For example, the abbreviated level property retains its original source address:

```json
{
  "LV": {
    "title": "Operational.Measurements.Level",
    "pg:opcUaSource": {
      "href": "/?id=nsu=urn:localhost:OPCFoundation:PumpDeviceIntegrationServer;i=678946954"
    }
  }
}
```

This is a property fragment; the complete TD also includes its type, units, range, and MQTT forms. Consumers can inspect `TD.properties["LV"]["pg:opcUaSource"].href` to recover the original address. The top-level `pg:opcUaSource` also preserves the original pump object's `dov:dataSource` identifier.

These are **model/provenance references**, not a claim that generated samples were read from OPC UA. They are intentionally outside active `forms`: the simulator implements only MQTT. The source server endpoint was not supplied, so the relative `/?id=...` references are not independently usable connection URLs and must not be resolved against the hosted MQTT TD's URL. An actual OPC UA integration would also need the original server endpoint, security settings, and compatible binding.

The original cavitation (`FC`) and motor-overheat (`MO`) entries both used `/?id=i=0`, the null NodeId. These are retained with `"placeholder": true`, not treated as known signal addresses. The generated `Timestamp` has no OPC UA source reference.

## Scenario

The pump is **already running normally** when sampling starts. There is no startup warm-up, restart, shutdown, cavitation episode, or motor-overheat event.

For the default 24-hour run:

- Hours 0–12: bearing temperature remains around 333 K (about 60 °C).
- Final 12 hours: it rises at a nominal 9 K/hour, with small smooth variation.
- Around hour 22: it crosses 423 K (about 150 °C).
- Final samples: about 441 K, above the normal range.

For longer runs, the rise still starts 12 hours before the end. `NS` remains 86; `FC` and `MO` remain false. Bearing overheating is not treated as motor overheating.

Other measurements vary smoothly within normal limits. Flow and pressure share a small load variation, and input power is derived from pressure, mass flow, and efficiency, assuming a water-like density of 1,000 kg/m³:

```text
InputPower = DifferentialPressure * (MassFlow / FluidDensity) / (Efficiency / 100)
```

Nominal values are approximately 7.7 kg/s, 234,000 Pa, 71%, and 2,538 W. This is plausible synthetic telemetry, not a calibrated physical pump simulation. Smooth seeded variation depends only on virtual elapsed time, not queueing, publish speed, or sampling cadence. An explicit start time and seed make runs repeatable.

## Delivery semantics

- One complete snapshot per message, MQTT 5, **QoS 1**, **retain disabled**.
- A day at one-second sampling produces **86,400 messages**.
- The generator streams data with bounded queues and at most 128 outgoing in-flight messages (or the broker's lower limit), rather than holding a whole day in memory.
- The optional rate cap paces actual outgoing publications; backpressure does not lead to catch-up bursts.
- Success is reported only after every publication receives a successful PUBACK and the MQTT disconnect has been sent.
- Rejected publications, lost connections, or lack of delivery progress cause a nonzero exit. There is no automatic reconnect, persisted checkpoint, or resume in this initial version. A failed run can leave a partially delivered dataset.
- QoS 1 is not exactly-once delivery. Consumers should tolerate duplicates, including reruns; the `asset-id` User Property plus `Timestamp` can be used as a deduplication key.
- Broker acceptance does **not** prove database storage. A no-matching-subscribers acknowledgement is accepted, with a warning.

**The consumer must use the payload's `Timestamp` as the measurement time.** If it stores ingestion time instead, an accelerated day will be compressed into the real publishing interval.

## Layout and validation

- [`src/simulation.rs`](src/simulation.rs): virtual clock, operating point, seeded variation, bearing deterioration.
- [`src/mqtt.rs`](src/mqtt.rs): MQTT 5 publishing, metadata, backpressure, pacing, acknowledgement handling.
- [`src/main.rs`](src/main.rs): CLI, TLS/authentication settings, dry-run output.
- [`src/model.rs`](src/model.rs): embedded TD and fixed User Property names.
- [`tests/model_contract.rs`](tests/model_contract.rs): exact payload/model key and metadata agreement.
- [`tests/mqtt_publish.rs`](tests/mqtt_publish.rs): local MQTT 5 wire tests, including delivery failure, final-ACK gating, pacing, and a full in-flight window. No external broker is required.

```text
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

TLS/mTLS support still needs an end-to-end check against the actual deployment's broker and credentials.
