use std::{collections::HashSet, future::Future, pin::Pin, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, SecondsFormat, TimeDelta, TimeZone, Utc};
use pump_generator::{model::MODEL_REFERENCE_PROPERTY_NAME, mqtt::publish, simulation::Simulation};
use rumqttc::v5::{
    MqttOptions,
    mqttbytes::{
        QoS,
        v5::{Disconnect, DisconnectReasonCode, Packet, Publish},
    },
};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Instant, sleep, timeout},
};

const TOPIC: &str = "pumps/wire-test/telemetry";
const MODEL_REFERENCE: &str = "urn:pump-generator:model:wire-test:1";
const IO_TIMEOUT: Duration = Duration::from_secs(3);
const TEST_TIMEOUT: Duration = Duration::from_secs(12);
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);
const CONNACK: &[u8] = &[0x20, 0x03, 0x00, 0x00, 0x00];
// MQTT 5 Server Keep Alive property: one second, so the timeout test sees real pings.
const CONNACK_WITH_KEEPALIVE: &[u8] = &[0x20, 0x06, 0x00, 0x00, 0x03, 0x13, 0x00, 0x01];

fn start() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).single().unwrap()
}

async fn setup() -> Result<(TcpListener, MqttOptions, Simulation)> {
    let listener = timeout(IO_TIMEOUT, TcpListener::bind(("127.0.0.1", 0)))
        .await
        .context("binding test listener timed out")??;
    let options = MqttOptions::new("mqtt-wire-test", "127.0.0.1", listener.local_addr()?.port());
    let simulation = Simulation::new(start(), 12, 3_600_000, 42)?;
    assert_eq!(simulation.sample_count(), 12);
    Ok((listener, options, simulation))
}

// Keep the publisher in this task: cancelling the outer test timeout drops both
// the publisher and the socket, rather than leaving a spawned task running.
async fn before_completion<T>(
    publishing: Pin<&mut impl Future<Output = Result<()>>>,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {
        result = publishing => bail!("publisher completed before the broker was ready: {result:?}"),
        result = operation => result,
    }
}

struct Broker {
    stream: TcpStream,
}

impl Broker {
    async fn accept(listener: &TcpListener, connack: &[u8]) -> Result<Self> {
        let (stream, _) = timeout(IO_TIMEOUT, listener.accept())
            .await
            .context("accepting MQTT connection timed out")??;
        let mut broker = Self { stream };
        // The v5 decoder validates both the MQTT protocol name and level 5.
        match broker.packet().await? {
            Packet::Connect(connect, _, _) => assert_eq!(connect.client_id, "mqtt-wire-test"),
            other => bail!("expected MQTT 5 CONNECT, got {other:?}"),
        }
        broker.send(connack).await?;
        Ok(broker)
    }

    async fn send(&mut self, bytes: &[u8]) -> Result<()> {
        timeout(IO_TIMEOUT, self.stream.write_all(bytes))
            .await
            .context("writing broker packet timed out")?
            .context("writing broker packet")
    }

    async fn acknowledge(&mut self, packet_id: u16, reason: u8) -> Result<()> {
        let [high, low] = packet_id.to_be_bytes();
        self.send(&[0x40, 0x04, high, low, reason, 0x00]).await
    }

    async fn packet(&mut self) -> Result<Packet> {
        let frame = timeout(IO_TIMEOUT, async {
            let mut frame = vec![self.stream.read_u8().await?];
            let mut remaining = 0_usize;
            for shift in [0, 7, 14, 21] {
                let byte = self.stream.read_u8().await?;
                frame.push(byte);
                remaining |= usize::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    ensure!(remaining <= 64 * 1024, "unexpectedly large test packet");
                    let header_length = frame.len();
                    frame.resize(header_length + remaining, 0);
                    self.stream.read_exact(&mut frame[header_length..]).await?;
                    return Ok(frame);
                }
            }
            bail!("invalid MQTT remaining length")
        })
        .await
        .context("reading client packet timed out")??;

        // rumqttc 0.25.1's top-level decoder rejects the valid two-byte DISCONNECT.
        if frame == [0xe0, 0x00] {
            return Ok(Packet::Disconnect(Disconnect::new(
                DisconnectReasonCode::NormalDisconnection,
            )));
        }
        // The inferred conversion supplies the decoder's BytesMut without needing
        // a direct bytes dependency just for this test.
        Packet::read(&mut frame.as_slice().into(), Some(64 * 1024))
            .context("decoding client MQTT 5 packet")
    }

    async fn publication(&mut self) -> Result<Publish> {
        match self.packet().await? {
            Packet::Publish(publication) => Ok(publication),
            other => bail!("expected PUBLISH, got {other:?}"),
        }
    }

    async fn expect_disconnect(&mut self) -> Result<()> {
        match self.packet().await? {
            Packet::Disconnect(disconnect) => {
                assert_eq!(
                    disconnect.reason_code,
                    DisconnectReasonCode::NormalDisconnection
                );
                Ok(())
            }
            other => bail!("expected DISCONNECT after all PUBACKs, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn publishes_all_snapshots_and_waits_for_final_puback_before_disconnect() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let (listener, options, simulation) = setup().await?;
        let expected = simulation
            .samples()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?;
        assert_eq!(expected.len(), 12);
        let publishing = publish(
            &simulation,
            options,
            TOPIC,
            MODEL_REFERENCE,
            None,
            PROGRESS_TIMEOUT,
        );
        tokio::pin!(publishing);
        let mut broker =
            before_completion(publishing.as_mut(), Broker::accept(&listener, CONNACK)).await?;
        let mut packet_ids = HashSet::new();
        let mut last_packet_id = 0;
        for (index, expected_snapshot) in expected.iter().enumerate() {
            let publication = before_completion(publishing.as_mut(), broker.publication()).await?;
            assert_eq!(publication.topic.as_ref(), TOPIC.as_bytes());
            assert_eq!(publication.qos, QoS::AtLeastOnce);
            assert!(!publication.retain);
            assert!(!publication.dup);
            assert_ne!(publication.pkid, 0);
            assert!(
                packet_ids.insert(publication.pkid),
                "packet ID reused while in flight"
            );
            let properties = publication
                .properties
                .as_ref()
                .context("missing PUBLISH properties")?;
            assert_eq!(
                properties.user_properties,
                vec![(
                    MODEL_REFERENCE_PROPERTY_NAME.to_owned(),
                    MODEL_REFERENCE.to_owned()
                )]
            );
            assert_eq!(properties.content_type.as_deref(), Some("application/json"));
            assert_eq!(properties.payload_format_indicator, Some(1));
            let snapshot: Value = serde_json::from_slice(&publication.payload)?;
            assert_eq!(
                &snapshot, expected_snapshot,
                "snapshot {index} differs from simulation"
            );
            assert_eq!(
                snapshot["Timestamp"],
                (start() + TimeDelta::hours(index as i64))
                    .to_rfc3339_opts(SecondsFormat::Millis, true)
            );
            if index == 11 {
                assert!(snapshot["BT"].as_f64().context("BT must be numeric")? > 423.0);
                last_packet_id = publication.pkid;
            } else {
                before_completion(
                    publishing.as_mut(),
                    broker.acknowledge(publication.pkid, 0x00),
                )
                .await?;
            }
        }
        assert_eq!(packet_ids.len(), 12);

        // All payloads have been received and the first eleven acknowledged, but
        // neither successful completion nor DISCONNECT is allowed before ACK 12.
        tokio::select! {
            result = publishing.as_mut() => bail!("completed without final PUBACK: {result:?}"),
            packet = broker.packet() => bail!("sent a packet before final PUBACK: {packet:?}"),
            _ = sleep(Duration::from_millis(200)) => {}
        }
        broker.acknowledge(last_packet_id, 0x00).await?;
        let disconnect = async {
            match broker.packet().await? {
                Packet::Disconnect(disconnect) => {
                    assert_eq!(
                        disconnect.reason_code,
                        DisconnectReasonCode::NormalDisconnection
                    );
                    Ok(())
                }
                other => bail!("expected DISCONNECT after all 12 PUBACKs, got {other:?}"),
            }
        };
        tokio::try_join!(publishing.as_mut(), disconnect)?;
        Ok(())
    })
    .await
    .context("snapshot delivery test timed out")?
}

#[tokio::test]
async fn paced_publications_allow_intentional_sleep_with_short_progress_timeout() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let (listener, options, _) = setup().await?;
        let simulation = Simulation::new(start(), 12, 19_800_000, 42)?;
        assert_eq!(simulation.sample_count(), 3);
        let publishing = publish(
            &simulation,
            options,
            TOPIC,
            MODEL_REFERENCE,
            Some(1),
            Duration::from_secs(1),
        );
        tokio::pin!(publishing);
        let mut broker =
            before_completion(publishing.as_mut(), Broker::accept(&listener, CONNACK)).await?;
        let mut previous_received = None;
        for (index, offset_minutes) in [0, 330, 660].into_iter().enumerate() {
            let publication = before_completion(publishing.as_mut(), broker.publication()).await?;
            let received = Instant::now();
            if let Some(previous) = previous_received {
                let interval = received.duration_since(previous);
                assert!(
                    interval >= Duration::from_millis(950),
                    "paced publication {index} arrived too soon: {interval:?}"
                );
            }
            previous_received = Some(received);
            // ACK immediately: the one-second gap must not trip the one-second
            // progress timeout while the producer deliberately sleeps.
            before_completion(
                publishing.as_mut(),
                broker.acknowledge(publication.pkid, 0x00),
            )
            .await?;
            let snapshot: Value = serde_json::from_slice(&publication.payload)?;
            assert_eq!(
                snapshot["Timestamp"],
                (start() + TimeDelta::minutes(offset_minutes))
                    .to_rfc3339_opts(SecondsFormat::Millis, true)
            );
            if index == 2 {
                assert!(snapshot["BT"].as_f64().context("BT must be numeric")? > 423.0);
            }
        }
        tokio::try_join!(publishing.as_mut(), broker.expect_disconnect())?;
        Ok(())
    })
    .await
    .context("paced publication test timed out")?
}

#[tokio::test]
async fn full_inflight_window_blocks_then_delivers_all_144_snapshots() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let (listener, mut options, _) = setup().await?;
        options.set_keep_alive(Duration::from_secs(15));
        let simulation = Simulation::new(start(), 12, 300_000, 42)?;
        let expected = simulation
            .samples()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?;
        assert_eq!(expected.len(), 144);
        let publishing = publish(
            &simulation,
            options,
            TOPIC,
            MODEL_REFERENCE,
            None,
            PROGRESS_TIMEOUT,
        );
        tokio::pin!(publishing);
        let mut broker =
            before_completion(publishing.as_mut(), Broker::accept(&listener, CONNACK)).await?;
        let mut withheld_ids = HashSet::new();
        for (index, expected_snapshot) in expected[..128].iter().enumerate() {
            let publication = before_completion(publishing.as_mut(), broker.publication()).await?;
            let snapshot: Value = serde_json::from_slice(&publication.payload)?;
            assert_eq!(&snapshot, expected_snapshot, "snapshot {index} lost or reordered");
            assert!(withheld_ids.insert(publication.pkid), "in-flight packet ID reused");
        }
        assert_eq!(withheld_ids.len(), 128);

        // Peek rather than partially consuming a frame: even one byte beyond
        // the full, unacknowledged window is unexpected. Keepalives take 15s.
        let mut byte = [0];
        tokio::select! {
            result = publishing.as_mut() => bail!("completed with a full unacknowledged window: {result:?}"),
            received = broker.stream.peek(&mut byte) => bail!("client sent data or closed a full window: {received:?}"),
            _ = sleep(Duration::from_millis(200)) => {}
        }
        for packet_id in withheld_ids {
            before_completion(publishing.as_mut(), broker.acknowledge(packet_id, 0x00)).await?;
        }
        for (index, expected_snapshot) in expected.iter().enumerate().skip(128) {
            let publication = before_completion(publishing.as_mut(), broker.publication()).await?;
            let snapshot: Value = serde_json::from_slice(&publication.payload)?;
            assert_eq!(&snapshot, expected_snapshot, "snapshot {index} lost or reordered");
            before_completion(
                publishing.as_mut(),
                broker.acknowledge(publication.pkid, 0x00),
            )
            .await?;
        }
        tokio::try_join!(publishing.as_mut(), broker.expect_disconnect())?;
        Ok(())
    })
    .await
    .context("in-flight backpressure test timed out")?
}

#[tokio::test]
async fn not_authorized_puback_fails_promptly() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let (listener, options, simulation) = setup().await?;
        let publishing = publish(
            &simulation,
            options,
            TOPIC,
            MODEL_REFERENCE,
            None,
            PROGRESS_TIMEOUT,
        );
        tokio::pin!(publishing);
        let mut broker =
            before_completion(publishing.as_mut(), Broker::accept(&listener, CONNACK)).await?;
        let publication = before_completion(publishing.as_mut(), broker.publication()).await?;
        // MQTT 5 PUBACK reason 0x87 is Not Authorized. Keep the socket open so
        // only the rejected ACK, not an incidental EOF, can explain the error.
        broker.acknowledge(publication.pkid, 0x87).await?;
        let error = timeout(IO_TIMEOUT, publishing.as_mut())
            .await
            .context("rejected PUBACK did not fail promptly")?
            .expect_err("NotAuthorized must not count as successful delivery");
        assert!(format!("{error:#}").contains("NotAuthorized"), "{error:#}");
        Ok(())
    })
    .await
    .context("negative PUBACK test timed out")?
}

#[tokio::test]
async fn closed_connection_fails_without_waiting_for_progress_timeout() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let (listener, options, simulation) = setup().await?;
        let publishing = publish(
            &simulation,
            options,
            TOPIC,
            MODEL_REFERENCE,
            None,
            PROGRESS_TIMEOUT,
        );
        tokio::pin!(publishing);
        let mut broker =
            before_completion(publishing.as_mut(), Broker::accept(&listener, CONNACK)).await?;
        before_completion(publishing.as_mut(), broker.publication()).await?;
        drop(broker);
        let error = timeout(IO_TIMEOUT, publishing.as_mut())
            .await
            .context("socket closure did not fail promptly")?
            .expect_err("losing an unacknowledged publication must fail");
        assert!(
            format!("{error:#}").contains("MQTT connection failed"),
            "{error:#}"
        );
        Ok(())
    })
    .await
    .context("connection failure test timed out")?
}

#[tokio::test]
async fn missing_pubacks_time_out_even_while_keepalives_succeed() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let (listener, options, simulation) = setup().await?;
        let publishing = publish(
            &simulation,
            options,
            TOPIC,
            MODEL_REFERENCE,
            None,
            Duration::from_secs(3),
        );
        tokio::pin!(publishing);
        let mut broker = before_completion(
            publishing.as_mut(),
            Broker::accept(&listener, CONNACK_WITH_KEEPALIVE),
        )
        .await?;
        for _ in 0..12 {
            before_completion(publishing.as_mut(), broker.publication()).await?;
        }

        let mut pings = 0;
        let result = timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    biased;
                    result = publishing.as_mut() => return result,
                    packet = broker.packet() => match packet? {
                        Packet::PingReq(_) => {
                            pings += 1;
                            broker.send(&[0xd0, 0x00]).await?;
                        }
                        other => bail!("unexpected packet while withholding every PUBACK: {other:?}"),
                    }
                }
            }
        })
        .await
        .context("missing PUBACKs did not trigger a bounded progress timeout")?;
        let error = result.expect_err("keepalives cannot substitute for delivery acknowledgements");
        assert!(pings > 0, "test must exercise PINGREQ/PINGRESP traffic");
        assert!(format!("{error:#}").contains("no MQTT delivery progress"), "{error:#}");
        Ok(())
    })
    .await
    .context("missing PUBACK timeout test timed out")?
}
