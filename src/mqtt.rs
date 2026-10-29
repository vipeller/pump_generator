use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use rumqttc::{
    Outgoing,
    v5::{
        AsyncClient, Event, MqttOptions,
        mqttbytes::{
            QoS,
            v5::{Packet, PubAckReason, PublishProperties},
        },
    },
};
use tokio::{
    sync::mpsc,
    time::{Instant, sleep_until, timeout_at},
};

use crate::{
    model::{ASSET_ID_PROPERTY_NAME, MODEL_REFERENCE_PROPERTY_NAME},
    simulation::Simulation,
};

const QUEUE_CAPACITY: usize = 128;

pub fn telemetry_properties(model_reference: &str, asset_id: &str) -> PublishProperties {
    PublishProperties {
        payload_format_indicator: Some(1),
        content_type: Some("application/json".into()),
        user_properties: vec![
            (MODEL_REFERENCE_PROPERTY_NAME.into(), model_reference.into()),
            (ASSET_ID_PROPERTY_NAME.into(), asset_id.into()),
        ],
        ..PublishProperties::default()
    }
}

pub async fn publish(
    simulation: &Simulation,
    mut options: MqttOptions,
    topic: &str,
    model_reference: &str,
    asset_id: &str,
    max_messages_per_second: Option<u32>,
    progress_timeout: Duration,
) -> Result<()> {
    validate_parameters(
        topic,
        model_reference,
        asset_id,
        max_messages_per_second,
        progress_timeout,
    )?;
    options.set_outgoing_inflight_upper_limit(QUEUE_CAPACITY as u16);
    let (client, mut eventloop) = AsyncClient::new(options, QUEUE_CAPACITY);
    let expected = simulation.sample_count();
    let properties = telemetry_properties(model_reference, asset_id);
    let spacing = max_messages_per_second
        .map(|rate| Duration::from_nanos(1_000_000_000_u64.div_ceil(u64::from(rate))));
    let (sent_tx, mut sent_rx) = mpsc::channel(1);
    // An intentional gap between publications is not a stalled connection.
    let delivery_timeout = progress_timeout.saturating_add(spacing.unwrap_or_default());

    let produce = async {
        let mut next_send = Instant::now();
        for sample in simulation.samples() {
            if spacing.is_some() {
                sleep_until(next_send).await;
            }
            let payload = serde_json::to_vec(&sample).context("could not serialize telemetry")?;
            client
                .publish_with_properties(
                    topic,
                    QoS::AtLeastOnce,
                    false,
                    payload,
                    properties.clone(),
                )
                .await
                .context("could not queue telemetry for MQTT publication")?;
            if let Some(spacing) = spacing {
                sent_rx
                    .recv()
                    .await
                    .context("MQTT send notification channel closed")?;
                // Pace actual writes, not queue insertion; backpressure must not create bursts.
                next_send = Instant::now() + spacing;
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    let drive = async {
        let mut acknowledged = 0_u64;
        let mut warned_no_subscribers = false;
        let mut deadline = Instant::now() + delivery_timeout;
        loop {
            let event = timeout_at(deadline, eventloop.poll())
                .await
                .with_context(|| {
                    format!(
                        "no MQTT delivery progress within {}s ({acknowledged}/{expected} acknowledged); dataset may be incomplete",
                        delivery_timeout.as_secs_f64()
                    )
                })?
                .with_context(|| {
                    format!(
                        "MQTT connection failed ({acknowledged}/{expected} acknowledged); dataset may be incomplete"
                    )
                })?;
            match event {
                Event::Incoming(Packet::ConnAck(_)) => {
                    deadline = Instant::now() + delivery_timeout;
                    eprintln!("Connected using MQTT 5; publishing {expected} samples to {topic}");
                }
                Event::Incoming(Packet::PubAck(ack)) => {
                    match ack.reason {
                        PubAckReason::Success => {}
                        PubAckReason::NoMatchingSubscribers => {
                            if !warned_no_subscribers {
                                eprintln!(
                                    "Warning: broker reports no matching subscribers; broker acceptance does not confirm database storage"
                                );
                                warned_no_subscribers = true;
                            }
                        }
                        reason => bail!(
                            "broker rejected a publication: {reason:?} ({acknowledged}/{expected} acknowledged)"
                        ),
                    }
                    acknowledged += 1;
                    deadline = Instant::now() + delivery_timeout;
                    if acknowledged.is_multiple_of(10_000) || acknowledged == expected {
                        eprintln!("Broker acknowledged {acknowledged}/{expected} samples");
                    }
                    if acknowledged == expected {
                        client
                            .disconnect()
                            .await
                            .context("could not queue MQTT disconnect")?;
                    }
                }
                Event::Incoming(Packet::Disconnect(_)) => {
                    bail!(
                        "broker disconnected before graceful completion ({acknowledged}/{expected} acknowledged)"
                    );
                }
                Event::Outgoing(Outgoing::Publish(_)) if spacing.is_some() => {
                    sent_tx
                        .try_send(())
                        .context("unexpected MQTT send notification backlog")?;
                }
                Event::Outgoing(Outgoing::Disconnect) => {
                    ensure!(
                        acknowledged == expected,
                        "disconnected with unacknowledged telemetry"
                    );
                    return Ok::<(), anyhow::Error>(());
                }
                _ => {}
            }
        }
    };

    // Polling both futures together keeps networking active while bounded queues apply backpressure.
    tokio::try_join!(produce, drive)?;
    Ok(())
}

fn validate_parameters(
    topic: &str,
    model_reference: &str,
    asset_id: &str,
    max_messages_per_second: Option<u32>,
    progress_timeout: Duration,
) -> Result<()> {
    ensure!(
        !topic.is_empty() && topic.len() <= u16::MAX as usize && !topic.contains(['+', '#', '\0']),
        "topic must be a nonempty MQTT topic without wildcards or NUL characters (maximum 65535 bytes)"
    );
    for (name, value) in [("model reference", model_reference), ("asset ID", asset_id)] {
        ensure!(
            !value.trim().is_empty() && value.len() <= u16::MAX as usize && !value.contains('\0'),
            "{name} must be nonempty and contain no NUL characters (maximum 65535 bytes)"
        );
    }
    ensure!(
        max_messages_per_second != Some(0),
        "publish rate must be positive"
    );
    ensure!(
        !progress_timeout.is_zero(),
        "progress timeout must be positive"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publications_have_model_reference_and_asset_id_user_properties() {
        let properties = telemetry_properties("urn:test:model:1", "pump-001");
        assert_eq!(
            properties.user_properties,
            vec![
                (
                    MODEL_REFERENCE_PROPERTY_NAME.into(),
                    "urn:test:model:1".into()
                ),
                ("asset-id".into(), "pump-001".into()),
            ]
        );
        assert_eq!(properties.content_type.as_deref(), Some("application/json"));
        assert_eq!(properties.payload_format_indicator, Some(1));
    }

    #[test]
    fn user_property_values_respect_mqtt_byte_limits() {
        let timeout = Duration::from_secs(30);
        let at_limit = "x".repeat(65_535);
        let too_long = "x".repeat(65_536);
        let multibyte_too_long = "é".repeat(32_768);
        assert!(validate_parameters("pumps/1", &at_limit, &at_limit, None, timeout).is_ok());
        assert!(validate_parameters("pumps/1", &too_long, "pump-001", None, timeout).is_err());
        for asset_id in [&too_long, &multibyte_too_long] {
            assert!(validate_parameters("pumps/1", "urn:test:1", asset_id, None, timeout).is_err());
        }
    }

    #[test]
    fn invalid_publish_parameters_fail_before_connecting() {
        let timeout = Duration::from_secs(30);
        for topic in ["", "pumps/+/telemetry", "pumps/#", "pump\0"] {
            assert!(validate_parameters(topic, "urn:test:1", "pump-001", None, timeout).is_err());
        }
        for reference in ["", " ", "urn:test\0"] {
            assert!(validate_parameters("pumps/1", reference, "pump-001", None, timeout).is_err());
        }
        for asset_id in ["", " ", "pump\0"] {
            let error =
                validate_parameters("pumps/1", "urn:test:1", asset_id, None, timeout).unwrap_err();
            assert!(error.to_string().contains("asset ID"));
        }
        assert!(
            validate_parameters("pumps/1", "urn:test:1", "pump-001", Some(0), timeout).is_err()
        );
        assert!(
            validate_parameters("pumps/1", "urn:test:1", "pump-001", None, Duration::ZERO).is_err()
        );
        assert!(
            validate_parameters("pumps/1", "urn:test:1", "pump-001", Some(5000), timeout).is_ok()
        );
    }
}
