use std::{
    fs,
    io::{self, BufWriter, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, TimeDelta, Utc};
use clap::Parser;
use pump_generator::{mqtt, simulation::Simulation};
use rumqttc::{Transport, v5::MqttOptions};

#[derive(Parser)]
#[command(version, about = "Generate an accelerated pump history over MQTT 5")]
struct Args {
    /// Write newline-delimited JSON to stdout without connecting to a broker.
    #[arg(long)]
    dry_run: bool,

    /// WoT-TD URL or registry identifier stamped on every MQTT message.
    #[arg(long, required_unless_present = "dry_run")]
    model_reference: Option<String>,

    /// First event timestamp (RFC 3339); default: yesterday at 00:00:00 UTC.
    #[arg(long)]
    start_time: Option<DateTime<Utc>>,

    /// Virtual duration; the bearing deteriorates during the final 12 hours.
    #[arg(long, default_value_t = 24, value_parser = clap::value_parser!(u32).range(12..))]
    duration_hours: u32,

    /// Virtual milliseconds between samples, independent of publication speed.
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u64).range(1..))]
    sample_interval_ms: u64,

    /// Seed for repeatable smooth measurement variation.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Optional real-time publication cap; omission means maximum speed.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    max_messages_per_second: Option<u32>,

    /// Broker hostname or IP address, without a scheme or port.
    #[arg(long, default_value = "localhost")]
    broker_host: String,

    /// Broker TCP port; defaults to 1883, or 8883 with --tls.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    broker_port: Option<u16>,

    #[arg(long, default_value = "pumps/pump-001/telemetry")]
    topic: String,

    /// MQTT client identifier; default includes this process's ID.
    #[arg(long)]
    client_id: Option<String>,

    /// Enable TLS with certificate and hostname verification.
    #[arg(long)]
    tls: bool,

    /// PEM CA bundle; omission uses system roots.
    #[arg(long, requires = "tls")]
    ca_cert: Option<PathBuf>,

    /// PEM client certificate chain for mutual TLS; requires a CA bundle.
    #[arg(long, requires_all = ["tls", "ca_cert", "client_key"])]
    client_cert: Option<PathBuf>,

    /// PEM private key for mutual TLS.
    #[arg(long, requires = "client_cert")]
    client_key: Option<PathBuf>,

    #[arg(long, env = "PUMP_MQTT_USERNAME", hide_env_values = true)]
    username: Option<String>,

    /// Prefer PUMP_MQTT_PASSWORD rather than exposing a password in shell history.
    #[arg(
        long,
        env = "PUMP_MQTT_PASSWORD",
        hide_env_values = true,
        requires = "username"
    )]
    password: Option<String>,

    /// Fail if connection/delivery makes no progress for this many seconds.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u32).range(1..))]
    progress_timeout_seconds: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let start = args.start_time.unwrap_or_else(default_start_time);
    let simulation = Simulation::new(
        start,
        args.duration_hours,
        args.sample_interval_ms,
        args.seed,
    )?;
    let started = Instant::now();
    eprintln!(
        "Generating {} samples over {} simulated hours, starting {start}",
        simulation.sample_count(),
        args.duration_hours
    );

    if args.dry_run {
        let mut output = BufWriter::new(io::stdout().lock());
        write_json_lines(&simulation, &mut output)?;
    } else {
        let options = mqtt_options(&args)?;
        mqtt::publish(
            &simulation,
            options,
            &args.topic,
            args.model_reference
                .as_deref()
                .context("missing model reference")?,
            args.max_messages_per_second,
            Duration::from_secs(u64::from(args.progress_timeout_seconds)),
        )
        .await?;
    }
    eprintln!(
        "Completed {} samples in {:.2}s ({:.0} samples/s)",
        simulation.sample_count(),
        started.elapsed().as_secs_f64(),
        simulation.sample_count() as f64 / started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn default_start_time() -> DateTime<Utc> {
    let yesterday = Utc::now().date_naive() - TimeDelta::days(1);
    yesterday.and_hms_opt(0, 0, 0).unwrap().and_utc()
}

fn mqtt_options(args: &Args) -> Result<MqttOptions> {
    let client_id = args
        .client_id
        .clone()
        .unwrap_or_else(|| format!("pump-generator-{}", std::process::id()));
    validate_connect_fields(
        &client_id,
        args.username.as_deref(),
        args.password.as_deref(),
    )?;
    ensure!(
        !args.broker_host.trim().is_empty() && !args.broker_host.contains("://"),
        "broker host must be a hostname or IP address without a URL scheme"
    );
    let port = args
        .broker_port
        .unwrap_or(if args.tls { 8883 } else { 1883 });
    let mut options = MqttOptions::new(client_id, &args.broker_host, port);
    options.set_keep_alive(Duration::from_secs(15));
    options.set_clean_start(true);
    options.set_connection_timeout(u64::from(args.progress_timeout_seconds));
    if let Some(username) = &args.username {
        if !args.tls {
            eprintln!("Warning: MQTT credentials will be sent over an unencrypted connection");
        }
        options.set_credentials(username, args.password.as_deref().unwrap_or(""));
    }
    if args.tls {
        let transport = match &args.ca_cert {
            Some(path) => {
                let ca = fs::read(path)
                    .with_context(|| format!("could not read CA bundle {}", path.display()))?;
                let client_auth = match (&args.client_cert, &args.client_key) {
                    (Some(cert), Some(key)) => Some((
                        fs::read(cert).with_context(|| {
                            format!("could not read client certificate {}", cert.display())
                        })?,
                        fs::read(key).with_context(|| {
                            format!("could not read client key {}", key.display())
                        })?,
                    )),
                    _ => None,
                };
                Transport::tls(ca, client_auth, None)
            }
            None => Transport::tls_with_default_config(),
        };
        options.set_transport(transport);
    }
    Ok(options)
}

fn validate_connect_fields(
    client_id: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<()> {
    ensure!(!client_id.is_empty(), "client ID must not be empty");
    for (name, value) in [("client ID", Some(client_id)), ("username", username)] {
        if let Some(value) = value {
            ensure!(
                value.len() <= u16::MAX as usize,
                "{name} exceeds MQTT's 65535-byte limit"
            );
            ensure!(
                !value.contains('\0'),
                "{name} must not contain NUL characters"
            );
        }
    }
    if let Some(password) = password {
        ensure!(
            password.len() <= u16::MAX as usize,
            "password exceeds MQTT's 65535-byte limit"
        );
    }
    Ok(())
}

fn write_json_lines(simulation: &Simulation, output: &mut impl Write) -> Result<()> {
    for sample in simulation.samples() {
        serde_json::to_writer(&mut *output, &sample).context("could not write telemetry JSON")?;
        writeln!(output).context("could not write telemetry line ending")?;
    }
    output.flush().context("could not flush telemetry output")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_requires_model_reference_only_for_mqtt() {
        assert!(Args::try_parse_from(["pump_generator"]).is_err());
        assert!(Args::try_parse_from(["pump_generator", "--dry-run"]).is_ok());
        assert!(
            Args::try_parse_from(["pump_generator", "--model-reference", "urn:test:1"]).is_ok()
        );
    }

    #[test]
    fn cli_rejects_invalid_timing_and_incomplete_tls_settings() {
        for extra in [
            vec!["--duration-hours", "11"],
            vec!["--sample-interval-ms", "0"],
            vec!["--max-messages-per-second", "0"],
            vec!["--progress-timeout-seconds", "0"],
            vec!["--ca-cert", "ca.pem"],
            vec!["--tls", "--client-cert", "cert.pem"],
        ] {
            let mut args = vec!["pump_generator", "--dry-run"];
            args.extend(extra);
            assert!(Args::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn port_defaults_follow_transport() {
        let args = Args::try_parse_from(["pump_generator", "--dry-run"]).unwrap();
        assert_eq!(mqtt_options(&args).unwrap().broker_address().1, 1883);
        let args = Args::try_parse_from(["pump_generator", "--dry-run", "--tls"]).unwrap();
        assert_eq!(mqtt_options(&args).unwrap().broker_address().1, 8883);
    }

    #[test]
    fn connect_fields_respect_mqtt_byte_lengths_and_utf8_rules() {
        let at_limit = "x".repeat(65_535);
        let too_long = "x".repeat(65_536);
        assert!(validate_connect_fields(&at_limit, Some(&at_limit), Some(&at_limit)).is_ok());
        assert!(validate_connect_fields(&too_long, None, None).is_err());
        assert!(validate_connect_fields("pump", Some(&too_long), None).is_err());
        assert!(validate_connect_fields("pump", None, Some(&too_long)).is_err());
        assert!(validate_connect_fields("", None, None).is_err());
        assert!(validate_connect_fields("pump\0", None, None).is_err());
        assert!(validate_connect_fields("pump", Some("user\0"), None).is_err());
        // MQTT passwords are binary data, unlike client IDs and usernames.
        assert!(validate_connect_fields("pump", Some("user"), Some("pass\0word")).is_ok());
        let multi_byte = "é".repeat(32_768);
        assert!(validate_connect_fields(&multi_byte, None, None).is_err());
    }

    #[test]
    fn dry_run_writes_one_complete_snapshot_per_line() {
        let start = "2026-10-28T00:00:00Z".parse().unwrap();
        let simulation = Simulation::new(start, 12, 3_600_000, 42).unwrap();
        let mut output = Vec::new();
        write_json_lines(&simulation, &mut output).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert_eq!(text.lines().count(), 12);
        let samples: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(samples[0]["Timestamp"], "2026-10-28T00:00:00.000Z");
        assert!(samples[11]["BT"].as_f64().unwrap() > 423.0);
    }
}
