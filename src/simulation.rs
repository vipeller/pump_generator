use std::f64::consts::TAU;

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use serde::Serialize;

const HOUR_MS: u64 = 3_600_000;
const DETERIORATION_MS: u64 = 12 * HOUR_MS;
pub const NORMAL_BEARING_MAX_K: f64 = 423.0;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Telemetry {
    #[serde(rename = "Timestamp")]
    pub timestamp: String,
    #[serde(rename = "BT")]
    pub bearing_temperature: f64,
    #[serde(rename = "DP")]
    pub differential_pressure: f64,
    #[serde(rename = "FT")]
    pub fluid_temperature: f64,
    #[serde(rename = "LV")]
    pub level: f64,
    #[serde(rename = "MF")]
    pub mass_flow: f64,
    #[serde(rename = "NS")]
    pub number_of_starts: u32,
    #[serde(rename = "PE")]
    pub pump_efficiency: f64,
    #[serde(rename = "PPI")]
    pub pump_power_input: f64,
    #[serde(rename = "FC")]
    pub fluid_cavitation: bool,
    #[serde(rename = "MO")]
    pub motor_overheat: bool,
}

#[derive(Debug)]
pub struct Simulation {
    start: DateTime<Utc>,
    duration_ms: u64,
    interval_ms: u64,
    seed: u64,
}

impl Simulation {
    pub fn new(
        start: DateTime<Utc>,
        duration_hours: u32,
        sample_interval_ms: u64,
        seed: u64,
    ) -> Result<Self> {
        ensure!(duration_hours >= 12, "duration must be at least 12 hours");
        ensure!(sample_interval_ms > 0, "sample interval must be positive");
        ensure!(
            start.timestamp_subsec_nanos().is_multiple_of(1_000_000),
            "start time must have at most millisecond precision"
        );
        let duration_ms = u64::from(duration_hours) * HOUR_MS;
        let end = start
            .checked_add_signed(TimeDelta::milliseconds(duration_ms as i64))
            .context("simulation end time is outside the supported timestamp range")?;
        // RFC 3339 uses four-digit years; chrono also supports dates outside that range.
        ensure!(
            (0..=9999).contains(&chrono::Datelike::year(&start))
                && (0..=9999).contains(&chrono::Datelike::year(&end)),
            "simulation timestamps must be within RFC 3339 years 0000..9999"
        );
        let simulation = Self {
            start,
            duration_ms,
            interval_ms: sample_interval_ms,
            seed,
        };
        let last_elapsed = (simulation.sample_count() - 1) * sample_interval_ms;
        ensure!(
            simulation.sample_at(last_elapsed).bearing_temperature > NORMAL_BEARING_MAX_K,
            "sample interval is too large to capture the final bearing-temperature anomaly"
        );
        Ok(simulation)
    }

    pub fn sample_count(&self) -> u64 {
        self.duration_ms.div_ceil(self.interval_ms)
    }

    pub fn samples(&self) -> impl Iterator<Item = Telemetry> + '_ {
        (0..self.sample_count()).map(|index| self.sample_at(index * self.interval_ms))
    }

    fn sample_at(&self, elapsed_ms: u64) -> Telemetry {
        let seconds = elapsed_ms as f64 / 1000.0;
        let wave = |period_seconds: f64| (TAU * seconds / period_seconds).sin();
        let noise = |channel: u64| smooth_noise(self.seed ^ channel, seconds / 60.0);
        let load = 0.7 * wave(2400.0) + 0.2 * wave(480.0) + 0.1 * noise(1);
        let deterioration_start = self.duration_ms - DETERIORATION_MS;
        let deterioration =
            elapsed_ms.saturating_sub(deterioration_start) as f64 / DETERIORATION_MS as f64;
        let bearing_temperature = 333.0 + 108.0 * deterioration + 0.25 * load + 0.1 * noise(2);
        let mass_flow = 7.7 + 0.18 * load;
        let differential_pressure = 234_000.0 + 6000.0 * load;
        let pump_efficiency = 71.0 + 0.6 * wave(21_600.0) + 0.15 * noise(3);
        // Water-like density of 1000 kg/m³; hydraulic output = pressure × volumetric flow.
        let pump_power_input =
            differential_pressure * (mass_flow / 1000.0) / (pump_efficiency / 100.0);
        let timestamp = self.start + TimeDelta::milliseconds(elapsed_ms as i64);

        Telemetry {
            timestamp: timestamp.to_rfc3339_opts(SecondsFormat::Millis, true),
            bearing_temperature: round(bearing_temperature, 3),
            differential_pressure: round(differential_pressure, 2),
            fluid_temperature: round(317.12 + 0.4 * load + 0.12 * noise(4), 3),
            level: round(2.98 + 0.1 * wave(14_400.0) + 0.02 * noise(5), 3),
            mass_flow: round(mass_flow, 3),
            number_of_starts: 86,
            pump_efficiency: round(pump_efficiency, 3),
            pump_power_input: round(pump_power_input, 2),
            fluid_cavitation: false,
            motor_overheat: false,
        }
    }
}

fn round(value: f64, decimals: i32) -> f64 {
    let scale = 10_f64.powi(decimals);
    (value * scale).round() / scale
}

fn smooth_noise(seed: u64, position: f64) -> f64 {
    let index = position.floor() as u64;
    let fraction = position.fract();
    let weight = fraction * fraction * (3.0 - 2.0 * fraction);
    let left = noise_at(seed, index);
    let right = noise_at(seed, index + 1);
    left + (right - left) * weight
}

fn noise_at(seed: u64, index: u64) -> f64 {
    let mut value = seed.wrapping_add(index.wrapping_mul(0x9e3779b97f4a7c15));
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^= value >> 31;
    (value >> 11) as f64 / ((1_u64 << 53) - 1) as f64 * 2.0 - 1.0
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn start() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 28, 0, 0, 0).unwrap()
    }

    #[test]
    fn day_is_start_inclusive_end_exclusive() {
        let simulation = Simulation::new(start(), 24, 1000, 42).unwrap();
        assert_eq!(simulation.sample_count(), 86_400);
        let first = simulation.samples().next().unwrap();
        let last = simulation.samples().last().unwrap();
        assert_eq!(first.timestamp, "2026-10-28T00:00:00.000Z");
        assert_eq!(last.timestamp, "2026-10-28T23:59:59.000Z");
    }

    #[test]
    fn bearing_is_stable_then_crosses_normal_limit_near_hour_22() {
        let simulation = Simulation::new(start(), 24, 1000, 42).unwrap();
        let mut first_exceedance = None;
        for (index, sample) in simulation.samples().enumerate() {
            if index < 43_200 {
                assert!((332.6..333.4).contains(&sample.bearing_temperature));
            }
            if sample.bearing_temperature > NORMAL_BEARING_MAX_K {
                first_exceedance.get_or_insert(index);
            }
            if index >= 23 * 3600 {
                assert!(sample.bearing_temperature > NORMAL_BEARING_MAX_K);
            }
        }
        let crossing = first_exceedance.expect("bearing must exceed normal limit");
        assert!(crossing.abs_diff(22 * 3600) <= 180);
        assert!(simulation.samples().last().unwrap().bearing_temperature > 440.0);
    }

    #[test]
    fn deterioration_is_relative_to_end_for_longer_runs() {
        let simulation = Simulation::new(start(), 48, 60_000, 42).unwrap();
        assert!(simulation.sample_at(35 * HOUR_MS).bearing_temperature < 334.0);
        assert!(simulation.sample_at(47 * HOUR_MS).bearing_temperature > 423.0);
    }

    #[test]
    fn other_measurements_remain_normal_and_power_is_consistent() {
        for sample in Simulation::new(start(), 24, 1000, 42).unwrap().samples() {
            assert!((0.0..=400_000.0).contains(&sample.differential_pressure));
            assert!((263.0..=395.0).contains(&sample.fluid_temperature));
            assert!((0.0..=5.0).contains(&sample.level));
            assert!((0.0..=10.0).contains(&sample.mass_flow));
            assert!((0.0..=100.0).contains(&sample.pump_efficiency));
            assert!((0.0..=4000.0).contains(&sample.pump_power_input));
            assert_eq!(sample.number_of_starts, 86);
            assert!(!sample.fluid_cavitation);
            assert!(!sample.motor_overheat);
            let expected = sample.differential_pressure * sample.mass_flow
                / 1000.0
                / (sample.pump_efficiency / 100.0);
            assert!((expected - sample.pump_power_input).abs() < 0.25);
        }
    }

    #[test]
    fn seed_and_virtual_time_determine_samples_not_sampling_cadence() {
        let dense = Simulation::new(start(), 24, 1000, 42).unwrap();
        let sparse = Simulation::new(start(), 24, 60_000, 42).unwrap();
        assert_eq!(dense.sample_at(3_600_000), sparse.sample_at(3_600_000));
        assert_eq!(dense.sample_at(0), dense.sample_at(0));
        let other_seed = Simulation::new(start(), 24, 1000, 43).unwrap();
        assert_ne!(dense.sample_at(0), other_seed.sample_at(0));
    }

    #[test]
    fn invalid_configurations_fail_before_generating() {
        assert!(Simulation::new(start(), 11, 1000, 42).is_err());
        assert!(Simulation::new(start(), 24, 0, 42).is_err());
        assert!(Simulation::new(start(), 24, 24 * HOUR_MS, 42).is_err());
        assert!(Simulation::new(start(), u32::MAX, 1000, 42).is_err());
        let submillisecond = start() + TimeDelta::nanoseconds(1);
        assert!(Simulation::new(submillisecond, 24, 1000, 42).is_err());
    }

    #[test]
    fn non_divisible_sampling_does_not_exceed_end() {
        let simulation = Simulation::new(start(), 24, 70_000, 42).unwrap();
        assert_eq!(simulation.sample_count(), 1235);
        let last = simulation.samples().last().unwrap();
        let timestamp = DateTime::parse_from_rfc3339(&last.timestamp).unwrap();
        assert!(timestamp < start() + TimeDelta::hours(24));
        assert!(timestamp + TimeDelta::milliseconds(70_000) >= start() + TimeDelta::hours(24));
    }
}
