use std::collections::BTreeSet;

use chrono::{DateTime, TimeZone, Utc};
use pump_generator::{model::THING_DESCRIPTION, simulation::Simulation};
use serde_json::{Value, json};

fn thing_description() -> Value {
    serde_json::from_str(THING_DESCRIPTION).expect("embedded Thing Description must be valid JSON")
}

fn start() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).single().unwrap()
}

fn simulation() -> Simulation {
    Simulation::new(start(), 48, 60_000, 42)
        .expect("contract simulation configuration must be valid")
}

fn mqtt_form(href: &str, operation: &str) -> Value {
    json!({
        "href": href,
        "op": operation,
        "contentType": "application/json",
        "mqv:controlPacket": "subscribe",
        "mqv:filter": "pumps/pump-001/telemetry",
        "mqv:qos": "1",
        "mqv:retain": false
    })
}

fn assert_sample_shape(td: &Value, sample: &Value) {
    let properties = td["properties"].as_object().unwrap();
    let sample = sample.as_object().expect("telemetry must be a JSON object");
    assert_eq!(
        properties.keys().collect::<BTreeSet<_>>(),
        sample.keys().collect::<BTreeSet<_>>(),
        "TD property keys and serialized telemetry keys must match exactly"
    );
    for (key, schema) in properties {
        let value = &sample[key];
        let matches_type = match schema["type"].as_str().unwrap() {
            "string" => value.is_string(),
            "number" => value.is_number(),
            "integer" => value.is_i64() || value.is_u64(),
            "boolean" => value.is_boolean(),
            other => panic!("unexpected telemetry schema type {other} for {key}"),
        };
        assert!(matches_type, "{key} does not match its TD type: {value}");
    }
}

fn assert_utc_timestamp(sample: &Value) -> DateTime<Utc> {
    let timestamp = sample["Timestamp"]
        .as_str()
        .expect("Timestamp must be a string");
    let parsed = DateTime::parse_from_rfc3339(timestamp).expect("Timestamp must be RFC 3339");
    assert_eq!(
        parsed.offset().local_minus_utc(),
        0,
        "Timestamp must be UTC"
    );
    parsed.with_timezone(&Utc)
}

#[test]
fn td_identity_and_mqtt_forms_describe_the_simulator() {
    assert_eq!(
        THING_DESCRIPTION,
        include_str!("../models/pump-telemetry.td.json"),
        "public model constant must embed the model file unchanged"
    );
    let td = thing_description();
    assert_eq!(
        td["@context"],
        json!([
            "https://www.w3.org/2022/wot/td/v1.1",
            {
                "dov": "http://azure.com/IoT/operations/tm#",
                "mqv": "http://www.w3.org/2019/wot/mqtt#",
                "pg": "urn:pump-generator:metadata:"
            }
        ])
    );
    assert_eq!(td["@type"], "Thing");
    assert_eq!(td["id"], "urn:pump-generator:model:pump-telemetry:1.0.0");
    assert_eq!(td["title"], "Pump telemetry simulator");
    assert_eq!(td["version"]["instance"], "1.0.0");
    assert_eq!(td["security"], json!(["nosec_sc"]));
    assert_eq!(
        td["securityDefinitions"],
        json!({"nosec_sc": {"scheme": "nosec"}})
    );
    let aggregate_href = td["forms"][0]["href"].as_str().unwrap();
    assert!(aggregate_href.starts_with("mqtt://") || aggregate_href.starts_with("mqtts://"));
    assert_eq!(
        td["forms"],
        json!([mqtt_form(aggregate_href, "observeallproperties")])
    );
    assert!(td.get("links").is_none());
    assert!(td.get("dataSource").is_none());

    let properties = td["properties"].as_object().unwrap();
    let expected_keys: BTreeSet<_> = [
        "Timestamp",
        "BT",
        "DP",
        "FT",
        "LV",
        "MF",
        "NS",
        "PE",
        "PPI",
        "FC",
        "MO",
    ]
    .into_iter()
    .collect();
    assert_eq!(
        properties
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        expected_keys
    );
    for (key, property) in properties {
        assert_eq!(property["readOnly"], true, "{key} must be read-only");
        assert_eq!(property["observable"], true, "{key} must be observable");
        // TD 1.1 requires property forms even when an aggregate form is present.
        assert_eq!(
            property["forms"],
            json!([mqtt_form("mqtt://localhost:1883", "observeproperty")])
        );
        assert!(property.get("dataSource").is_none(), "{key}");
        assert!(property.get("links").is_none(), "{key}");
    }
    // Source addresses are descriptive annotations, never active simulator interfaces.
    for form in td["forms"].as_array().unwrap().iter().chain(
        properties
            .values()
            .flat_map(|property| property["forms"].as_array().unwrap()),
    ) {
        let href = form["href"].as_str().unwrap();
        assert!(href.starts_with("mqtt://") || href.starts_with("mqtts://"));
    }
}

#[test]
fn source_metadata_preserves_original_opc_ua_addresses() {
    let td = thing_description();
    let namespace = "nsu=urn:localhost:OPCFoundation:PumpDeviceIntegrationServer";
    assert_eq!(
        td["pg:opcUaSource"]["dov:dataSource"],
        format!("{namespace};i=3764731297")
    );
    for (key, identifier) in [
        ("BT", 2_051_551_167_u32),
        ("DP", 2_981_318_979),
        ("FT", 1_749_985_422),
        ("LV", 678_946_954),
        ("MF", 2_818_955_220),
        ("NS", 53_451_422),
        ("PE", 3_078_466_443),
        ("PPI", 1_071_090_168),
    ] {
        let source = &td["properties"][key]["pg:opcUaSource"];
        assert_eq!(
            source["href"],
            format!("/?id={namespace};i={identifier}"),
            "{key}"
        );
        assert!(source.get("placeholder").is_none(), "{key}");
    }
    assert!(
        td["properties"]["Timestamp"]
            .get("pg:opcUaSource")
            .is_none()
    );
}

#[test]
fn original_null_node_ids_are_explicit_source_placeholders() {
    let td = thing_description();
    for key in ["FC", "MO"] {
        assert_eq!(
            td["properties"][key]["pg:opcUaSource"],
            json!({"href": "/?id=i=0", "placeholder": true}),
            "{key} has no usable source node ID in the original export"
        );
    }
}

#[test]
fn property_metadata_preserves_titles_units_and_normal_operating_limits() {
    let td = thing_description();
    let properties = &td["properties"];
    for (key, name, unit, minimum, maximum) in [
        ("BT", "BearingTemperature", "Kelvin", 273, 423),
        ("DP", "DifferentialPressure", "Pascal", 0, 400_000),
        ("FT", "FluidTemperature", "Kelvin", 263, 395),
        ("LV", "Level", "meter", 0, 5),
        ("MF", "MassFlow", "kg/s", 0, 10),
        ("PE", "PumpEfficiency", "Percent", 0, 100),
        ("PPI", "PumpPowerInput", "Watt", 0, 4_000),
    ] {
        let property = &properties[key];
        assert_eq!(
            property["title"],
            format!("Operational.Measurements.{name}")
        );
        assert_eq!(property["type"], "number", "{key}");
        assert_eq!(property["dov:unit"], unit, "{key}");
        assert_eq!(property["dov:normalOperatingMinimum"], minimum, "{key}");
        assert_eq!(property["dov:normalOperatingMaximum"], maximum, "{key}");
        for bound in ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"] {
            assert!(
                property.get(bound).is_none(),
                "{key}: normal operating limits must not become schema bounds"
            );
        }
    }

    let starts = &properties["NS"];
    assert_eq!(starts["title"], "Operational.Measurements.NumberOfStarts");
    assert_eq!(starts["type"], "integer");
    assert_eq!(starts["minimum"], 0);
    for absent in [
        "maximum",
        "exclusiveMaximum",
        "dov:normalOperatingMinimum",
        "dov:normalOperatingMaximum",
    ] {
        assert!(starts.get(absent).is_none(), "NS has no operating range");
    }

    for (key, title) in [
        ("FC", "Events.SupervisionProcessFluid.Cavitation"),
        ("MO", "Events.SupervisionPumpOperation.MotorOverheat"),
    ] {
        assert_eq!(properties[key]["title"], title);
        assert_eq!(properties[key]["type"], "boolean");
        assert_eq!(properties[key]["dov:unit"], "state");
    }
    assert_eq!(properties["Timestamp"]["type"], "string");
    assert_eq!(properties["Timestamp"]["format"], "date-time");
}

#[test]
fn first_serialized_sample_matches_the_model_and_uses_utc_event_time() {
    let simulation = simulation();
    let first = simulation.samples().next().expect("simulation has samples");
    let sample = serde_json::to_value(first).expect("telemetry must serialize as JSON");
    assert_sample_shape(&thing_description(), &sample);
    assert_eq!(assert_utc_timestamp(&sample), start());
}

#[test]
fn final_bearing_anomaly_is_representable_without_changing_normal_limits() {
    let td = thing_description();
    let simulation = simulation();
    let last = simulation.samples().last().expect("simulation has samples");
    let encoded = serde_json::to_string(&last).expect("anomalous telemetry must serialize");
    let sample: Value =
        serde_json::from_str(&encoded).expect("anomalous telemetry must round-trip");
    assert_sample_shape(&td, &sample);
    assert_utc_timestamp(&sample);

    let bearing_temperature = sample["BT"].as_f64().expect("BT must remain numeric");
    assert!(
        bearing_temperature.is_finite() && bearing_temperature > 423.0,
        "last BT must exceed the normal 423 K limit, got {bearing_temperature}"
    );
    let bearing_schema = &td["properties"]["BT"];
    assert_eq!(bearing_schema["dov:normalOperatingMaximum"], 423);
    assert_eq!(bearing_schema["type"], "number");
    for constraint in [
        "maximum",
        "exclusiveMaximum",
        "enum",
        "const",
        "oneOf",
        "anyOf",
        "allOf",
        "not",
    ] {
        assert!(
            bearing_schema.get(constraint).is_none(),
            "BT must remain unconstrained by {constraint} so anomalies are valid data"
        );
    }
}
