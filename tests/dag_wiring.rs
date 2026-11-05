//! Integration tests for DAG wiring, validation, and topology variations.

use genpipe::components::processors::{FilterProcessor, MapProcessor};
use genpipe::components::sinks::CollectorSink;
use genpipe::components::sources::IterSource;
use genpipe::prelude::*;

// ── Helpers ─────────────────────────────────────────────────────────────

fn text_msgs(origin: &str, texts: &[&str]) -> Vec<Message> {
    texts.iter().map(|t| Message::text(origin, *t)).collect()
}

// ── Broadcast fan-out ───────────────────────────────────────────────────

#[tokio::test]
async fn broadcast_fan_out_delivers_to_all_sinks() {
    // source -> processor -> [sink_a, sink_b]
    //
    // Both sinks should receive the same messages.
    let input = text_msgs("src", &["alpha", "beta"]);

    let a = CollectorSink::new("sink_a");
    let b = CollectorSink::new("sink_b");
    let ha = a.handle();
    let hb = b.handle();

    let svc = Service::builder("fan-out")
        .source(IterSource::new("src", input))
        .processor(MapProcessor::new("upper", |mut msg| {
            if let Some(t) = msg.payload().as_text() {
                msg.set_payload(Payload::Text(t.to_uppercase()));
            }
            msg
        }))
        .sink(a)
        .sink(b)
        .build()
        .expect("build");

    svc.run(CancellationToken::new()).await.expect("run");

    let ca = ha.lock().await;
    let cb = hb.lock().await;

    assert_eq!(ca.len(), 2);
    assert_eq!(cb.len(), 2);

    // Both sinks see the uppercase versions.
    assert_eq!(ca[0].payload().as_text().unwrap(), "ALPHA");
    assert_eq!(ca[1].payload().as_text().unwrap(), "BETA");
    assert_eq!(cb[0].payload().as_text().unwrap(), "ALPHA");
    assert_eq!(cb[1].payload().as_text().unwrap(), "BETA");
}

// ── Multi-source topology ───────────────────────────────────────────────

#[tokio::test]
async fn multiple_sources_merge_into_single_processor() {
    // source_a ──┐
    //            ├──► processor ──► sink
    // source_b ──┘
    let a = text_msgs("a", &["from_a"]);
    let b = text_msgs("b", &["from_b"]);

    let collector = CollectorSink::new("collector");
    let handle = collector.handle();

    let svc = Service::builder("multi-src")
        .source(IterSource::new("src_a", a))
        .source(IterSource::new("src_b", b))
        .processor(MapProcessor::new("pass", |m| m))
        .sink(collector)
        .build()
        .expect("build");

    svc.run(CancellationToken::new()).await.expect("run");

    let msgs = handle.lock().await;
    assert_eq!(msgs.len(), 2);

    let mut texts: Vec<&str> = msgs
        .iter()
        .map(|m| m.payload().as_text().unwrap())
        .collect();
    texts.sort();
    assert_eq!(texts, vec!["from_a", "from_b"]);
}

// ── Direct source-to-sink (no processors) ───────────────────────────────

#[tokio::test]
async fn source_directly_to_sink_without_processors() {
    let input = text_msgs("src", &["one", "two", "three"]);
    let collector = CollectorSink::new("out");
    let handle = collector.handle();

    let svc = Service::builder("direct")
        .source(IterSource::new("src", input))
        .sink(collector)
        .build()
        .expect("build");

    svc.run(CancellationToken::new()).await.expect("run");

    let msgs = handle.lock().await;
    assert_eq!(msgs.len(), 3);
}

// ── Filter processor drops messages ─────────────────────────────────────

#[tokio::test]
async fn filter_processor_drops_messages() {
    let input = text_msgs("src", &["ok", "toolongtopass", "hi"]);
    let collector = CollectorSink::new("out");
    let handle = collector.handle();

    let svc = Service::builder("filter-test")
        .source(IterSource::new("src", input))
        .processor(FilterProcessor::new("short-only", |msg| {
            msg.payload().as_text().is_some_and(|t| t.len() <= 3)
        }))
        .sink(collector)
        .build()
        .expect("build");

    svc.run(CancellationToken::new()).await.expect("run");

    let msgs = handle.lock().await;
    assert_eq!(msgs.len(), 2);
    let texts: Vec<&str> = msgs
        .iter()
        .map(|m| m.payload().as_text().unwrap())
        .collect();
    assert_eq!(texts, vec!["ok", "hi"]);
}

// ── Processor chaining ──────────────────────────────────────────────────

#[tokio::test]
async fn chained_processors_applied_in_order() {
    let input = text_msgs("src", &["hello", "world"]);
    let collector = CollectorSink::new("out");
    let handle = collector.handle();

    let svc = Service::builder("chain")
        .source(IterSource::new("src", input))
        .processor(MapProcessor::new("upper", |mut msg| {
            if let Some(t) = msg.payload().as_text() {
                msg.set_payload(Payload::Text(t.to_uppercase()));
            }
            msg
        }))
        .processor(MapProcessor::new("exclaim", |mut msg| {
            if let Some(t) = msg.payload().as_text() {
                msg.set_payload(Payload::Text(format!("{}!", t)));
            }
            msg
        }))
        .sink(collector)
        .build()
        .expect("build");

    svc.run(CancellationToken::new()).await.expect("run");

    let msgs = handle.lock().await;
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].payload().as_text().unwrap(), "HELLO!");
    assert_eq!(msgs[1].payload().as_text().unwrap(), "WORLD!");
}

// ── Validation: cycle detection ─────────────────────────────────────────

#[test]
fn cycle_in_processor_graph_is_rejected() {
    use std::collections::HashMap;

    // Build a config where processor A depends on B and B depends on A.
    let config = ServiceConfig {
        name: "cycle-test".into(),
        channel_capacity: 64,
        drain_timeout_ms: 1000,
        error_strategy: ErrorStrategy::Drop,
        max_retry_attempts: 1,
        sources: vec![ComponentConfig {
            component_type: "interval".into(),
            name: Some("src".into()),
            from: vec![],
            settings: {
                let mut m = HashMap::new();
                m.insert("interval_ms".into(), serde_json::json!(1000));
                m
            },
        }],
        processors: vec![
            ComponentConfig {
                component_type: "passthrough".into(),
                name: Some("proc_a".into()),
                from: vec!["proc_b".into()],
                settings: HashMap::new(),
            },
            ComponentConfig {
                component_type: "passthrough".into(),
                name: Some("proc_b".into()),
                from: vec!["proc_a".into()],
                settings: HashMap::new(),
            },
        ],
        sinks: vec![ComponentConfig {
            component_type: "log".into(),
            name: Some("sink".into()),
            from: vec!["proc_b".into()],
            settings: HashMap::new(),
        }],
    };

    let registry = ComponentRegistry::with_builtins();
    let result = Service::from_config(&config, &registry);

    match result {
        Err(e) => {
            let err = e.to_string();
            assert!(err.contains("cycle"), "expected cycle error, got: {err}");
        }
        Ok(_) => panic!("should have rejected cycle"),
    }
}

// ── Validation: unknown predecessor ─────────────────────────────────────

#[test]
fn unknown_predecessor_is_rejected() {
    use std::collections::HashMap;

    let config = ServiceConfig {
        name: "bad-ref".into(),
        channel_capacity: 64,
        drain_timeout_ms: 1000,
        error_strategy: ErrorStrategy::Drop,
        max_retry_attempts: 1,
        sources: vec![ComponentConfig {
            component_type: "interval".into(),
            name: Some("src".into()),
            from: vec![],
            settings: {
                let mut m = HashMap::new();
                m.insert("interval_ms".into(), serde_json::json!(1000));
                m
            },
        }],
        processors: vec![ComponentConfig {
            component_type: "passthrough".into(),
            name: Some("proc".into()),
            from: vec!["nonexistent".into()],
            settings: HashMap::new(),
        }],
        sinks: vec![ComponentConfig {
            component_type: "log".into(),
            name: Some("sink".into()),
            from: vec!["proc".into()],
            settings: HashMap::new(),
        }],
    };

    let registry = ComponentRegistry::with_builtins();
    let result = Service::from_config(&config, &registry);

    match result {
        Err(e) => {
            let err = e.to_string();
            assert!(err.contains("unknown predecessor"), "expected unknown predecessor error, got: {err}");
        }
        Ok(_) => panic!("should have rejected unknown predecessor"),
    }
}

// ── Validation: duplicate from entries ──────────────────────────────────

#[test]
fn duplicate_from_entry_is_rejected() {
    use std::collections::HashMap;

    let config = ServiceConfig {
        name: "dup-from".into(),
        channel_capacity: 64,
        drain_timeout_ms: 1000,
        error_strategy: ErrorStrategy::Drop,
        max_retry_attempts: 1,
        sources: vec![ComponentConfig {
            component_type: "interval".into(),
            name: Some("src".into()),
            from: vec![],
            settings: {
                let mut m = HashMap::new();
                m.insert("interval_ms".into(), serde_json::json!(1000));
                m
            },
        }],
        processors: vec![ComponentConfig {
            component_type: "passthrough".into(),
            name: Some("proc".into()),
            from: vec!["src".into(), "src".into()],
            settings: HashMap::new(),
        }],
        sinks: vec![ComponentConfig {
            component_type: "log".into(),
            name: Some("sink".into()),
            from: vec!["proc".into()],
            settings: HashMap::new(),
        }],
    };

    let registry = ComponentRegistry::with_builtins();
    let result = Service::from_config(&config, &registry);

    match result {
        Err(e) => {
            let err = e.to_string();
            assert!(err.contains("duplicate"), "expected duplicate error, got: {err}");
        }
        Ok(_) => panic!("should have rejected duplicate from"),
    }
}

// ── Validation: builder rejects empty config ────────────────────────────

#[test]
fn builder_rejects_no_sources() {
    let collector = CollectorSink::new("out");
    let result = Service::builder("no-src").sink(collector).build();
    match result {
        Err(e) => assert!(e.to_string().contains("source")),
        Ok(_) => panic!("should reject no sources"),
    }
}

#[test]
fn builder_rejects_no_sinks() {
    let result = Service::builder("no-sink")
        .source(IterSource::new("src", vec![]))
        .build();
    match result {
        Err(e) => assert!(e.to_string().contains("sink")),
        Ok(_) => panic!("should reject no sinks"),
    }
}

// ── Config-based diamond topology ───────────────────────────────────────

#[tokio::test]
async fn config_based_diamond_topology() {
    use std::collections::HashMap;

    // src -> proc_a ──┐
    //                 ├──► sink
    // src -> proc_b ──┘
    //
    // Both proc_a and proc_b read from "src", sink reads from both.
    // The sink should see messages from both processors.
    let config = ServiceConfig {
        name: "diamond".into(),
        channel_capacity: 64,
        drain_timeout_ms: 5000,
        error_strategy: ErrorStrategy::Drop,
        max_retry_attempts: 1,
        sources: vec![ComponentConfig {
            component_type: "interval".into(),
            name: Some("src".into()),
            from: vec![],
            settings: {
                let mut m = HashMap::new();
                m.insert("interval_ms".into(), serde_json::json!(100));
                m
            },
        }],
        processors: vec![
            ComponentConfig {
                component_type: "passthrough".into(),
                name: Some("proc_a".into()),
                from: vec!["src".into()],
                settings: HashMap::new(),
            },
            ComponentConfig {
                component_type: "passthrough".into(),
                name: Some("proc_b".into()),
                from: vec!["src".into()],
                settings: HashMap::new(),
            },
        ],
        sinks: vec![ComponentConfig {
            component_type: "log".into(),
            name: Some("out".into()),
            from: vec!["proc_a".into(), "proc_b".into()],
            settings: HashMap::new(),
        }],
    };

    let registry = ComponentRegistry::with_builtins();
    let svc = Service::from_config(&config, &registry).expect("build");

    let shutdown = CancellationToken::new();
    let token = shutdown.clone();

    // Let it run briefly then cancel.
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        token.cancel();
    });

    svc.run(shutdown).await.expect("run");
    // If we get here, the pipeline started and stopped without deadlock.
}
