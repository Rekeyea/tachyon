//! Dos consumidores del mismo proceso se pasan una partición con un lote ya
//! copiado y todavía no aplicado. Ese lote se cuenta una vez, y el consumidor
//! nuevo retoma en el offset de después del apply.
//!
//!   cargo test -p tachyon-source --test live_handoff -- --ignored --nocapture

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use tachyon_source::consumer::RdkafkaSource;
use tachyon_source::WindowHandoff;

const BROKER: &str = "localhost:9092";

fn client(group: &str, instance: &str) -> ClientConfig {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    cc.set("group.id", group);
    cc.set("enable.auto.commit", "false");
    cc.set("auto.offset.reset", "earliest");
    cc.set("session.timeout.ms", "45000");
    cc.set("fetch.min.bytes", "1");
    cc.set("fetch.wait.max.ms", "200");
    cc.set("group.instance.id", instance);
    cc
}

async fn recreate(topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("admin");
    let _ = admin.delete_topics(&[topic], &AdminOptions::new()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    admin
        .create_topics(
            &[NewTopic::new(topic, 2, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("topic");
}

async fn produce(topic: &str, records: &[(i32, &str)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .create()
        .expect("producer");
    for (partition, payload) in records {
        producer
            .send(
                FutureRecord::to(topic)
                    .partition(*partition)
                    .key(&partition.to_string())
                    .payload(*payload),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }
}

fn next_offsets(records: &[(i32, i64)]) -> BTreeMap<i32, i64> {
    let mut out = BTreeMap::new();
    for &(partition, offset) in records {
        let slot = out.entry(partition).or_insert(0);
        *slot = (*slot).max(offset + 1);
    }
    out
}

async fn drain(
    stream: &mut tachyon_source::RecordStream,
    for_how_long: Duration,
) -> Vec<(i32, i64, String)> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + for_how_long;
    while tokio::time::Instant::now() < deadline {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(left.min(Duration::from_millis(250)), stream.next()).await {
            Ok(Some(Ok(batch))) => {
                for record in batch {
                    let payload = String::from_utf8_lossy(&record.value).into_owned();
                    out.push((record.partition, record.offset, payload));
                }
            }
            Ok(Some(Err(e))) => panic!("el stream falló: {e:#}"),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    out
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn two_consumers_apply_a_mixed_lot_once() {
    let topic = format!("tachyon-handoff-{}", std::process::id());
    let group = format!("tachyon-handoff-{}", std::process::id());
    recreate(&topic).await;
    // p y q, cuatro registros cada una, antes de que arranque el segundo consumidor.
    let mut first = Vec::new();
    for i in 0..4 {
        first.push((0, format!("p{i}")));
        first.push((1, format!("q{i}")));
    }
    let first_ref: Vec<(i32, &str)> = first.iter().map(|(p, s)| (*p, s.as_str())).collect();
    produce(&topic, &first_ref).await;

    let gate = Arc::new(WindowHandoff::starting(&group, &topic, "commit-a"));
    let source0 = RdkafkaSource::new(&client(&group, "inst-0"), &topic)
        .expect("c0")
        .with_max_batch(8)
        .with_window_handoff(gate.clone());
    let mut stream0 = source0.record_stream();

    let mut pre = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let batch = drain(&mut stream0, Duration::from_millis(500)).await;
        pre.extend(batch);
        let partitions: std::collections::HashSet<i32> = pre.iter().map(|row| row.0).collect();
        if partitions.contains(&0) && partitions.contains(&1) {
            break;
        }
    }
    assert!(
        pre.iter().any(|row| row.0 == 0) && pre.iter().any(|row| row.0 == 1),
        "el primer consumidor no juntó las dos particiones: {pre:?}"
    );

    let source1 = RdkafkaSource::new(&client(&group, "inst-1"), &topic)
        .expect("c1")
        .with_max_batch(8)
        .with_window_handoff(gate.clone());
    let mut stream1 = source1.record_stream();

    let mut blocked = false;
    let wait = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < wait {
        if gate.blocked_offset(0).is_some() || gate.blocked_offset(1).is_some() {
            blocked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        blocked,
        "el revoke no dejó una partición en espera: admitted={:?}",
        gate.admitted_map()
    );

    // Mientras el apply no publica, nadie vuelve a entregar el lote ya copiado.
    let during = drain(&mut stream0, Duration::from_millis(400)).await;
    let during1 = drain(&mut stream1, Duration::from_millis(400)).await;
    let pre_keys: std::collections::HashSet<(i32, i64)> =
        pre.iter().map(|row| (row.0, row.1)).collect();
    for row in during.iter().chain(during1.iter()) {
        assert!(
            !pre_keys.contains(&(row.0, row.1)),
            "el lote en vuelo se leyó otra vez: {row:?}"
        );
    }

    let mut seen: HashMap<(i32, i64), usize> = HashMap::new();
    for row in pre.iter().chain(during.iter()).chain(during1.iter()) {
        *seen.entry((row.0, row.1)).or_insert(0) += 1;
    }
    let mut applied = next_offsets(&seen.keys().copied().collect::<Vec<_>>());
    // Lo que el gate ya tiene admitido puede ir un poll adelante de lo drenado.
    for (partition, next) in gate.admitted_map() {
        let slot = applied.entry(partition).or_insert(next);
        *slot = (*slot).max(next);
    }
    gate.publish_applied(&applied);
    for (partition, next) in &applied {
        assert!(
            gate.applied_map().get(partition).copied().unwrap_or(0) >= *next,
            "applied bajó en la partición {partition}"
        );
    }

    produce(&topic, &[(0, "p-new"), (1, "q-new")]).await;
    let mut after = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        after.extend(drain(&mut stream0, Duration::from_millis(300)).await);
        after.extend(drain(&mut stream1, Duration::from_millis(300)).await);
        let payloads: Vec<&str> = after.iter().map(|row| row.2.as_str()).collect();
        if payloads.contains(&"p-new") && payloads.contains(&"q-new") {
            break;
        }
    }
    for row in &after {
        assert!(
            !pre_keys.contains(&(row.0, row.1)),
            "después del apply se releyó el lote: {row:?}"
        );
        *seen.entry((row.0, row.1)).or_insert(0) += 1;
    }
    let dupes: Vec<_> = seen.iter().filter(|(_, n)| **n != 1).collect();
    assert!(dupes.is_empty(), "offsets repetidos: {dupes:?}");
    assert!(
        after.iter().any(|row| row.2 == "p-new"),
        "p no retomó: {after:?}"
    );
    assert!(
        after.iter().any(|row| row.2 == "q-new"),
        "q no retomó: {after:?}"
    );
}
