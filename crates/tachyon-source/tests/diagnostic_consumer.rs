//! Diagnóstico: ¿el consumidor rdkafka entrega mensajes FUERA del wrapper tokio?
//!
//! Pregunta que responde: si el spike (`raw_consume_spike`) nunca recibe lotes,
//! ¿es por el camino async (spawn_blocking + mpsc + tokio) o por librdkafka?
//!
//! Este test no usa tokio ni tachyon: crea un `BaseConsumer` plano, se
//! suscribe al topic (que ya tiene 10M mensajes de la última corrida del
//! spike) y hace un poll loop bloqueante de 10s en el thread actual.
//!
//!   cargo test -p tachyon-source --test diagnostic_consumer -- --ignored --nocapture
//!
//! Resultado esperado:
//! - PRIMER mensaje + ~100K+ msg/s → librdkafka está bien; el bug está en el
//!   wrapper async (spawn_blocking/mpsc/tokio).
//! - Polls vacíos durante 10s → el problema está en librdkafka/broker en este
//!   contexto (group join, offsets, metadata).

use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;
use rdkafka::Message;

const BROKER: &str = "localhost:9092";
const TOPIC: &str = "tachyon-bench-e2e";

#[test]
#[ignore = "requiere Redpanda local en localhost:9092"]
fn diagnostic_consumer() {
    let group_id = format!(
        "tachyon-diag-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    // Mismo config que el spike (run.rs: source_client_config).
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    cc.set("group.id", &group_id);
    cc.set("auto.offset.reset", "earliest");
    cc.set("enable.auto.commit", "false");
    cc.set("session.timeout.ms", "10000");
    cc.set("fetch.min.bytes", "524288");
    cc.set("fetch.wait.max.ms", "1000");
    cc.set("queue.buffering.max.messages", "1000000");
    cc.set("queue.buffering.max.kbytes", "65536");

    let consumer: BaseConsumer = cc.create().expect("consumer");
    consumer.subscribe(&[TOPIC]).expect("subscribe");
    println!("subscrito (group {group_id}); drenado completo (max 60s)");

    let t0 = Instant::now();
    let mut n = 0u64;
    let mut last_offset: i64 = -1;
    let mut last_mark = Instant::now();
    let drain_max = Duration::from_secs(60);
    let mut idle_secs = 0u64;
    while t0.elapsed() < drain_max {
        match consumer.poll(Duration::from_millis(500)) {
            Some(Ok(m)) => {
                n += 1;
                last_offset = m.offset();
                idle_secs = 0;
                if n == 1 {
                    println!(
                        "PRIMER mensaje: offset {} a los {:.2}s (payload {} bytes)",
                        m.offset(),
                        t0.elapsed().as_secs_f64(),
                        m.payload().map(|p| p.len()).unwrap_or(0)
                    );
                }
                if n % 500_000 == 0 {
                    let dt = last_mark.elapsed().as_secs_f64();
                    last_mark = Instant::now();
                    println!("  {n} mensajes (offset {last_offset}), marca: {:.0} msg/s", 500_000.0 / dt);
                }
            }
            Some(Err(e)) => {
                println!("  ERROR de poll en offset {last_offset}: {e}");
                break;
            }
            None => {
                idle_secs += 1;
                if idle_secs == 5 {
                    println!("  sin mensajes por 5s (total: {n}, último offset {last_offset})...");
                }
                if idle_secs >= 10 {
                    println!("  10s sin mensajes: dando por agotado el topic");
                    break;
                }
            }
        }
    }
    println!(
        "total: {n} mensajes en {:.1}s (último offset {last_offset}, high watermark esperado: {n} si no quedó nada)",
        t0.elapsed().as_secs_f64()
    );
    assert!(
        n > 0,
        "el consumidor no recibió ningún mensaje en 60s: el problema no está en el wrapper async"
    );
}
