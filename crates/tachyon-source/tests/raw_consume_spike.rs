//! Spike de consumo crudo (diagnóstico de throughput).
//!
//! Se ejecuta con:
//!   cargo test -p tachyon-source --test raw_consume_spike -- --ignored --nocapture
//!
//! Pregunta que responde: ¿cuántas filas/s puede drenar SOLO el conector
//! (rdkafka -> RecordStream), sin DataFusion ni Paimon?
//!
//! - Si el resultado es ~igual al E2E (≈200K), el consumidor es el techo:
//!   el overhead por mensaje del poll loop (FFI + lock de librdkafka) acota
//!   el throughput y hay que optimizar la capa de consumo.
//! - Si es mucho mayor (1M+), el cuello está aguas abajo (DataFusion/sink)
//!   y el conector no es el límite.
//!
//! Usa el mismo topic que `bench_e2e` (tachyon-bench-e2e) para reutilizar
//! la pre-producción de 10M eventos.

use std::time::{Duration, Instant};

use futures::StreamExt;
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::ClientConfig;
use tachyon_source::consumer::RdkafkaSource;

const BROKER: &str = "localhost:9092";
const TOPIC: &str = "tachyon-bench-e2e";
/// Particiones del topic (overridable con `SPIKE_PARTITIONS`): cada partición
/// tiene su propio fetch stream, así que el throughput de consumo debería
/// escalar con el número de particiones si el techo es el pacing de fetch.
fn partitions() -> i32 {
    std::env::var("SPIKE_PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}
/// Eventos a drenar (overridable con `SPIKE_EVENTS` para corridas rápidas).
fn events() -> u64 {
    std::env::var("SPIKE_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000)
}

async fn ensure_topic() {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    let admin: AdminClient<DefaultClientContext> = cc.create().expect("admin");
    let _ = admin.delete_topics(&[TOPIC], &Default::default()).await;
    admin
        .create_topics(
            &[NewTopic::new(TOPIC, partitions(), TopicReplication::Fixed(1))],
            &Default::default(),
        )
        .await
        .expect("creando topic");
}

fn pre_produce() {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    cc.set("queue.buffering.max.messages", "1000000");
    cc.set("message.send.max.retries", "10");
    cc.set("batch.size", "1048576");
    cc.set("linger.ms", "5");
    let producer: BaseProducer = cc.create().expect("producer");
    let t0 = Instant::now();
    let n = events();
    let mut ok = 0u64;
    let mut err = 0u64;
    for i in 0..n {
        let key = (i % 1024).to_string();
        let payload = format!(
            r#"{{"order_id":{},"status":"paid","source_version":{},"amount":1.5}}"#,
            i % 100_000,
            i
        );
        // Reintenta drenando el buffer si está lleno (el `send` del producer
        // devuelve Err cuando la cola interna está llena: ignorarlo tiraba
        // eventos en silencio y el topic quedaba con menos datos de los
        // "producidos").
        let record = BaseRecord::to(TOPIC).key(&key).payload(payload.as_str());
        if producer.send(record).is_ok() {
            ok += 1;
        } else {
            producer.poll(Duration::from_millis(10));
            let record = BaseRecord::to(TOPIC).key(&key).payload(payload.as_str());
            if producer.send(record).is_ok() {
                ok += 1;
            } else {
                err += 1;
                if err <= 5 {
                    eprintln!("no se pudo enviar el evento {i} (buffer saturado)");
                }
            }
        }
    }
    producer.flush(Duration::from_secs(60)).expect("flush");
    assert!(err == 0, "no deben haber errores de producción");
    println!(
        "pre-producción: {ok}/{n} eventos en {:.1}s ({:.0} ev/s)",
        t0.elapsed().as_secs_f64(),
        n as f64 / t0.elapsed().as_secs_f64()
    );
}

#[tokio::test]
#[ignore = "requiere Redpanda local en localhost:9092"]
async fn raw_consume_spike() {
    ensure_topic().await;
    pre_produce();

    // Mismo config que el runtime (run.rs: source_client_config).
    let group_id = format!(
        "tachyon-spike-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", BROKER);
    cc.set("group.id", &group_id);
    cc.set("auto.offset.reset", "earliest");
    cc.set("enable.auto.commit", "false");
    cc.set("session.timeout.ms", "10000");
    // Tunables vía env para aislar el origen del pacing de llegada:
    // SPIKE_FETCH_MIN_BYTES (default 524288) y SPIKE_FETCH_WAIT_MS (default 1000).
    let fetch_min_bytes = std::env::var("SPIKE_FETCH_MIN_BYTES").ok();
    let fetch_wait_ms = std::env::var("SPIKE_FETCH_WAIT_MS").ok();
    if let Some(v) = &fetch_min_bytes {
        cc.set("fetch.min.bytes", v);
    } else {
        cc.set("fetch.min.bytes", "524288");
    }
    if let Some(v) = &fetch_wait_ms {
        cc.set("fetch.wait.max.ms", v);
    } else {
        cc.set("fetch.wait.max.ms", "1000");
    }
    cc.set("queue.buffering.max.messages", "1000000");
    cc.set("queue.buffering.max.kbytes", "65536");
    let consumers = std::env::var("SPIKE_CONSUMERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    println!(
        "fetch config: min.bytes={}, wait.max.ms={}, consumers={}",
        fetch_min_bytes.as_deref().unwrap_or("524288"),
        fetch_wait_ms.as_deref().unwrap_or("1000"),
        consumers
    );

    // N consumidores en el MISMO grupo: el protocolo de grupo reparte las
    // particiones entre ellos. Si el throughput agregado escala con N, el
    // techo es por-consumidor (librdkafka del cliente); si se mantiene plano,
    // es el broker (Redpanda 1-core).
    let mut streams: Vec<
        std::pin::Pin<
            Box<
                dyn futures::Stream<
                    Item = anyhow::Result<Vec<tachyon_source::record::SourceRecord>>,
                > + Send,
            >,
        >,
    > = Vec::new();
    for _ in 0..consumers {
        let source = RdkafkaSource::new(&cc, TOPIC).expect("source");
        streams.push(source.record_stream());
    }
    // Select sobre todos los consumidores: el bucle de drenado no sabe (ni le
    // importa) de qué consumidor llega cada lote.
    let mut all = futures::stream::select_all(streams);

    // Drenado puro: cuenta lotes y filas, sin decodificar ni escribir.
    // Adicionalmente mide el gap entre lotes (donde va el wall time) y la
    // distribución de tamaños de lote.
    // Drenado acotado en TIEMPO (no en filas): si el topic se agota, el poll
    // bloqueante espera hasta `fetch.wait.max.ms` por poll y el test se
    // colgaría esperando la última fila. Con ventana de tiempo el test
    // siempre termina y el throughput se mide en estado estacionario.
    let t0 = Instant::now();
    let drain_for = Duration::from_secs(20);
    let mut rows = 0u64;
    let mut batches = 0u64;
    let mut last_batch_at = Instant::now();
    let mut last_progress = Instant::now();
    let mut gap_sum_ms = 0.0f64;
    let mut gap_max_ms = 0.0f64;
    let mut gap_hist: [u64; 6] = [0; 6]; // <1, 1-5, 5-20, 20-100, 100-500, >500 ms
    let mut sizes: Vec<u64> = Vec::new();
    while Instant::now().duration_since(t0) < drain_for {
        // `next()` acotado por timeout: si el primer lote no llega nunca, el
        // bucle no se cuelga (el timeout devuelve el control y la ventana de
        // tiempo sigue siendo efectiva). El timeout individual es de 5s: si un
        // lote tarda más, se cuenta como gap largo y se sigue midiendo.
        let elapsed_left = drain_for - t0.elapsed();
        let wait = elapsed_left.min(Duration::from_secs(5));
        match tokio::time::timeout(wait, all.next()).await {
            Ok(Some(Ok(batch))) => {
                let now = Instant::now();
                let gap_ms = now.duration_since(last_batch_at).as_secs_f64() * 1e3;
                last_batch_at = now;
                gap_sum_ms += gap_ms;
                gap_max_ms = gap_max_ms.max(gap_ms);
                let b = if gap_ms < 1.0 {
                    0
                } else if gap_ms < 5.0 {
                    1
                } else if gap_ms < 20.0 {
                    2
                } else if gap_ms < 100.0 {
                    3
                } else if gap_ms < 500.0 {
                    4
                } else {
                    5
                };
                gap_hist[b] += 1;
                sizes.push(batch.len() as u64);
                rows += batch.len() as u64;
                batches += 1;
            }
            Ok(Some(Err(e))) => panic!("error en el stream: {e:#}"),
            Ok(None) => break, // canal cerrado
            Err(_) => {
                // Timeout sin lote: progreso (solo si llevamos >5s sin lotes).
                if last_progress.elapsed() > Duration::from_secs(5) {
                    eprintln!(
                        "  ...sin lotes nuevos desde hace {:.0}s (total: {rows} filas, {batches} lotes)",
                        last_progress.elapsed().as_secs_f64()
                    );
                    last_progress = Instant::now();
                }
            }
        }
    }
    let el = t0.elapsed().as_secs_f64();
    // Tamaño de lote: media y percentiles.
    let mut sizes_sorted = sizes.clone();
    sizes_sorted.sort_unstable();
    let p = |q: usize| sizes_sorted[(sizes_sorted.len() as f64 * q as f64 / 100.0) as usize];
    let size_mean = sizes.iter().sum::<u64>() as f64 / sizes.len() as f64;
    let in_gap = gap_sum_ms / 1e3;
    println!(
        "\n=== Consumo crudo (sin DataFusion ni Paimon) ===\n  filas: {rows}\n  tiempo: {el:.2}s\n  throughput: {:.0} rows/s\n  lotes: {batches}\n  filas/lote: media {size_mean:.0}, p50 {} , p90 {}, p99 {}, max {}\n  gap entre lotes: suma {in_gap:.2}s ({:.0}% del wall), max {gap_max_ms:.0}ms, media {:.1}ms\n  histograma de gaps: <1ms {} | 1-5ms {} | 5-20ms {} | 20-100ms {} | 100-500ms {} | >500ms {}",
        rows as f64 / el,
        p(50),
        p(90),
        p(99),
        *sizes_sorted.last().unwrap(),
        in_gap / el * 100.0,
        gap_sum_ms / batches as f64,
        gap_hist[0],
        gap_hist[1],
        gap_hist[2],
        gap_hist[3],
        gap_hist[4],
        gap_hist[5]
    );
}
