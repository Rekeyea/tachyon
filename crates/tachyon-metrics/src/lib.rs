//! `tachyon-metrics`: métricas de una instancia de pipeline.
//!
//! Métricas básicas (throughput, lag, memoria) + endpoint HTTP
//! `GET /metrics` en formato Prometheus (scrapeable por Prometheus/Grafana
//! directamente). La export OpenTelemetry (OTLP) corre en paralelo: una tarea
//! de fondo lee los deltas de los contadores y los registra en el SDK de
//! OpenTelemetry, que los envía a un collector cada `interval` (ver
//! DESIGN.md §10.2).
//!
//! El endpoint HTTP no usa framework: `tokio::net` + parseo manual de la
//! request (solo soporta el método GET a la ruta `/metrics`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Métricas de una instancia de pipeline.
///
/// Contadores monotónicos (rows procesadas/escritas) + gauge de lag. Se
/// actualizan desde el loop de ejecución y se leen desde el endpoint HTTP
/// y desde la export OTLP.
#[derive(Debug, Default)]
pub struct InstanceMetrics {
    /// Filas leídas de la fuente (acumulado).
    pub rows_read: Arc<AtomicU64>,
    /// Filas escritas al sink (acumulado).
    pub rows_written: Arc<AtomicU64>,
    /// Commits de sink completados (acumulado).
    pub commits: Arc<AtomicU64>,
    /// Consumer lag total (offsets no consumidos, gauge).
    pub consumer_lag: Arc<AtomicU64>,
    /// Errores de ejecución (acumulado).
    pub errors: Arc<AtomicU64>,
    /// Tiempo dentro de `stream.next()` (ns, acumulado): consumo + decode +
    /// transformación (la etapa "fuente" del pipeline).
    pub source_next_ns: Arc<AtomicU64>,
    /// Tiempo bloqueado enviando al writer (ns, acumulado): backpressure del
    /// sink. Si domina, el cuello es el writer (write o commit).
    pub send_wait_ns: Arc<AtomicU64>,
    /// Tiempo dentro de `sink.write()` (ns, acumulado, task del writer).
    pub write_ns: Arc<AtomicU64>,
    /// Tiempo dentro del checkpoint del sink (ns, acumulado, task del writer).
    /// Durante un checkpoint el writer no escribe: es tiempo muerto del sink.
    pub commit_ns: Arc<AtomicU64>,
}

impl InstanceMetrics {
    pub fn new() -> Self {
        InstanceMetrics::default()
    }

    pub fn inc_rows_read(&self, n: u64) {
        self.rows_read.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_rows_written(&self, n: u64) {
        self.rows_written.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_commits(&self) {
        self.commits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_consumer_lag(&self, lag: u64) {
        self.consumer_lag.store(lag, Ordering::Relaxed);
    }

    pub fn inc_errors(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_source_next_ns(&self, ns: u64) {
        self.source_next_ns.fetch_add(ns, Ordering::Relaxed);
    }

    pub fn add_send_wait_ns(&self, ns: u64) {
        self.send_wait_ns.fetch_add(ns, Ordering::Relaxed);
    }

    pub fn add_write_ns(&self, ns: u64) {
        self.write_ns.fetch_add(ns, Ordering::Relaxed);
    }

    pub fn add_commit_ns(&self, ns: u64) {
        self.commit_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// Serializa las métricas actuales en formato Prometheus (texto plano con
    /// `# HELP`/`# TYPE`), legible por humanos y scrapeable por
    /// Prometheus/Grafana.
    pub fn render(&self) -> String {
        let mut out = String::new();
        render_metric(
            &mut out,
            "rows_read",
            "counter",
            "Filas leídas de la fuente (acumulado).",
            self.rows_read.load(Ordering::Relaxed),
        );
        render_metric(
            &mut out,
            "rows_written",
            "counter",
            "Filas escritas al sink (acumulado).",
            self.rows_written.load(Ordering::Relaxed),
        );
        render_metric(
            &mut out,
            "commits",
            "counter",
            "Commits de sink completados (acumulado).",
            self.commits.load(Ordering::Relaxed),
        );
        render_metric(
            &mut out,
            "consumer_lag",
            "gauge",
            "Consumer lag total (offsets no consumidos).",
            self.consumer_lag.load(Ordering::Relaxed),
        );
        render_metric(
            &mut out,
            "errors",
            "counter",
            "Errores de ejecución (acumulado).",
            self.errors.load(Ordering::Relaxed),
        );
        render_metric(
            &mut out,
            "source_next_ns",
            "counter",
            "Tiempo en stream.next() (ns, acumulado).",
            self.source_next_ns.load(Ordering::Relaxed),
        );
        render_metric(
            &mut out,
            "send_wait_ns",
            "counter",
            "Tiempo bloqueado enviando al writer (ns, acumulado).",
            self.send_wait_ns.load(Ordering::Relaxed),
        );
        render_metric(
            &mut out,
            "write_ns",
            "counter",
            "Tiempo en sink.write() (ns, acumulado).",
            self.write_ns.load(Ordering::Relaxed),
        );
        render_metric(
            &mut out,
            "commit_ns",
            "counter",
            "Tiempo en el checkpoint del sink (ns, acumulado).",
            self.commit_ns.load(Ordering::Relaxed),
        );
        out
    }
}

/// Añade una métrica al texto Prometheus: `# HELP`, `# TYPE` y el valor.
fn render_metric(out: &mut String, name: &str, ty: &str, help: &str, value: u64) {
    out.push_str(&format!("# HELP tachyon_{name} {help}\n"));
    out.push_str(&format!("# TYPE tachyon_{name} {ty}\n"));
    out.push_str(&format!("tachyon_{name} {value}\n"));
}

/// Servidor de métricas HTTP minimalista.
///
/// Sirve `GET /metrics` con el texto de `metrics.render()`. Cualquier otra
/// ruta o método devuelve 404. Se corre en una tarea tokio dedicada.
pub struct MetricsServer {
    addr: std::net::SocketAddr,
    metrics: Arc<InstanceMetrics>,
}

impl MetricsServer {
    /// Crea el servidor (no lo arranca). `bind_addr` es la dirección a
    /// escuchar (p. ej. `127.0.0.1:9090`).
    pub fn new(bind_addr: std::net::SocketAddr, metrics: Arc<InstanceMetrics>) -> Self {
        MetricsServer { addr: bind_addr, metrics }
    }

    /// Arranca el servidor en una tarea tokio. Devuelve la dirección real
    /// (útil si se binda al puerto 0).
    pub async fn start(self) -> Result<std::net::SocketAddr> {
        let listener = tokio::net::TcpListener::bind(self.addr)
            .await
            .with_context(|| format!("bind en {}", self.addr))?;
        let actual = listener.local_addr().context("local_addr")?;
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((mut socket, _peer)) => {
                        let metrics = metrics.clone();
                        tokio::spawn(async move {
                            let _ = handle_connection(&mut socket, &metrics).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "aceptando conexiones de métricas");
                    }
                }
            }
        });

        Ok(actual)
    }
}

/// Maneja una conexión: lee la request, responde 200 con las métricas si es
/// `GET /metrics`, 404 en otro caso. Cierre después de la respuesta.
async fn handle_connection(
    socket: &mut tokio::net::TcpStream,
    metrics: &InstanceMetrics,
) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    // Solo necesitamos la primera línea (request line).
    let n = socket.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buf[..n]);
    let first_line = request.lines().next().unwrap_or("");
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    let (status, body): (&str, String) = if method == "GET" && (path == "/metrics" || path == "/") {
        ("200 OK", metrics.render())
    } else {
        ("404 Not Found", "not found\n".to_string())
    };

    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.shutdown().await?;
    Ok(())
}

/// Export de métricas vía OpenTelemetry (OTLP).
///
/// El SDK de OpenTelemetry (`opentelemetry_sdk`) recoge los instrumentos y
/// un `PeriodicReader` los envía a un collector OTLP cada `interval`. El
/// hot path del pipeline no toca el SDK: una tarea de fondo lee los deltas
/// de los atómicos de `InstanceMetrics` y los registra como
/// `MonotonicCounter`/`Gauge` (ver DESIGN.md §10.2).
pub mod otlp {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
    use opentelemetry_sdk::resource::Resource;
    use opentelemetry_otlp::MetricExporter;
    use opentelemetry_otlp::WithExportConfig;

    /// Configuración de la export OTLP.
    #[derive(Debug, Clone)]
    pub struct OtelConfig {
        /// URL base del collector (p. ej. `http://otel-collector:4318`).
        /// La ruta de señal (`/v1/metrics`) se añade sola.
        pub endpoint: String,
        /// Intervalo de export (también el de muestreo de los deltas).
        pub interval: Duration,
        /// `service.name` del recurso OTel (default: nombre del pipeline).
        pub service_name: String,
        /// `service.instance.id` del recurso OTel (identidad de la instancia).
        pub instance: String,
    }

    /// Registro OTel de la instancia. El provider vive dentro de la tarea de
    /// export; el registro solo lo expone para tests (drop = shutdown +
    /// export final).
    pub struct OtelRegistry {
        /// No se lee: se conserva para que el provider viva mientras viva el
        /// registro (drop = shutdown + export final). En tests se usa para
        /// `force_flush`.
        #[allow(dead_code)]
        pub(crate) provider: SdkMeterProvider,
    }

    /// Instrumentos de la instancia (counters de deltas + gauge de lag).
    struct Instruments {
        rows_read: opentelemetry::metrics::Counter<u64>,
        rows_written: opentelemetry::metrics::Counter<u64>,
        commits: opentelemetry::metrics::Counter<u64>,
        errors: opentelemetry::metrics::Counter<u64>,
        source_next_ns: opentelemetry::metrics::Counter<u64>,
        send_wait_ns: opentelemetry::metrics::Counter<u64>,
        write_ns: opentelemetry::metrics::Counter<u64>,
        commit_ns: opentelemetry::metrics::Counter<u64>,
        consumer_lag: opentelemetry::metrics::Gauge<u64>,
    }

    impl Instruments {
        fn new(provider: &SdkMeterProvider) -> Self {
            let meter = provider.meter("tachyon");
            Instruments {
                rows_read: meter.u64_counter("tachyon.rows.read").build(),
                rows_written: meter.u64_counter("tachyon.rows.written").build(),
                commits: meter.u64_counter("tachyon.commits").build(),
                errors: meter.u64_counter("tachyon.errors").build(),
                source_next_ns: meter.u64_counter("tachyon.stage.source_next_ns").build(),
                send_wait_ns: meter.u64_counter("tachyon.stage.send_wait_ns").build(),
                write_ns: meter.u64_counter("tachyon.stage.write_ns").build(),
                commit_ns: meter.u64_counter("tachyon.stage.commit_ns").build(),
                consumer_lag: meter.u64_gauge("tachyon.consumer.lag").build(),
            }
        }
    }

    impl OtelRegistry {
        /// Arranca la export OTLP: el provider vive en la tarea de fondo (la
        /// export dura toda la vida del proceso). Devuelve error si el
        /// exporter no se puede construir.
        pub fn start(metrics: Arc<InstanceMetrics>, cfg: OtelConfig) -> Result<()> {
            let url = otlp_metrics_url(&cfg.endpoint);
            let exporter = MetricExporter::builder()
                .with_http()
                .with_endpoint(url)
                .build()
                .context("construyendo el exporter OTLP")?;
            let reader = PeriodicReader::builder(exporter)
                .with_interval(cfg.interval)
                .build();
            let resource = resource_for(&cfg.service_name, &cfg.instance);
            let provider = SdkMeterProvider::builder()
                .with_reader(reader)
                .with_resource(resource)
                .build();
            let instruments = Instruments::new(&provider);
            tokio::spawn(async move {
                delta_loop(metrics, cfg.interval, instruments).await;
                // El provider se dropa al terminar la tarea: export final.
                drop(provider);
            });
            Ok(())
        }

        /// Variante con un reader propio (tests con exporter in-memory). El
        /// caller posee el provider: su drop dispara el shutdown y la export
        /// final.
        #[doc(hidden)]
        pub fn with_reader<E: PushMetricExporter + 'static>(
            metrics: Arc<InstanceMetrics>,
            interval: Duration,
            reader: PeriodicReader<E>,
            service_name: String,
            instance: String,
        ) -> OtelRegistry {
            let resource = resource_for(&service_name, &instance);
            let provider = SdkMeterProvider::builder()
                .with_reader(reader)
                .with_resource(resource)
                .build();
            let instruments = Instruments::new(&provider);
            tokio::spawn(async move {
                delta_loop(metrics, interval, instruments).await;
            });
            OtelRegistry { provider }
        }
    }

    fn resource_for(service_name: &str, instance: &str) -> Resource {
        Resource::builder()
            .with_service_name(service_name.to_string())
            .with_attribute(KeyValue::new("service.instance.id", instance.to_string()))
            .build()
    }

    /// Bucle de deltas: cada `interval` registra en OTel lo que los
    /// atómicos acumularon desde la última vuelta (los counters OTel son
    /// monotónicos; el gauge refleja el lag actual).
    async fn delta_loop(
        metrics: Arc<InstanceMetrics>,
        interval: Duration,
        inst: Instruments,
    ) {
        let mut last = [0u64; 8];
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            let current = [
                metrics.rows_read.load(Ordering::Relaxed),
                metrics.rows_written.load(Ordering::Relaxed),
                metrics.commits.load(Ordering::Relaxed),
                metrics.errors.load(Ordering::Relaxed),
                metrics.source_next_ns.load(Ordering::Relaxed),
                metrics.send_wait_ns.load(Ordering::Relaxed),
                metrics.write_ns.load(Ordering::Relaxed),
                metrics.commit_ns.load(Ordering::Relaxed),
            ];
            let counters = [
                &inst.rows_read,
                &inst.rows_written,
                &inst.commits,
                &inst.errors,
                &inst.source_next_ns,
                &inst.send_wait_ns,
                &inst.write_ns,
                &inst.commit_ns,
            ];
            for (i, counter) in counters.iter().enumerate() {
                let delta = current[i].saturating_sub(last[i]);
                if delta > 0 {
                    counter.add(delta, &[]);
                }
                last[i] = current[i];
            }
            inst.consumer_lag
                .record(metrics.consumer_lag.load(Ordering::Relaxed), &[]);
        }
    }

    /// URL del endpoint OTLP de métricas: respeta una URL completa que ya
    /// incluya la ruta de señal y añade `/v1/metrics` a una URL base.
    pub(crate) fn otlp_metrics_url(endpoint: &str) -> String {
        let trimmed = endpoint.trim_end_matches('/');
        if trimmed.ends_with("/v1/metrics") {
            trimmed.to_string()
        } else {
            format!("{trimmed}/v1/metrics")
        }
    }
}

/// Arranca la observabilidad de la instancia: el endpoint HTTP de métricas
/// (si hay `bind`) y la export OTLP (si hay `otlp`). Devuelve la dirección
/// real del endpoint HTTP (None si no hay).
pub async fn start_observability(
    metrics: &Arc<InstanceMetrics>,
    bind: Option<std::net::SocketAddr>,
    otlp: Option<&otlp::OtelConfig>,
) -> Result<Option<std::net::SocketAddr>> {
    let metrics_addr = match bind {
        Some(addr) => {
            let server = MetricsServer::new(addr, metrics.clone());
            let actual = server
                .start()
                .await
                .context("arrancando el endpoint de métricas")?;
            tracing::info!(%actual, "métricas disponibles");
            Some(actual)
        }
        None => None,
    };
    if let Some(cfg) = otlp {
        otlp::OtelRegistry::start(metrics.clone(), cfg.clone())
            .context("arrancando la export OTLP")?;
        tracing::info!(endpoint = %cfg.endpoint, "export OTLP activa");
    }
    Ok(metrics_addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment() {
        let m = InstanceMetrics::new();
        m.inc_rows_read(10);
        m.inc_rows_read(5);
        m.inc_rows_written(3);
        m.inc_commits();
        m.set_consumer_lag(42);
        m.inc_errors();

        assert_eq!(m.rows_read.load(Ordering::Relaxed), 15);
        assert_eq!(m.rows_written.load(Ordering::Relaxed), 3);
        assert_eq!(m.commits.load(Ordering::Relaxed), 1);
        assert_eq!(m.consumer_lag.load(Ordering::Relaxed), 42);
        assert_eq!(m.errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn render_contains_all_metrics() {
        let m = InstanceMetrics::new();
        m.inc_rows_read(7);
        let text = m.render();
        assert!(text.contains("tachyon_rows_read 7"));
        assert!(text.contains("tachyon_rows_written 0"));
        assert!(text.contains("tachyon_commits 0"));
        assert!(text.contains("tachyon_consumer_lag 0"));
        assert!(text.contains("tachyon_errors 0"));
        assert!(text.contains("# TYPE tachyon_rows_read counter"));
        assert!(text.contains("# TYPE tachyon_consumer_lag gauge"));
    }

    #[tokio::test]
    async fn http_endpoint_serves_metrics() {
        let metrics = Arc::new(InstanceMetrics::new());
        metrics.inc_rows_read(99);
        let server = MetricsServer::new("127.0.0.1:0".parse().unwrap(), metrics);
        let addr = server.start().await.expect("arrancando servidor");

        // Cliente: leer /metrics.
        let mut socket = tokio::net::TcpStream::connect(addr).await.expect("conectando");
        socket
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .expect("escribiendo request");
        let mut buf = Vec::new();
        socket.read_to_end(&mut buf).await.expect("leyendo respuesta");
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 200"), "debe ser 200: {response}");
        assert!(response.contains("tachyon_rows_read 99"), "cuerpo: {response}");
    }

    #[tokio::test]
    async fn http_endpoint_404_for_unknown_path() {
        let metrics = Arc::new(InstanceMetrics::new());
        let server = MetricsServer::new("127.0.0.1:0".parse().unwrap(), metrics);
        let addr = server.start().await.expect("arrancando servidor");

        let mut socket = tokio::net::TcpStream::connect(addr).await.expect("conectando");
        socket
            .write_all(b"GET /nope HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .expect("escribiendo request");
        let mut buf = Vec::new();
        socket.read_to_end(&mut buf).await.expect("leyendo respuesta");
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 404"), "debe ser 404: {response}");
    }

    // --- Export OTLP (con exporter in-memory del SDK) ---

    /// El valor máximo (acumulado) de la métrica `name` en las colecciones
    /// del exporter (los counters son acumulativos: el último valor es el
    /// mayor).
    fn metric_max(
        rms: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
        name: &str,
    ) -> Option<u64> {
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
        let mut out: Option<u64> = None;
        for rm in rms {
            for scope in rm.scope_metrics() {
                for m in scope.metrics() {
                    if m.name() != name {
                        continue;
                    }
                    let values: Vec<u64> = match m.data() {
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                            sum.data_points().map(|d| d.value()).collect()
                        }
                        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                            gauge.data_points().map(|d| d.value()).collect()
                        }
                        _ => Vec::new(),
                    };
                    let max = *values.iter().max().unwrap_or(&0);
                    out = out.map(|v| v.max(max)).or(Some(max));
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn otlp_registry_records_deltas_and_gauge() {
        use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader};

        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(Duration::from_millis(20))
            .build();
        let metrics = Arc::new(InstanceMetrics::new());
        let registry = otlp::OtelRegistry::with_reader(
            metrics.clone(),
            Duration::from_millis(20),
            reader,
            "test-pipeline".to_string(),
            "test-instance".to_string(),
        );

        metrics.inc_rows_read(10);
        metrics.inc_rows_read(5);
        metrics.inc_rows_written(4);
        metrics.inc_commits();
        metrics.inc_errors();
        metrics.add_source_next_ns(123);
        metrics.add_commit_ns(77);
        metrics.set_consumer_lag(9);

        // Varias vueltas del reader para que el in-memory exporter acumule.
        tokio::time::sleep(Duration::from_millis(120)).await;
        registry.provider.force_flush().expect("force_flush");
        let rms = exporter.get_finished_metrics().expect("métricas");

        assert_eq!(metric_max(&rms, "tachyon.rows.read"), Some(15));
        assert_eq!(metric_max(&rms, "tachyon.rows.written"), Some(4));
        assert_eq!(metric_max(&rms, "tachyon.commits"), Some(1));
        assert_eq!(metric_max(&rms, "tachyon.errors"), Some(1));
        assert_eq!(metric_max(&rms, "tachyon.stage.source_next_ns"), Some(123));
        assert_eq!(metric_max(&rms, "tachyon.stage.commit_ns"), Some(77));
        assert_eq!(metric_max(&rms, "tachyon.consumer.lag"), Some(9));
    }

    #[test]
    fn otlp_url_appends_metrics_path() {
        assert_eq!(
            otlp::otlp_metrics_url("http://collector:4318"),
            "http://collector:4318/v1/metrics"
        );
        assert_eq!(
            otlp::otlp_metrics_url("http://collector:4318/"),
            "http://collector:4318/v1/metrics"
        );
        assert_eq!(
            otlp::otlp_metrics_url("http://collector:4318/v1/metrics"),
            "http://collector:4318/v1/metrics"
        );
    }
}
