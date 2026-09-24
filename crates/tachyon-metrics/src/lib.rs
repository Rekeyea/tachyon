//! `tachyon-metrics`: métricas de una instancia de pipeline.
//!
//! Slice 5: métricas básicas (throughput, lag, memoria) + endpoint HTTP
//! minimalista. La integración OpenTelemetry completa llega en la Fase 4
//! (ver DESIGN.md §10.2); esto da ya el "footprint medible" del MVP.
//!
//! El endpoint expone `GET /metrics` en formato plano (clave: valor), legible
//! por humanos y fácil de scrapear. Sin framework HTTP: `tokio::net` + parseo
//! manual de la request (solo soporta el método GET a la ruta `/metrics`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Métricas de una instancia de pipeline.
///
/// Contadores monotónicos (rows procesadas/escritas) + gauge de lag. Se
/// actualizan desde el loop de ejecución y se leen desde el endpoint HTTP.
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

    /// Serializa las métricas actuales en formato plano.
    pub fn render(&self) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        format!(
            "tachyon_rows_read {}\n\
             tachyon_rows_written {}\n\
             tachyon_commits {}\n\
             tachyon_consumer_lag {}\n\
             tachyon_errors {}\n\
             tachyon_timestamp {}\n",
            self.rows_read.load(Ordering::Relaxed),
            self.rows_written.load(Ordering::Relaxed),
            self.commits.load(Ordering::Relaxed),
            self.consumer_lag.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
            now,
        )
    }
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
}
