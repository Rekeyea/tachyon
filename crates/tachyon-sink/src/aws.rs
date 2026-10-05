//! Clientes AWS para los sinks de Kinesis y SQS.
//!
//! El `SdkConfig` sale de `aws-config` (chain de credenciales estándar:
//! entorno, archivo de perfil, IMDS). La config del pipeline fija región,
//! perfil y endpoint (endpoint para floCi/LocalStack o una puerta de prueba).

use anyhow::{Context, Result};
use aws_types::region::Region;

/// `SdkConfig` para un conector: región de la config, perfil y endpoint
/// opcionales.
pub async fn sdk_config(
    region: &str,
    endpoint: Option<&str>,
    profile: Option<&str>,
) -> Result<aws_config::SdkConfig> {
    let mut builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_string()));
    if let Some(profile) = profile {
        builder = builder.profile_name(profile.to_string());
    }
    if let Some(endpoint) = endpoint {
        builder = builder.endpoint_url(endpoint.to_string());
    }
    Ok(builder.load().await)
}

/// URL de la cola: si la config ya trae una URL completa se usa tal cual;
/// si trae un nombre se resuelve con `GetQueueUrl`.
pub async fn queue_url(client: &aws_sdk_sqs::Client, queue: &str) -> Result<String> {
    if queue.contains("://") {
        return Ok(queue.to_string());
    }
    let resp = client
        .get_queue_url()
        .queue_name(queue)
        .send()
        .await
        .with_context(|| format!("GetQueueUrl ({queue})"))?;
    resp.queue_url()
        .map(|url| url.to_string())
        .with_context(|| format!("GetQueueUrl ({queue}) sin queue_url"))
}
