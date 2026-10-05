//! Clientes AWS para los inputs de Kinesis y SQS.
//!
//! El `SdkConfig` sale de `aws-config` (chain de credenciales estándar:
//! entorno, archivo de perfil, IMDS). La config del pipeline fija región,
//! perfil y endpoint (endpoint para floCi/LocalStack o una puerta de prueba).

use anyhow::{Context, Result};
use aws_config::SdkConfig;
use aws_types::region::Region;
use tachyon_config::{KinesisConfig, SqsConfig};
use tachyon_source::KinesisReader;

/// Carga el `SdkConfig` de un conector: región de la config, perfil y
/// endpoint opcionales.
pub(crate) async fn sdk_config(
    region: &str,
    profile: Option<&str>,
    endpoint: Option<&str>,
) -> Result<SdkConfig> {
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

pub(crate) struct KinesisSession {
    pub client: aws_sdk_kinesis::Client,
    pub reader: Option<KinesisReader>,
}

pub(crate) async fn kinesis_client(config: &KinesisConfig) -> Result<KinesisSession> {
    let sdk = sdk_config(
        &config.region,
        config.profile.as_deref(),
        config.endpoint.as_deref(),
    )
    .await
    .with_context(|| format!("cargando credenciales para kinesis ({})", config.region))?;
    let client = aws_sdk_kinesis::Client::new(&sdk);
    let reader = match sdk.credentials_provider() {
        Some(provider) => {
            let region = sdk
                .region()
                .map(|region| region.as_ref().to_string())
                .unwrap_or_else(|| config.region.clone());
            let endpoint = sdk
                .endpoint_url()
                .map(|endpoint| endpoint.to_string())
                .unwrap_or_else(|| format!("https://kinesis.{region}.amazonaws.com"));
            Some(
                KinesisReader::new(provider, region, endpoint).map_err(|error| {
                    anyhow::anyhow!("lector kinesis ({}): {error}", config.region)
                })?,
            )
        }
        None => None,
    };
    Ok(KinesisSession { client, reader })
}

pub(crate) async fn sqs_client(config: &SqsConfig) -> Result<aws_sdk_sqs::Client> {
    let sdk = sdk_config(
        &config.region,
        config.profile.as_deref(),
        config.endpoint.as_deref(),
    )
    .await
    .with_context(|| format!("cargando credenciales para sqs ({})", config.region))?;
    Ok(aws_sdk_sqs::Client::new(&sdk))
}

/// URL de la cola: si la config ya trae una URL completa se usa tal cual;
/// si trae un nombre se resuelve con `GetQueueUrl`.
pub(crate) async fn queue_url(client: &aws_sdk_sqs::Client, queue: &str) -> Result<String> {
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
