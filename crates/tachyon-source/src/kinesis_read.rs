//! `GetRecords` propio, al lado del cliente del SDK.
//!
//! El SDK de Rust pide JSON. Este lector pide CBOR (`application/x-amz-cbor-1.1`),
//! que es lo que habla el SDK de Java y lo que floCi contesta con los mismos
//! nombres de campo. `Data` llega como bytes en AWS y como texto base64 en
//! floCi: el parser acepta los dos. DescribeStream y GetShardIterator siguen
//! en el SDK.

use std::time::SystemTime;

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use base64::Engine;

const CONTENT_TYPE: &str = "application/x-amz-cbor-1.1";
const TARGET: &str = "Kinesis_20131202.GetRecords";

/// Un registro de `GetRecords` ya con el payload en bytes crudos.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawRecord {
    pub sequence: String,
    pub data: Vec<u8>,
}

/// Una página de `GetRecords`. `next_iterator == None` cierra el shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GetRecordsPage {
    pub next_iterator: Option<String>,
    pub records: Vec<RawRecord>,
    pub child_shards: Vec<String>,
}

/// Error de una lectura. `Fallback` deja ese shard en el `GetRecords` del SDK
/// para el resto del proceso (un emulador que solo habla JSON).
#[derive(Debug)]
pub(crate) enum ReadError {
    Fallback(String),
    Fatal(String),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Fallback(msg) | ReadError::Fatal(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for ReadError {}

/// Cliente HTTP de `GetRecords`. Barato de clonar: el pool es compartido.
#[derive(Clone)]
pub struct KinesisReader {
    http: reqwest::Client,
    credentials: SharedCredentialsProvider,
    region: String,
    url: String,
    host: String,
}

impl KinesisReader {
    /// `endpoint` es el origen (`http://127.0.0.1:4566` o el de AWS).
    pub fn new(
        credentials: SharedCredentialsProvider,
        region: String,
        endpoint: String,
    ) -> Result<Self, String> {
        let (url, host) = request_target(&endpoint)?;
        let http = reqwest::Client::builder()
            .http1_only()
            .pool_max_idle_per_host(32)
            .build()
            .map_err(|e| format!("cliente http de kinesis: {e}"))?;
        Ok(Self {
            http,
            credentials,
            region,
            url,
            host,
        })
    }

    pub(crate) async fn get_records(
        &self,
        iterator: &str,
        limit: i32,
    ) -> Result<GetRecordsPage, ReadError> {
        let body = encode_get_records(iterator, limit);
        let signed = self.sign(&body).await?;
        let mut request = self
            .http
            .post(&self.url)
            .header("content-type", CONTENT_TYPE)
            .header("x-amz-target", TARGET);
        for (name, value) in signed {
            request = request.header(name, value);
        }
        let response = request
            .body(body)
            .send()
            .await
            .map_err(|e| ReadError::Fatal(format!("GetRecords: {e}")))?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string());
        let bytes = response
            .bytes()
            .await
            .map_err(|e| ReadError::Fatal(format!("GetRecords cuerpo: {e}")))?;
        disposition(status, content_type.as_deref()).map_err(|err| match err {
            ReadError::Fatal(msg) => ReadError::Fatal(format!("{msg}: {}", snippet(&bytes))),
            other => other,
        })?;
        let bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || parse_page(&bytes))
            .await
            .map_err(|e| ReadError::Fatal(format!("parse GetRecords: {e}")))?
            .map_err(ReadError::Fatal)
    }

    async fn sign(&self, body: &[u8]) -> Result<Vec<(&'static str, String)>, ReadError> {
        let creds = self
            .credentials
            .provide_credentials()
            .await
            .map_err(|e| ReadError::Fatal(format!("credenciales: {e}")))?;
        let identity = Identity::new(creds, None);
        let mut settings = SigningSettings::default();
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name("kinesis")
            .time(SystemTime::now())
            .settings(settings)
            .build()
            .map_err(|e| ReadError::Fatal(format!("firma: {e}")))?;
        let params = aws_sigv4::http_request::SigningParams::from(params);
        let signable = SignableRequest::new(
            "POST",
            self.url.as_str(),
            [
                ("host", self.host.as_str()),
                ("content-type", CONTENT_TYPE),
                ("x-amz-target", TARGET),
            ]
            .into_iter(),
            SignableBody::Bytes(body),
        )
        .map_err(|e| ReadError::Fatal(format!("firma: {e}")))?;
        let (instructions, _) = sign(signable, &params)
            .map_err(|e| ReadError::Fatal(format!("firma: {e}")))?
            .into_parts();
        let (headers, _) = instructions.into_parts();
        Ok(headers
            .into_iter()
            .map(|header| (header.name(), header.value().to_string()))
            .collect())
    }
}

/// `Ok` si el cuerpo hay que parsearlo como CBOR. JSON y 415 son fallback.
pub(crate) fn disposition(status: u16, content_type: Option<&str>) -> Result<(), ReadError> {
    if status == 415 {
        return Err(ReadError::Fallback("HTTP 415".to_string()));
    }
    if !(200..300).contains(&status) {
        return Err(ReadError::Fatal(format!("HTTP {status}")));
    }
    if content_type
        .unwrap_or("")
        .to_ascii_lowercase()
        .contains("json")
    {
        return Err(ReadError::Fallback(format!(
            "content-type {}",
            content_type.unwrap_or("")
        )));
    }
    Ok(())
}

fn snippet(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(&bytes[..bytes.len().min(180)]);
    text.replace('\n', " ")
}

fn request_target(endpoint: &str) -> Result<(String, String), String> {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if !trimmed.contains("://") {
        return Err(format!("endpoint sin esquema: {endpoint}"));
    }
    let url = format!("{trimmed}/");
    let parsed = reqwest::Url::parse(&url).map_err(|e| format!("endpoint {endpoint}: {e}"))?;
    let host = match (parsed.host_str(), parsed.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_string(),
        _ => return Err(format!("endpoint sin host: {endpoint}")),
    };
    Ok((url, host))
}

fn encode_get_records(iterator: &str, limit: i32) -> Vec<u8> {
    let mut out = Vec::with_capacity(iterator.len() + 32);
    out.push(0xa2);
    write_text(&mut out, "ShardIterator");
    write_text(&mut out, iterator);
    write_text(&mut out, "Limit");
    write_uint(&mut out, limit.max(0) as u64);
    out
}

fn write_text(out: &mut Vec<u8>, text: &str) {
    let bytes = text.as_bytes();
    write_len(out, 0x60, bytes.len());
    out.extend_from_slice(bytes);
}

fn write_uint(out: &mut Vec<u8>, value: u64) {
    if value < 24 {
        out.push(value as u8);
    } else if value < 0x100 {
        out.push(24);
        out.push(value as u8);
    } else if value < 0x10000 {
        out.push(25);
        out.extend_from_slice(&(value as u16).to_be_bytes());
    } else if value < 0x1_0000_0000 {
        out.push(26);
        out.extend_from_slice(&(value as u32).to_be_bytes());
    } else {
        out.push(27);
        out.extend_from_slice(&value.to_be_bytes());
    }
}

fn write_len(out: &mut Vec<u8>, major: u8, len: usize) {
    let len = len as u64;
    if len < 24 {
        out.push(major | len as u8);
    } else if len < 0x100 {
        out.push(major | 24);
        out.push(len as u8);
    } else if len < 0x10000 {
        out.push(major | 25);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(major | 26);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

pub(crate) fn parse_page(buf: &[u8]) -> Result<GetRecordsPage, String> {
    let mut cur = Cur { buf, i: 0 };
    let (major, pairs, indefinite) = cur.header()?;
    if major != 5 {
        return Err("GetRecords no es un mapa".into());
    }
    let mut records = Vec::new();
    let mut saw_records = false;
    let mut next_iterator = None;
    let mut child_shards = Vec::new();
    cur.walk_map(pairs, indefinite, |cur| {
        let Some(key) = cur.read_key()? else {
            cur.skip()?;
            return Ok(());
        };
        match key.as_str() {
            "Records" => {
                records = cur.read_records()?;
                saw_records = true;
            }
            "NextShardIterator" => next_iterator = cur.read_optional_text()?,
            "ChildShards" => child_shards = cur.read_children()?,
            _ => cur.skip()?,
        }
        Ok(())
    })?;
    if cur.i != cur.buf.len() {
        return Err("cbor con bytes de sobra".into());
    }
    if !saw_records {
        records.clear();
    }
    Ok(GetRecordsPage {
        next_iterator,
        records,
        child_shards,
    })
}

struct Cur<'a> {
    buf: &'a [u8],
    i: usize,
}

impl<'a> Cur<'a> {
    fn header(&mut self) -> Result<(u8, u64, bool), String> {
        let byte = self.byte()?;
        let major = byte >> 5;
        let extra = byte & 0x1f;
        if extra < 24 {
            return Ok((major, extra as u64, false));
        }
        if extra == 31 {
            return Ok((major, 0, true));
        }
        let arg = match extra {
            24 => self.byte()? as u64,
            25 => u16::from_be_bytes(self.array::<2>()?) as u64,
            26 => u32::from_be_bytes(self.array::<4>()?) as u64,
            27 => u64::from_be_bytes(self.array::<8>()?),
            _ => return Err(format!("cbor adicional {extra}")),
        };
        Ok((major, arg, false))
    }

    fn byte(&mut self) -> Result<u8, String> {
        let byte = *self.buf.get(self.i).ok_or("cbor truncado")?;
        self.i += 1;
        Ok(byte)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        if self.buf.len() - self.i < N {
            return Err("cbor truncado".into());
        }
        let mut out = [0u8; N];
        out.copy_from_slice(&self.buf[self.i..self.i + N]);
        self.i += N;
        Ok(out)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.buf.len() - self.i < n {
            return Err("cbor truncado".into());
        }
        let slice = &self.buf[self.i..self.i + n];
        self.i += n;
        Ok(slice)
    }

    fn at_break(&self) -> Result<bool, String> {
        Ok(*self.buf.get(self.i).ok_or("cbor truncado")? == 0xff)
    }

    fn skip(&mut self) -> Result<(), String> {
        let (major, arg, indefinite) = self.header()?;
        match major {
            0 | 1 => {
                if indefinite {
                    return Err("entero cbor indefinido".into());
                }
                Ok(())
            }
            2 | 3 => self.skip_string(indefinite, arg),
            4 => self.walk_array(arg, indefinite, |cur| cur.skip()),
            5 => self.walk_map(arg, indefinite, |cur| {
                cur.skip()?;
                cur.skip()
            }),
            6 => {
                if indefinite {
                    return Err("tag cbor indefinido".into());
                }
                self.skip()
            }
            7 => {
                if indefinite {
                    return Err("break cbor inesperado".into());
                }
                Ok(())
            }
            _ => Err(format!("major cbor {major}")),
        }
    }

    fn skip_string(&mut self, indefinite: bool, len: u64) -> Result<(), String> {
        if !indefinite {
            let n = self.ulen(len)?;
            self.take(n)?;
            return Ok(());
        }
        loop {
            if self.at_break()? {
                self.byte()?;
                return Ok(());
            }
            self.skip()?;
        }
    }

    fn walk_array<F>(&mut self, len: u64, indefinite: bool, mut each: F) -> Result<(), String>
    where
        F: FnMut(&mut Self) -> Result<(), String>,
    {
        if indefinite {
            loop {
                if self.at_break()? {
                    self.byte()?;
                    return Ok(());
                }
                each(self)?;
            }
        }
        if len > self.buf.len() as u64 {
            return Err("array cbor demasiado largo".into());
        }
        for _ in 0..len {
            each(self)?;
        }
        Ok(())
    }

    fn walk_map<F>(&mut self, pairs: u64, indefinite: bool, mut each: F) -> Result<(), String>
    where
        F: FnMut(&mut Self) -> Result<(), String>,
    {
        if indefinite {
            loop {
                if self.at_break()? {
                    self.byte()?;
                    return Ok(());
                }
                each(self)?;
            }
        }
        if pairs > self.buf.len() as u64 {
            return Err("mapa cbor demasiado largo".into());
        }
        for _ in 0..pairs {
            each(self)?;
        }
        Ok(())
    }

    fn read_key(&mut self) -> Result<Option<String>, String> {
        let major = self.buf.get(self.i).copied().ok_or("cbor truncado")? >> 5;
        if major == 3 {
            Ok(Some(self.read_text()?))
        } else {
            self.skip()?;
            Ok(None)
        }
    }

    fn read_text(&mut self) -> Result<String, String> {
        let (major, arg, indefinite) = self.header()?;
        if major == 6 {
            return self.read_text();
        }
        if major != 3 {
            return Err("se esperaba texto cbor".into());
        }
        self.finish_text(arg, indefinite)
    }

    fn finish_text(&mut self, arg: u64, indefinite: bool) -> Result<String, String> {
        if !indefinite {
            let n = self.ulen(arg)?;
            let bytes = self.take(n)?;
            return String::from_utf8(bytes.to_vec()).map_err(|_| "texto cbor no es utf-8".into());
        }
        let mut text = String::new();
        loop {
            if self.at_break()? {
                self.byte()?;
                return Ok(text);
            }
            let (major, arg, indefinite) = self.header()?;
            if major != 3 || indefinite {
                return Err("chunk de texto cbor inválido".into());
            }
            text.push_str(&self.finish_text(arg, false)?);
        }
    }

    fn read_optional_text(&mut self) -> Result<Option<String>, String> {
        let (major, arg, indefinite) = self.header()?;
        if major == 6 {
            return self.read_optional_text();
        }
        if major == 7 && !indefinite && (arg == 22 || arg == 23) {
            return Ok(None);
        }
        if major != 3 {
            return Err("NextShardIterator inesperado".into());
        }
        let text = self.finish_text(arg, indefinite)?;
        if text.is_empty() {
            Ok(None)
        } else {
            Ok(Some(text))
        }
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, String> {
        let (major, arg, indefinite) = self.header()?;
        if major == 6 {
            return self.read_bytes();
        }
        match major {
            2 => self.finish_bytes(arg, indefinite),
            3 => {
                let text = self.finish_text(arg, indefinite)?;
                base64::engine::general_purpose::STANDARD
                    .decode(text.trim())
                    .map_err(|_| "Data base64 inválido".into())
            }
            _ => Err("Data no es bytes ni texto".into()),
        }
    }

    fn finish_bytes(&mut self, arg: u64, indefinite: bool) -> Result<Vec<u8>, String> {
        if !indefinite {
            let n = self.ulen(arg)?;
            return Ok(self.take(n)?.to_vec());
        }
        let mut out = Vec::new();
        loop {
            if self.at_break()? {
                self.byte()?;
                return Ok(out);
            }
            let (major, arg, indefinite) = self.header()?;
            if major != 2 || indefinite {
                return Err("chunk de bytes cbor inválido".into());
            }
            out.extend_from_slice(&self.finish_bytes(arg, false)?);
        }
    }

    fn read_records(&mut self) -> Result<Vec<RawRecord>, String> {
        let (major, len, indefinite) = self.header()?;
        if major != 4 {
            return Err("Records no es un array".into());
        }
        let mut records = Vec::new();
        self.walk_array(len, indefinite, |cur| {
            records.push(cur.read_record()?);
            Ok(())
        })?;
        Ok(records)
    }

    fn read_record(&mut self) -> Result<RawRecord, String> {
        let (major, pairs, indefinite) = self.header()?;
        if major != 5 {
            return Err("un registro no es un mapa".into());
        }
        let mut sequence = None;
        let mut data = None;
        self.walk_map(pairs, indefinite, |cur| {
            let Some(key) = cur.read_key()? else {
                cur.skip()?;
                return Ok(());
            };
            match key.as_str() {
                "SequenceNumber" => sequence = Some(cur.read_text()?),
                "Data" => data = Some(cur.read_bytes()?),
                _ => cur.skip()?,
            }
            Ok(())
        })?;
        Ok(RawRecord {
            sequence: sequence.ok_or("registro sin SequenceNumber")?,
            data: data.ok_or("registro sin Data")?,
        })
    }

    fn read_children(&mut self) -> Result<Vec<String>, String> {
        let (major, len, indefinite) = self.header()?;
        if major != 4 {
            return Err("ChildShards no es un array".into());
        }
        let mut children = Vec::new();
        self.walk_array(len, indefinite, |cur| {
            let (major, pairs, indefinite) = cur.header()?;
            if major != 5 {
                return Err("un shard hijo no es un mapa".into());
            }
            let mut shard_id = None;
            cur.walk_map(pairs, indefinite, |cur| {
                let Some(key) = cur.read_key()? else {
                    cur.skip()?;
                    return Ok(());
                };
                if key == "ShardId" {
                    shard_id = Some(cur.read_text()?);
                } else {
                    cur.skip()?;
                }
                Ok(())
            })?;
            children.push(shard_id.ok_or("shard hijo sin ShardId")?);
            Ok(())
        })?;
        Ok(children)
    }

    fn ulen(&self, len: u64) -> Result<usize, String> {
        let len = usize::try_from(len).map_err(|_| "longitud cbor".to_string())?;
        if len > self.buf.len() {
            return Err("cbor truncado".into());
        }
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floci_capture_decodes_two_base64_records() {
        let page = parse_page(include_bytes!("../fixtures/floci-getrecords-limit2.cbor"))
            .expect("fixture");
        assert_eq!(page.records.len(), 2);
        assert_eq!(page.records[0].sequence, "1790798633266");
        assert_eq!(
            page.records[0].data,
            br#"{"order_id":22,"status":"paid","source_version":2,"amount":2.5,"event_time":1790798604338}"#
        );
        assert_eq!(page.records[1].sequence, "1790798633277");
        assert_eq!(
            page.records[1].data,
            br#"{"order_id":66,"status":"paid","source_version":8,"amount":8.5,"event_time":1790798604338}"#
        );
        assert_eq!(
            page.next_iterator.as_deref(),
            Some("YmVuY2gtZXRsfHNoYXJkSWQtMDAwMDAwMDAwMDAwfFRSSU1fSE9SSVpPTnx8Mnw=")
        );
        assert!(page.child_shards.is_empty());
    }

    #[test]
    fn definite_page_accepts_raw_bytes_null_iterator_and_a_child() {
        // Mapa definido: Data es byte string (AWS), NextShardIterator es null
        // y hay un ChildShards con ShardId.
        let page = parse_page(
            &hex("a4675265636f72647381a36e53657175656e63654e756d6265726231306444617461436162636c506172746974696f6e4b65796170714e65787453686172644974657261746f72f66b4368696c6453686172647381a167536861726449646b73686172642d6368696c64724d696c6c6973426568696e644c617465737400"),
        )
        .expect("página");
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].sequence, "10");
        assert_eq!(page.records[0].data, b"abc");
        assert!(page.next_iterator.is_none());
        assert_eq!(page.child_shards, vec!["shard-child".to_string()]);
    }

    #[test]
    fn empty_lot_keeps_the_iterator() {
        let page = parse_page(&hex(
            "a2675265636f72647380714e65787453686172644974657261746f7266697465722d32",
        ))
        .expect("vacía");
        assert!(page.records.is_empty());
        assert_eq!(page.next_iterator.as_deref(), Some("iter-2"));
        assert!(page.child_shards.is_empty());
    }

    #[test]
    fn a_missing_iterator_closes_the_shard() {
        let page = parse_page(&hex("a1675265636f72647380")).expect("sin iterator");
        assert!(page.records.is_empty());
        assert!(page.next_iterator.is_none());
    }

    #[test]
    fn indefinite_empty_page_with_null_iterator() {
        let page = parse_page(&hex(
            "bf675265636f7264739fff714e65787453686172644974657261746f72f6ff",
        ))
        .expect("indefinida");
        assert!(page.records.is_empty());
        assert!(page.next_iterator.is_none());
    }

    #[test]
    fn json_and_415_fall_back_other_statuses_fail() {
        assert!(matches!(
            disposition(415, Some("application/x-amz-json-1.1")),
            Err(ReadError::Fallback(_))
        ));
        assert!(matches!(
            disposition(200, Some("application/x-amz-json-1.1")),
            Err(ReadError::Fallback(_))
        ));
        assert!(disposition(200, Some("application/x-amz-cbor-1.1")).is_ok());
        assert!(matches!(
            disposition(400, Some("application/x-amz-json-1.1")),
            Err(ReadError::Fatal(_))
        ));
    }

    #[test]
    fn request_body_is_a_two_entry_map() {
        let body = encode_get_records("iter", 10_000);
        assert_eq!(body[0], 0xa2);
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("ShardIterator"));
        assert!(text.contains("iter"));
        assert!(body.ends_with(&[0x19, 0x27, 0x10]));
    }

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }
}
