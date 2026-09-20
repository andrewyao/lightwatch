//! Reading hotpath's HTTP server.
//!
//! Two endpoints and a fixed shape of reply make a dependency-free client the
//! honest choice here. hotpath answers on loopback over `tiny_http`, always
//! HTTP/1.1 with a `Content-Length`, and it honours `Connection: close`, so
//! "read to end of stream" is the whole framing problem.
//!
//! Every way this can fail is a variant of [`PollError`] and none of them
//! panic. A target that is not running yet, is shutting down, or is still
//! starting its profiler worker (hotpath answers `503` until then) all reach
//! the caller as something to retry.

use hotpath::json::{JsonFunctionsList, JsonProfilerStatus};
use std::fmt;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Refuses a reply large enough to be a bug rather than a report.
const MAX_BODY: u64 = 8 << 20;

#[derive(Debug)]
pub enum PollError {
    Unreachable(std::io::Error),
    Io(std::io::Error),
    Status(u16),
    Truncated,
    NotHttp,
    Body(serde_json::Error),
}

impl fmt::Display for PollError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PollError::Unreachable(e) => write!(f, "target unreachable: {e}"),
            PollError::Io(e) => write!(f, "target connection failed: {e}"),
            PollError::Status(code) => write!(f, "target answered HTTP {code}"),
            PollError::Truncated => write!(f, "target closed the connection mid-reply"),
            PollError::NotHttp => write!(f, "target replied with something that is not HTTP"),
            PollError::Body(e) => write!(f, "target reply did not parse: {e}"),
        }
    }
}

/// A profiled process's metrics server.
#[derive(Debug, Clone)]
pub struct Target {
    host: String,
    port: u16,
    timeout: Duration,
}

impl Target {
    pub fn new(host: impl Into<String>, port: u16, timeout: Duration) -> Self {
        Target {
            host: host.into(),
            port,
            timeout,
        }
    }

    pub fn status(&self) -> Result<JsonProfilerStatus, PollError> {
        let body = self.get("/profiler_status")?;
        serde_json::from_slice(&body).map_err(PollError::Body)
    }

    pub fn functions_timing(&self) -> Result<JsonFunctionsList, PollError> {
        let body = self.get("/functions_timing")?;
        serde_json::from_slice(&body).map_err(PollError::Body)
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    fn get(&self, path: &str) -> Result<Vec<u8>, PollError> {
        let address = self.resolve()?;
        let mut socket =
            TcpStream::connect_timeout(&address, self.timeout).map_err(PollError::Unreachable)?;
        socket
            .set_read_timeout(Some(self.timeout))
            .map_err(PollError::Io)?;
        socket
            .set_write_timeout(Some(self.timeout))
            .map_err(PollError::Io)?;
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.address()
        );
        socket
            .write_all(request.as_bytes())
            .map_err(PollError::Io)?;
        let mut raw = Vec::new();
        socket
            .take(MAX_BODY)
            .read_to_end(&mut raw)
            .map_err(PollError::Io)?;
        body_of(&raw)
    }

    fn resolve(&self) -> Result<SocketAddr, PollError> {
        self.address()
            .to_socket_addrs()
            .map_err(PollError::Unreachable)?
            .next()
            .ok_or_else(|| {
                PollError::Unreachable(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "host resolved to no address",
                ))
            })
    }
}

/// The body of a `200`, or why there isn't one.
fn body_of(raw: &[u8]) -> Result<Vec<u8>, PollError> {
    if raw.is_empty() {
        return Err(PollError::Truncated);
    }
    let head_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or(PollError::Truncated)?;
    let status_line = raw[..head_end]
        .split(|b| *b == b'\r')
        .next()
        .ok_or(PollError::NotHttp)?;
    let status_line = std::str::from_utf8(status_line).map_err(|_| PollError::NotHttp)?;
    let mut fields = status_line.split(' ');
    if !fields.next().is_some_and(|v| v.starts_with("HTTP/")) {
        return Err(PollError::NotHttp);
    }
    let code: u16 = fields
        .next()
        .and_then(|c| c.parse().ok())
        .ok_or(PollError::NotHttp)?;
    if code != 200 {
        return Err(PollError::Status(code));
    }
    Ok(raw[head_end + 4..].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(head: &str, body: &str) -> Vec<u8> {
        format!("{head}\r\nContent-Length: {}\r\n\r\n{body}", body.len()).into_bytes()
    }

    #[test]
    fn a_normal_reply_yields_its_body() {
        let raw = reply(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json",
            r#"{"a":1}"#,
        );
        assert_eq!(body_of(&raw).expect("a 200 has a body"), br#"{"a":1}"#);
    }

    #[test]
    fn a_profiler_that_is_not_ready_yet_is_an_error_not_an_empty_report() {
        let raw = reply(
            "HTTP/1.1 503 Service Unavailable",
            r#"{"error":"not ready"}"#,
        );
        assert!(matches!(body_of(&raw), Err(PollError::Status(503))));
    }

    #[test]
    fn a_target_that_died_mid_reply_is_survived() {
        for raw in [
            b"".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Len".as_slice(),
            b"\0\0\0\0\r\n\r\nbody".as_slice(),
            b"garbage".as_slice(),
        ] {
            assert!(
                body_of(raw).is_err(),
                "{raw:?} must not be read as a report"
            );
        }
    }

    #[test]
    fn a_body_that_is_not_a_report_is_an_error_rather_than_a_panic() {
        let raw = reply("HTTP/1.1 200 OK", "not json at all");
        let body = body_of(&raw).expect("the reply itself was well formed");
        let parsed: Result<JsonFunctionsList, _> = serde_json::from_slice(&body);
        assert!(parsed.is_err());
    }

    #[test]
    fn an_empty_two_hundred_does_not_deserialize_into_an_empty_report() {
        let raw = reply("HTTP/1.1 200 OK", "");
        let body = body_of(&raw).expect("an empty body is still a body");
        let parsed: Result<JsonFunctionsList, _> = serde_json::from_slice(&body);
        assert!(
            parsed.is_err(),
            "an empty body must not become a report with no functions"
        );
    }
}
