//! The HTTP surface the web UI reads.

use std::sync::Arc;

use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use include_dir::{include_dir, Dir};
use tokio::sync::broadcast::error::RecvError;
use tracing::warn;

use crate::api;
use crate::store::{now_unix_ms, ProcessId, Registry, Update};

/// The web UI's built bundle. Empty until a UI is built into it, at which point
/// `/` starts serving the bundle instead of the placeholder.
static WEB: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/web");

pub const DEFAULT_PORT: u16 = 7700;

pub fn port() -> u16 {
    std::env::var("LIGHTWATCH_PORT")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_PORT)
}

pub fn router(registry: Arc<Registry>) -> Router {
    Router::new()
        .route("/api/processes", get(list_processes))
        .route("/api/processes/{id}/snapshot", get(snapshot))
        .route("/api/processes/{id}/stream", get(stream))
        .fallback(static_file)
        .with_state(registry)
}

async fn list_processes(State(registry): State<Arc<Registry>>) -> Json<api::ProcessList> {
    let mut processes: Vec<api::ProcessSummary> = registry
        .all()
        .iter()
        .map(|state| api::summary(&state.read().expect("process lock")))
        .collect();
    processes.sort_by(|a, b| b.started_unix_ms.cmp(&a.started_unix_ms).then(a.id.cmp(&b.id)));
    Json(api::ProcessList { processes })
}

async fn snapshot(
    State(registry): State<Arc<Registry>>,
    Path(id): Path<String>,
) -> Result<Json<api::Snapshot>, StatusCode> {
    let state = registry.get(&ProcessId::from(id.as_str())).ok_or(StatusCode::NOT_FOUND)?;
    let process = state.read().expect("process lock");
    Ok(Json(api::snapshot(&process, now_unix_ms())))
}

async fn stream(
    State(registry): State<Arc<Registry>>,
    Path(id): Path<String>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let id = ProcessId::from(id.as_str());
    let Some(state) = registry.get(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let updates = state.read().expect("process lock").subscribe();
    upgrade.on_upgrade(move |socket| pump(socket, updates, id))
}

async fn pump(
    mut socket: WebSocket,
    mut updates: tokio::sync::broadcast::Receiver<Arc<Update>>,
    id: ProcessId,
) {
    loop {
        tokio::select! {
            update = updates.recv() => match update {
                Ok(update) => {
                    let message = api::stream_message(&id, &update);
                    let Ok(text) = serde_json::to_string(&message) else { break };
                    if socket.send(WsMessage::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Lagged(missed)) => {
                    warn!(process = %id, missed, "a websocket client fell behind and lost windows");
                }
                Err(RecvError::Closed) => break,
            },
            // A client that goes away is only noticed by reading from it.
            incoming = socket.recv() => match incoming {
                None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            },
        }
    }
}

async fn static_file(uri: Uri) -> Response {
    let requested = uri.path().trim_start_matches('/');
    let path = if requested.is_empty() { "index.html" } else { requested };

    if let Some(file) = WEB.get_file(path) {
        return ([(header::CONTENT_TYPE, mime_for(path))], file.contents()).into_response();
    }
    // A single-page UI owns its own routes, so an unknown path below a real
    // bundle is its problem to render rather than a 404 from here.
    match WEB.get_file("index.html") {
        Some(index) => ([(header::CONTENT_TYPE, mime_for("index.html"))], index.contents())
            .into_response(),
        None => (StatusCode::OK, [(header::CONTENT_TYPE, "text/plain; charset=utf-8")], PLACEHOLDER)
            .into_response(),
    }
}

const PLACEHOLDER: &str = "\
lightwatch daemon

No web UI is embedded in this build. The API is live:

  GET /api/processes                    every process this daemon has seen
  GET /api/processes/{id}/snapshot      functions, types, edges and the fine ring
  GET /api/processes/{id}/stream        websocket, one message per closed window

Emitters connect to the unix socket named by $LIGHTWATCH_SOCK_DIR.
See crates/lightwatch-daemon/API.md for the response shapes.
";

fn mime_for(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "webp" => "image/webp",
        "woff2" => "font/woff2",
        "map" => "application/json",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_placeholder_names_every_route_the_daemon_serves() {
        for route in ["/api/processes", "/api/processes/{id}/snapshot", "/api/processes/{id}/stream"]
        {
            assert!(PLACEHOLDER.contains(route), "the placeholder does not mention {route}");
        }
    }

    #[test]
    fn a_bundle_file_is_served_with_a_type_a_browser_will_execute() {
        assert_eq!(mime_for("assets/app.js"), "text/javascript; charset=utf-8");
        assert_eq!(mime_for("index.html"), "text/html; charset=utf-8");
    }
}
