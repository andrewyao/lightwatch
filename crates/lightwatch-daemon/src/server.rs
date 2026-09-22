//! The HTTP surface the web UI reads.

use std::sync::Arc;

use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use include_dir::{include_dir, Dir};
use tokio::sync::broadcast::error::RecvError;
use tracing::warn;

use crate::api::{self, Resolution, SnapshotView};
use crate::session::{self, Feed};
use crate::store::{now_unix_ms, ProcessId, Registry, Update};

/// The web UI, embedded at compile time. Editing anything under `web/` needs a
/// rebuild of this crate to reach a running daemon.
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
        .route("/api/sessions", get(list_sessions))
        .route("/api/sessions/{id}/stream", get(session_stream))
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

/// What a client may ask a snapshot to cover.
#[derive(Debug, serde::Deserialize)]
struct SnapshotParams {
    resolution: Option<String>,
    limit: Option<usize>,
}

async fn snapshot(
    State(registry): State<Arc<Registry>>,
    Path(id): Path<String>,
    Query(params): Query<SnapshotParams>,
) -> Result<Json<api::Snapshot>, StatusCode> {
    // A resolution nobody recognizes is a client bug, and answering it with
    // the fine view would hide that behind an axis quietly a quarter the
    // length the client asked for.
    let resolution = match params.resolution.as_deref() {
        None => Resolution::Fine,
        Some(raw) => Resolution::parse(raw).ok_or(StatusCode::BAD_REQUEST)?,
    };
    let view = SnapshotView { resolution, limit: params.limit.unwrap_or(usize::MAX) };

    let state = registry.get(&ProcessId::from(id.as_str())).ok_or(StatusCode::NOT_FOUND)?;
    let process = state.read().expect("process lock");
    Ok(Json(api::snapshot(&process, now_unix_ms(), view)))
}

async fn list_sessions(State(registry): State<Arc<Registry>>) -> Json<api::SessionList> {
    Json(api::session_list(session::pair_sessions(&registry)))
}

/// Both of a session's feeds down one socket. There is no matching snapshot
/// route: a session names its process ids, and each one already has a snapshot.
async fn session_stream(
    State(registry): State<Arc<Registry>>,
    Path(id): Path<String>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let Some(session) = session::find(&registry, &id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let subscribe = |member: Option<&session::Member>| {
        let member = member?;
        let state = registry.get(&member.process_id)?;
        let updates = state.read().expect("process lock").subscribe();
        Some((member.process_id.clone(), updates))
    };
    let cpu = subscribe(session.cpu.as_ref());
    let memory = subscribe(session.memory.as_ref());
    if cpu.is_none() && memory.is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    upgrade.on_upgrade(move |socket| fan_in(socket, session.id, cpu, memory))
}

type Updates = tokio::sync::broadcast::Receiver<Arc<Update>>;

/// One message per closed window from either feed, tagged with which.
///
/// `select!` over the two receivers rather than a task per feed: the socket has
/// one writer, and a second task would need a channel to reach it that would
/// only re-buffer what `broadcast` already buffers.
async fn fan_in(
    mut socket: WebSocket,
    session_id: String,
    cpu: Option<(ProcessId, Updates)>,
    memory: Option<(ProcessId, Updates)>,
) {
    // A session with one emitter still has to wait on its socket and on the
    // feed it does have. The missing half becomes a receiver whose sender
    // nobody holds but this function, so it pends instead of reporting itself
    // closed and spinning the loop.
    let (idle, _) = tokio::sync::broadcast::channel::<Arc<Update>>(1);
    let absent = || (ProcessId::from(""), idle.subscribe());
    let (cpu_id, mut cpu_updates) = cpu.unwrap_or_else(absent);
    let (memory_id, mut memory_updates) = memory.unwrap_or_else(absent);

    loop {
        let message = tokio::select! {
            update = cpu_updates.recv() => {
                match tagged(&session_id, Feed::Cpu, &cpu_id, update) {
                    Tagged::Send(message) => Some(message),
                    Tagged::Skip => None,
                    Tagged::Stop => break,
                }
            }
            update = memory_updates.recv() => {
                match tagged(&session_id, Feed::Memory, &memory_id, update) {
                    Tagged::Send(message) => Some(message),
                    Tagged::Skip => None,
                    Tagged::Stop => break,
                }
            }
            // A client that goes away is only noticed by reading from it.
            incoming = socket.recv() => match incoming {
                None | Some(Err(_)) => break,
                Some(Ok(_)) => None,
            },
        };
        let Some(message) = message else { continue };
        let Ok(text) = serde_json::to_string(&message) else { break };
        if socket.send(WsMessage::Text(text.into())).await.is_err() {
            break;
        }
    }
}

enum Tagged {
    Send(api::SessionStreamMessage),
    Skip,
    Stop,
}

fn tagged(
    session_id: &str,
    feed: Feed,
    process_id: &ProcessId,
    update: Result<Arc<Update>, RecvError>,
) -> Tagged {
    match update {
        Ok(update) => {
            Tagged::Send(api::session_stream_message(session_id, feed, process_id, &update))
        }
        Err(RecvError::Lagged(missed)) => {
            let lost = "a session client fell behind and lost windows";
            warn!(session = %session_id, process = %process_id, missed, lost);
            Tagged::Skip
        }
        Err(RecvError::Closed) => Tagged::Stop,
    }
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
  GET /api/processes/{id}/snapshot      functions, types, the call tree, and a ring
                                        ?resolution=fine|coarse &limit=<n>
  GET /api/processes/{id}/stream        websocket, one message per closed window
  GET /api/sessions                     one entry per program, both emitters joined
  GET /api/sessions/{id}/stream         websocket, both feeds, each message tagged

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
        for route in [
            "/api/processes",
            "/api/processes/{id}/snapshot",
            "/api/processes/{id}/stream",
            "/api/sessions",
            "/api/sessions/{id}/stream",
        ] {
            assert!(PLACEHOLDER.contains(route), "the placeholder does not mention {route}");
        }
    }

    #[test]
    fn a_resolution_nobody_recognises_is_refused_rather_than_quietly_narrowed() {
        assert_eq!(Resolution::parse("fine"), Some(Resolution::Fine));
        assert_eq!(Resolution::parse("coarse"), Some(Resolution::Coarse));

        // Answering these with the fine view would hand back a minute of
        // history to a client that asked for a quarter of an hour, and
        // nothing in the response would say so.
        for raw in ["FINE", "1s", "", "fine "] {
            assert_eq!(Resolution::parse(raw), None, "`{raw}` is not a resolution");
        }
    }

    #[test]
    fn a_bundle_file_is_served_with_a_type_a_browser_will_execute() {
        assert_eq!(mime_for("assets/app.js"), "text/javascript; charset=utf-8");
        assert_eq!(mime_for("index.html"), "text/html; charset=utf-8");
    }
}
