use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use lightwatch_daemon::{ingest, server, store::Registry};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("LIGHTWATCH_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let registry = Arc::new(Registry::new());

    let dir = ingest::socket_dir();
    let listener = match ingest::bind(&dir) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("lightwatch: cannot listen in {}: {err}", dir.display());
            return std::process::ExitCode::FAILURE;
        }
    };
    info!(socket = %dir.join(ingest::SOCKET_FILE).display(), "accepting emitters");

    // Loopback only: this is a developer tool and the streams it holds are a
    // program's internals.
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, server::port()));
    let http = match tokio::net::TcpListener::bind(address).await {
        Ok(http) => http,
        Err(err) => {
            eprintln!("lightwatch: cannot bind http://{address}: {err}");
            return std::process::ExitCode::FAILURE;
        }
    };
    info!(url = %format!("http://{address}"), "serving the api");

    let accepting = tokio::spawn(ingest::accept_loop(listener, Arc::clone(&registry)));
    let serving = tokio::spawn(async move {
        let _ = axum::serve(http, server::router(registry)).await;
    });

    let _ = tokio::signal::ctrl_c().await;
    info!("shutting down");
    accepting.abort();
    serving.abort();
    let _ = std::fs::remove_file(dir.join(ingest::SOCKET_FILE));
    std::process::ExitCode::SUCCESS
}
