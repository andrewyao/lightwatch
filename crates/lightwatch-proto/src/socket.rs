//! Where a daemon and its emitters meet.
//!
//! The rendezvous is as much of the contract as the frames are: a stream
//! nobody can find carries nothing. It lives here, in the crate every
//! participant already depends on, because the rule is only useful while the
//! binder and the connectors agree on it, and a copy per binary is a rule that
//! can drift apart one edit at a time.

use std::path::PathBuf;

/// The socket the daemon listens on inside its socket directory.
pub const SOCKET_FILE: &str = "lightwatch.sock";

/// `$LIGHTWATCH_SOCK_DIR`, else `$TMPDIR/lightwatch` on macOS, else
/// `$XDG_RUNTIME_DIR/lightwatch`, else `lightwatch` under the platform's
/// temporary directory.
pub fn socket_dir() -> PathBuf {
    resolve_socket_dir(
        std::env::var_os("LIGHTWATCH_SOCK_DIR").map(PathBuf::from),
        std::env::var_os("TMPDIR").map(PathBuf::from),
        std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from),
        cfg!(target_os = "macos"),
        std::env::temp_dir(),
    )
}

/// Takes the environment as arguments so the rule is testable without
/// mutating a process-wide variable.
fn resolve_socket_dir(
    explicit: Option<PathBuf>,
    tmpdir: Option<PathBuf>,
    xdg_runtime: Option<PathBuf>,
    on_macos: bool,
    temp_dir: PathBuf,
) -> PathBuf {
    if let Some(explicit) = explicit {
        return explicit;
    }
    if on_macos {
        if let Some(tmpdir) = tmpdir {
            return tmpdir.join("lightwatch");
        }
    }
    match xdg_runtime {
        Some(runtime) => runtime.join("lightwatch"),
        None => temp_dir.join("lightwatch"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_socket_directory_prefers_the_explicit_override_then_the_platform_default() {
        let explicit = Some(PathBuf::from("/run/chosen"));
        let tmpdir = Some(PathBuf::from("/var/tmp-per-user"));
        let xdg = Some(PathBuf::from("/run/user/501"));
        let fallback = PathBuf::from("/tmp");

        assert_eq!(
            resolve_socket_dir(explicit.clone(), tmpdir.clone(), xdg.clone(), true, fallback.clone()),
            PathBuf::from("/run/chosen")
        );
        assert_eq!(
            resolve_socket_dir(None, tmpdir.clone(), xdg.clone(), true, fallback.clone()),
            PathBuf::from("/var/tmp-per-user/lightwatch")
        );
        assert_eq!(
            resolve_socket_dir(None, tmpdir, xdg.clone(), false, fallback.clone()),
            PathBuf::from("/run/user/501/lightwatch")
        );
        assert_eq!(
            resolve_socket_dir(None, None, None, false, fallback),
            PathBuf::from("/tmp/lightwatch")
        );
    }
}
