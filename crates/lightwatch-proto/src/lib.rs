//! The lightwatch wire protocol.
//!
//! A profiled process opens a socket, sends one [`Message::Hello`], then a
//! [`Message::Frame`] per window until it exits. Frames carry aggregate deltas,
//! never individual hits, so the cost of being observed stays flat as the
//! program gets busier.
//!
//! The encoding is one JSON object per line. That is the whole contract a
//! client in another language has to meet. MessagePack over the same socket is
//! an optimization for the Rust probe, not a second protocol.
//!
//! Which socket is part of that contract as well, so [`socket`] carries the
//! rule every participant finds it by.

pub mod dist;
pub mod socket;

pub use dist::{bucket_of, bucket_range, Dist};
pub use socket::{socket_dir, SOCKET_FILE};

use serde::{Deserialize, Serialize};

/// Bumped when a frame's meaning changes. The daemon refuses a mismatch rather
/// than guessing at a field it does not recognize.
pub const SCHEMA_VERSION: u32 = 1;

/// Default window length. An emitter may choose its own and declare it in
/// [`Hello::window_ms`].
pub const DEFAULT_WINDOW_MS: u32 = 100;

macro_rules! id_type {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub u32);
    };
}

id_type!(FunctionId, "Identifies a function within one process. Interned by the emitter, never reused.");
id_type!(TypeId, "Identifies a tracked type within one process.");

/// Where a symbol was declared. Paths are relative to the emitter's root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    pub file: String,
    pub line: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
}

/// Binds an id to a name. Sent once, the first time the id appears, so names
/// never repeat on the wire and a steady-state frame is only integers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Register {
    Function {
        id: FunctionId,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        module: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        location: Option<Location>,
    },
    Type {
        id: TypeId,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        location: Option<Location>,
    },
}

/// What happened during one window.
///
/// `Calls` and `Edge` are deltas and accumulate across windows. `Census` is an
/// absolute reading at window close and replaces the previous one. Mixing the
/// two up is the easiest bug to write against this protocol, so the daemon's
/// store keeps them in separate types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// Delta. `count` calls of `func` closed during the window, with their
    /// wall durations in nanoseconds.
    Calls {
        func: FunctionId,
        count: u64,
        #[serde(default = "Dist::empty", skip_serializing_if = "Dist::is_empty")]
        ns: Dist,
    },
    /// Delta. `from` called `to` this many times during the window. Only edges
    /// where both ends are instrumented are visible, so an uninstrumented frame
    /// between two measured functions collapses into a direct edge.
    Edge {
        from: FunctionId,
        to: FunctionId,
        count: u64,
    },
    /// Absolute. Live instances of `ty` at window close, their total footprint
    /// in bytes, and the sizes of those live instances.
    Census {
        ty: TypeId,
        live: u64,
        bytes: u64,
        #[serde(default = "Dist::empty", skip_serializing_if = "Dist::is_empty")]
        sizes: Dist,
    },
}

/// One window's worth of observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frame {
    /// Increments by one per frame. A gap means the emitter dropped a window.
    pub seq: u64,
    /// Monotonic nanoseconds since process start, at window close.
    pub t_ns: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub registers: Vec<Register>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<Event>,
}

/// The first message on a connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub schema: u32,
    /// What to call this process in the UI.
    pub app: String,
    pub pid: u32,
    /// Wall clock at process start, so a recycled pid is still distinguishable.
    pub started_unix_ms: u64,
    pub window_ms: u32,
    /// Which emitter produced the stream, for diagnosing a bad feed.
    pub source: String,
}

impl Hello {
    pub fn new(app: impl Into<String>, source: impl Into<String>, pid: u32, started_unix_ms: u64) -> Self {
        Hello {
            schema: SCHEMA_VERSION,
            app: app.into(),
            pid,
            started_unix_ms,
            window_ms: DEFAULT_WINDOW_MS,
            source: source.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "msg", rename_all = "snake_case")]
pub enum Message {
    Hello(Hello),
    Frame(Frame),
}

impl Message {
    /// Encodes one line of the stream, without the newline.
    pub fn to_json_line(&self) -> String {
        serde_json::to_string(self).expect("protocol types are always serializable")
    }

    pub fn from_json_line(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_frame() -> Frame {
        Frame {
            seq: 7,
            t_ns: 700_000_000,
            registers: vec![
                Register::Function {
                    id: FunctionId(1),
                    name: "thumbnail::get_or_make".into(),
                    module: Some("lightphotos".into()),
                    location: Some(Location { file: "src/thumbnail.rs".into(), line: 407, column: Some(12) }),
                },
                Register::Type { id: TypeId(1), name: "Thumbnail".into(), location: None },
            ],
            events: vec![
                Event::Calls { func: FunctionId(1), count: 16, ns: Dist::Raw { v: vec![2_110_000, 5_750_000] } },
                Event::Edge { from: FunctionId(1), to: FunctionId(2), count: 16 },
                Event::Census { ty: TypeId(1), live: 3, bytes: 900, sizes: Dist::Raw { v: vec![300, 300, 300] } },
            ],
        }
    }

    #[test]
    fn a_frame_survives_a_json_round_trip() {
        let original = Message::Frame(sample_frame());
        let decoded = Message::from_json_line(&original.to_json_line()).expect("decodes");
        assert_eq!(original, decoded);
    }

    #[test]
    fn a_hello_survives_a_json_round_trip() {
        let original = Message::Hello(Hello::new("lightphotos", "lightwatch-probe", 3218, 1_700_000_000_000));
        let decoded = Message::from_json_line(&original.to_json_line()).expect("decodes");
        assert_eq!(original, decoded);
    }

    #[test]
    fn a_line_never_contains_a_newline_that_would_split_the_frame() {
        let line = Message::Frame(sample_frame()).to_json_line();
        assert!(!line.contains('\n'), "a frame with an embedded newline would desync the stream");
    }

    #[test]
    fn a_client_may_omit_every_optional_field() {
        let minimal = r#"{"msg":"frame","seq":0,"t_ns":0}"#;
        let decoded = Message::from_json_line(minimal).expect("minimal frame decodes");
        assert_eq!(
            decoded,
            Message::Frame(Frame { seq: 0, t_ns: 0, registers: vec![], events: vec![] })
        );
    }

    #[test]
    fn the_hand_written_shape_another_language_would_emit_decodes() {
        let line = r#"{"msg":"frame","seq":1,"t_ns":100000000,
            "registers":[{"kind":"function","id":4,"name":"handle_request"}],
            "events":[{"kind":"calls","func":4,"count":12,"ns":{"enc":"raw","v":[900,1200]}}]}"#;
        let decoded = Message::from_json_line(&line.replace('\n', "")).expect("decodes");
        let Message::Frame(frame) = decoded else { panic!("expected a frame") };
        assert_eq!(frame.events.len(), 1);
        assert_eq!(
            frame.events[0],
            Event::Calls { func: FunctionId(4), count: 12, ns: Dist::Raw { v: vec![900, 1200] } }
        );
    }

    #[test]
    fn an_unknown_schema_is_visible_to_the_daemon_rather_than_silently_accepted() {
        let line = r#"{"msg":"hello","schema":99,"app":"x","pid":1,"started_unix_ms":0,"window_ms":100,"source":"x"}"#;
        let Message::Hello(hello) = Message::from_json_line(line).expect("decodes") else {
            panic!("expected a hello")
        };
        assert_ne!(hello.schema, SCHEMA_VERSION);
    }
}
