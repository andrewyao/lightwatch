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
pub const SCHEMA_VERSION: u32 = 2;

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
id_type!(
    PathId,
    "Identifies one calling context: a node in this process's call tree, meaning \
     a function reached by one particular chain of callers. `PathId(0)` is the \
     root, which is not a function and is never registered."
);

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
    /// One node of the call tree: `func`, reached through `parent`. A
    /// `parent` of [`PathId(0)`](PathId) means `func` was the outermost
    /// measured frame on its thread.
    ///
    /// Parent-linked rather than carrying the whole frame list, so every
    /// prefix is sent exactly once however many leaves grow out of it. A
    /// node's `id` is therefore always greater than its `parent`: an emitter
    /// cannot name a context without already holding the one it hangs off.
    ///
    /// The tree is process-wide, not per-thread. Two threads running the same
    /// chain of calls share a node and their times merge, which is what a
    /// flame graph wants and is also why a path can never answer *which
    /// thread*. Per-thread attribution is a further dimension on this event,
    /// not something a client can recover.
    Path {
        id: PathId,
        parent: PathId,
        func: FunctionId,
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
    /// Delta. `count` activations of this calling context closed during the
    /// window, having spent `self_ns` nanoseconds *outside* any measured
    /// callee.
    ///
    /// Self time, not inclusive time: a caller's own `self_ns` excludes what
    /// its measured callees spent, so summing a subtree gives that subtree's
    /// inclusive time and no nanosecond is counted twice. Time inside an
    /// *un*measured callee is not excluded — it lands on the nearest measured
    /// caller, which is the same collapse rule the call graph already applies.
    ///
    /// `count` counts activations that *opened* this context. For a recursive
    /// function that is one per outermost entry, not one per level, matching
    /// [`Event::Calls`]. It follows that summing a function's contexts equals
    /// its `Calls` count only when it never recursed; when it did, the
    /// recursion node contributes one more.
    ///
    /// A plain `u64` rather than a [`Dist`], because a flame graph needs a
    /// width. The per-call distribution is not lost: [`Event::Calls`] still
    /// carries it per function. What is not here is a distribution *per
    /// context* — how long `decode` takes specifically when `import` called
    /// it — which is a further dimension, not a field this one should grow.
    Stack {
        path: PathId,
        count: u64,
        self_ns: u64,
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
                Register::Path { id: PathId(1), parent: PathId(0), func: FunctionId(1) },
                Register::Path { id: PathId(2), parent: PathId(1), func: FunctionId(2) },
            ],
            events: vec![
                Event::Calls { func: FunctionId(1), count: 16, ns: Dist::Raw { v: vec![2_110_000, 5_750_000] } },
                Event::Edge { from: FunctionId(1), to: FunctionId(2), count: 16 },
                Event::Stack { path: PathId(2), count: 16, self_ns: 4_300_000 },
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
            "registers":[{"kind":"function","id":4,"name":"handle_request"},
                         {"kind":"function","id":9,"name":"parse_body"},
                         {"kind":"path","id":1,"parent":0,"func":4},
                         {"kind":"path","id":2,"parent":1,"func":9}],
            "events":[{"kind":"calls","func":4,"count":12,"ns":{"enc":"raw","v":[900,1200]}},
                      {"kind":"stack","path":2,"count":12,"self_ns":8400}]}"#;
        let decoded = Message::from_json_line(&line.replace('\n', "")).expect("decodes");
        let Message::Frame(frame) = decoded else { panic!("expected a frame") };
        assert_eq!(
            frame.events[0],
            Event::Calls { func: FunctionId(4), count: 12, ns: Dist::Raw { v: vec![900, 1200] } }
        );
        assert_eq!(frame.events[1], Event::Stack { path: PathId(2), count: 12, self_ns: 8_400 });
        assert_eq!(
            frame.registers[3],
            Register::Path { id: PathId(2), parent: PathId(1), func: FunctionId(9) },
            "a bare integer parent, not an object, is what a client in another language writes"
        );
    }

    #[test]
    fn a_path_node_is_always_numbered_above_the_one_it_hangs_off() {
        // The daemon may lean on this to register a frame's paths in one
        // pass, so it is a property of the protocol rather than an accident
        // of how the Rust probe interns.
        let frame = sample_frame();
        for register in &frame.registers {
            if let Register::Path { id, parent, .. } = register {
                assert!(id.0 > parent.0, "{id:?} hangs off {parent:?}");
            }
        }
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
