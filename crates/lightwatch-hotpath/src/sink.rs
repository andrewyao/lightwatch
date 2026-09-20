//! Writing lightwatch frames to the daemon.
//!
//! One connection describes one run of one target process. It opens with a
//! [`Hello`] and carries newline-delimited frames until either end goes away.
//! A write that fails ends the connection, and the caller opens a fresh one
//! whose first frame registers every function again.

use lightwatch_proto::{Frame, Hello, Message};
use std::io::{self, BufWriter, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

pub const DEFAULT_SOCKET: &str = "/tmp/lightwatch.sock";

/// An open stream to the daemon.
pub struct Connection {
    writer: BufWriter<UnixStream>,
}

impl Connection {
    /// Connects and announces the target. Fails if the daemon is not listening.
    pub fn open(path: &Path, hello: &Hello) -> io::Result<Self> {
        let mut connection = Connection {
            writer: BufWriter::new(UnixStream::connect(path)?),
        };
        connection.write(&Message::Hello(hello.clone()))?;
        Ok(connection)
    }

    pub fn send(&mut self, frame: Frame) -> io::Result<()> {
        self.write(&Message::Frame(frame))
    }

    fn write(&mut self, message: &Message) -> io::Result<()> {
        self.writer.write_all(message.to_json_line().as_bytes())?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightwatch_proto::{Dist, Event, FunctionId};
    use std::io::{BufRead, BufReader};
    use std::os::unix::net::UnixListener;

    fn socket_path(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("lightwatch-hotpath-{name}.sock"));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn hello() -> Hello {
        Hello::new("lightphotos", "lightwatch-hotpath", 4384, 1_700_000_000_000)
    }

    #[test]
    fn a_daemon_reads_a_hello_then_one_line_per_frame() {
        let path = socket_path("stream");
        let listener = UnixListener::bind(&path).expect("binds");

        let mut connection = Connection::open(&path, &hello()).expect("connects");
        connection
            .send(Frame {
                seq: 0,
                t_ns: 1,
                registers: vec![],
                events: vec![Event::Calls {
                    func: FunctionId(7),
                    count: 3,
                    ns: Dist::empty(),
                }],
            })
            .expect("sends");

        let (accepted, _) = listener.accept().expect("accepts");
        let mut lines = BufReader::new(accepted).lines();
        let first = lines.next().expect("a hello arrived").expect("reads");
        let second = lines.next().expect("a frame arrived").expect("reads");

        assert_eq!(
            Message::from_json_line(&first).expect("the hello decodes"),
            Message::Hello(hello())
        );
        let Message::Frame(frame) = Message::from_json_line(&second).expect("the frame decodes")
        else {
            panic!("expected a frame")
        };
        assert_eq!(frame.events.len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_absent_daemon_is_an_error_the_caller_can_retry() {
        let path = socket_path("absent");
        assert!(Connection::open(&path, &hello()).is_err());
    }
}
