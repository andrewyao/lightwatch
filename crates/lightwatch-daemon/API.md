# lightwatch daemon API

The daemon ingests protocol streams from profiled processes and serves them
over HTTP. This file is the contract a client writes against.

## Running it

| Variable | Default | What it sets |
| --- | --- | --- |
| `LIGHTWATCH_SOCK_DIR` | `$TMPDIR/lightwatch` on macOS, else `$XDG_RUNTIME_DIR/lightwatch` | Directory holding the ingest socket. Created if absent. |
| `LIGHTWATCH_PORT` | `7700` | HTTP port, bound on `127.0.0.1` only. |
| `LIGHTWATCH_LOG` | `info` | `tracing` filter, for example `lightwatch_daemon::ingest=debug`. |

Emitters connect to `$LIGHTWATCH_SOCK_DIR/lightwatch.sock` and write
newline-delimited JSON, one `lightwatch_proto::Message` per line, `hello`
first. See `crates/lightwatch-proto/src/lib.rs` for the message shapes.

## The one thing to get right

`calls` and `stack` events are **deltas** and accumulate. A `census` event is an
**absolute reading** at window close and replaces the previous one. The API
keeps them apart by name. A field called `*_total` or a window's `calls`,
`edges` and `stacks` accumulate; a `census` object is a reading and must never
be summed across windows. Charting a census as a sum produces a number that
grows forever and means nothing.

This bites hardest when a client folds several windows into one bucket for a
chart. Deltas add. A census takes the **last** reading in the bucket, and when
a bucket has none it carries the previous one forward, because a type that had
nothing to say did not cease to exist. The daemon has `Cumulative` and
`Absolute` to make the mistake a compile error; a client has only this
paragraph and the observation that memory has to be able to fall.

## Time

The daemon buckets frames by the emitter's own monotonic `t_ns`, not by the
`window_ms` the emitter declares. The fine ring holds 600 windows of 100ms and
the coarse ring holds 900 windows of 1s, so a process is visible at full detail
for the last minute and at second resolution for the last fifteen. An emitter
that picks a different `window_ms` folds into the same axis instead of giving
each process its own.

A window's `index` is `t_ns / <the resolution you asked for>`. Windows are
contiguous and ascending, and an idle stretch appears as empty windows rather
than a gap in the series.

Both rings are reachable: see `?resolution=` below. Note that a window can hold
more self time than it holds wall clock. The call tree is process-wide, so N
busy threads contribute N times the clock, and a client that normalises against
the window duration will render a busy program at several hundred percent.

## `GET /api/processes`

Every stream this daemon has seen since it started, newest first. A stream
that disconnects stays listed with `ended_unix_ms` set.

An `id` is `{pid}-{started_unix_ms}-{source}` and is opaque: read it, pass it
back, do not parse it. The source is part of it because two emitters watching
one OS process agree on the pid and can agree on the start millisecond too, and
folding their frames into one history corrupts every call total. `GET
/api/sessions` is what puts the two back together.

```json
{
  "processes": [
    {
      "id": "3218-1700000000000-lightwatch-probe",
      "app": "lightphotos",
      "pid": 3218,
      "source": "lightwatch-probe",
      "started_unix_ms": 1700000000000,
      "ended_unix_ms": null,
      "connected": true,
      "window_ms": 100,
      "last_frame_unix_ms": 1789925630120,
      "last_t_ns": 400000000,
      "counts": {
        "frames": 5,
        "events": 11,
        "functions": 2,
        "types": 1,
        "edges": 2,
        "paths": 4,
        "missed_frames": 3,
        "late_frames": 0,
        "unknown_id": 1,
        "malformed_lines": 1
      }
    }
  ]
}
```

`id` is `"<pid>-<started_unix_ms>"`. A process that reconnects resumes the same
id and the history behind it; a recycled pid is a different process because its
start time differs.

`connected` is live socket state. `ended_unix_ms` is when the last connection
for this process closed, and is null while it is streaming.

`window_ms` is what the emitter declared it was doing, which is not the
daemon's own window length. Use `resolution_ms` from a snapshot for the axis.

The counts are the daemon's view of feed health. `missed_frames` counts windows
the emitter itself admits it dropped, read off gaps in `seq`. `late_frames`
counts frames whose `t_ns` fell before the oldest window still retained.
`unknown_id` counts events naming a function, type or context that was never
registered, plus context registrations the daemon refused: one naming an
unregistered function, one hanging off an unregistered or higher-numbered
parent, and one that would redefine a context events have already been
attributed to. Those events and registrations are dropped. `malformed_lines` counts lines that did not parse,
plus any second `hello` mid-stream. All four are cumulative and a non-zero
value means the feed is worth looking at, not that the daemon failed.

## `GET /api/processes/{id}/snapshot`

Everything known about one process. `404` if the id is unknown.

| Query | Default | What it does |
| --- | --- | --- |
| `resolution` | `fine` | Which ring the `windows` come from: `fine` (100ms, last minute) or `coarse` (1s, last quarter hour). Anything else is a `400` rather than a quiet fall back to `fine`. |
| `limit` | the ring's capacity | Return at most this many of the **most recent** windows. |

`limit` is not decoration. The coarse ring is nine hundred windows deep and a
window can name hundreds of contexts, so a client that wants a two-minute axis
should ask for a two-minute axis.

The `*_per_sec` fields are always averaged over the fine ring whatever
`resolution` says. Asking for a longer axis is not asking for "per second" to
start meaning something else.

```json
{
  "process": { "...": "the same object as in the list" },
  "taken_unix_ms": 1789925684934,
  "rate_window_ms": 1000,
  "resolution": "fine",
  "resolution_ms": 100,
  "resolution_capacity": 600,
  "functions": [
    {
      "id": 1,
      "name": "thumbnail::get_or_make",
      "module": "lightphotos",
      "location": { "file": "src/thumbnail.rs", "line": 407, "column": 12 },
      "calls_total": 40,
      "ns_total": 12160000,
      "self_ns_total": 3040000,
      "calls_per_sec": 100.0,
      "ns_per_sec": 30400000.0,
      "self_ns_per_sec": 7600000.0,
      "ns_buckets": [{ "lo": 1835008, "hi": 1900543, "count": 1 }]
    }
  ],
  "types": [
    {
      "id": 1,
      "name": "Thumbnail",
      "location": null,
      "census": {
        "live": 5,
        "bytes": 1500,
        "at_t_ns": 400000000,
        "size_buckets": [{ "lo": 288, "hi": 303, "count": 5 }]
      }
    }
  ],
  "edges": [{ "from": 1, "to": 2, "calls_total": 40 }],
  "paths": [
    { "id": 1, "parent": 0, "func": 1, "calls_total": 40, "self_ns_total": 3040000 },
    { "id": 2, "parent": 1, "func": 2, "calls_total": 40, "self_ns_total": 9120000 }
  ],
  "windows": [
    {
      "index": 1,
      "start_t_ns": 100000000,
      "end_t_ns": 200000000,
      "calls": [{ "func": 1, "calls": 16, "ns": 7860000 }],
      "edges": [{ "from": 1, "to": 2, "calls": 16 }],
      "stacks": [{ "path": 2, "calls": 16, "self_ns": 4300000 }],
      "census": [{ "ty": 1, "live": 3, "bytes": 900 }]
    }
  ]
}
```

**functions.** `calls_total` and `ns_total` accumulate over the whole process
lifetime. `calls_per_sec` and `ns_per_sec` are averaged over the last
`rate_window_ms` of the fine ring, so they answer "what is it doing now"; the
window still filling is included, which understates the newest rate slightly.
`ns_buckets` is the lifetime duration histogram, already resolved to value
ranges in nanoseconds so a client never has to port the bucketing. `lo` and
`hi` are inclusive and `count` is how many calls landed in that range.

`ns_total` is exact when the emitter sent raw samples. When it sent a
histogram, the total is a bucket-midpoint estimate, within the 6.25% the
protocol's bucketing guarantees.

`self_ns_total` is time spent in the function and outside any measured callee,
summed over every context it was reached through. Unlike `ns_total`, adding it
across every function double-counts nothing, so it is the field to chart a
process's own time with.

**types.** `census` is the latest reading, or null when that type has never
reported one. `live` and `bytes` are what was true at `at_t_ns` and are not
running totals. `size_buckets` describes the sizes of those live instances,
with `lo` and `hi` in bytes.

**edges.** Derived from `paths`: a context names a function and the context
that reached it, so the pair is already there and its weight is how often that
context was entered. `calls_total` accumulates over the whole process lifetime
and never expires, so the call graph does not flicker as an edge goes quiet.
Only edges where both ends are instrumented exist, so an uninstrumented frame
between two measured functions shows as a direct edge.

**paths.** The call tree, kept for the process lifetime like `edges`. A node is
a function reached through one particular chain of callers; `parent` names the
context above it, and `0` means nothing measured called it. A node's `id` is
always greater than its `parent`, so walking upward terminates.

`self_ns_total` is time in that context and outside any measured callee, so
summing a subtree gives that subtree's inclusive time with nothing counted
twice. That is what makes a flame graph drawable from this: box width is the
subtree sum, and no client has to subtract anything.

A recursive function is two contexts however deep it goes: its outermost one,
and one shared by every level below, hanging off the first. `calls_total` on a
context counts activations that *opened* it, so for the shared one that is one
per outermost entry, not one per level.

**windows.** The ring you asked for, oldest first, at most
`resolution_capacity` entries and at most `limit` of them. Inside a window, `calls` and `edges` are that window's own deltas and
`census` is the reading taken at its close. An empty window is an idle stretch,
not a hole.

Function and type ids are unique within one process and are never reused, so a
client can key on `id` and look the name up once.

## `GET /api/processes/{id}/stream`

A WebSocket that pushes one message per closed window. A fine window closes
when a frame arrives that belongs to a later one, and the window still open is
flushed when the process disconnects. Windows that stayed empty are not pushed.
`404` before the upgrade if the id is unknown.

```json
{ "type": "window", "process_id": "3218-1700000000000-lightwatch-probe", "window": { "...": "the same window object as in a snapshot" } }
{ "type": "process_ended", "process_id": "3218-1700000000000-lightwatch-probe", "ended_unix_ms": 1789925695446 }
```

Fetch a snapshot first and then subscribe. A client that falls more than 256
windows behind loses the ones in between and the daemon logs it; the next
snapshot is the way back to a correct picture.

Only **fine** windows are pushed, whatever resolution the snapshot was taken
at. A client on a coarse axis folds them itself or re-polls.

## `GET /api/sessions`

One entry per running program rather than per emitter. A target instrumented
for both halves connects twice, once from the probe inside it and once from a
bridge outside it, and the two report `started_unix_ms` computed by different
arithmetic, so they are two processes to `/api/processes` and one session here.

The join is on `(app, pid)`, within ten seconds of start time so that a pid the
OS handed out again is a separate session. It is recomputed per request and
never stored; ingest knows nothing about it.

```json
{
  "sessions": [
    {
      "id": "lightphotos-64376-1790000000000",
      "app": "lightphotos",
      "pid": 64376,
      "connected": true,
      "cpu": {
        "process_id": "64376-1790000000137-lightwatch-hotpath",
        "feed": "cpu",
        "source": "lightwatch-hotpath",
        "started_unix_ms": 1790000000137,
        "ended_unix_ms": null,
        "connected": true,
        "last_frame_unix_ms": 1790000012044
      },
      "memory": { "...": "the same shape, feed \"memory\", source \"lightwatch-probe\"" },
      "superseded": []
    }
  ]
}
```

`cpu` and `memory` each name a `process_id` that
`/api/processes/{id}/snapshot` accepts, which is where a client gets its first
picture. Either may be `null`: half a picture is what there is until both
emitters are running.

`superseded` holds entries that matched this session and lost to a better one
of their own feed. A bridge restarted against a target that never died leaves
one behind, because its start estimate is `now - elapsed` and that drifts
between runs. The connected entry wins, then the newest one; the loser is
reported here rather than discarded, so nothing looks arbitrary.

## `GET /api/sessions/{id}/stream`

Both of the session's feeds down one WebSocket, each message tagged with which
one it came from. Otherwise identical to the per-process stream. `404` before
the upgrade if the id names no session.

```json
{ "type": "window", "session_id": "lightphotos-64376-1790000000000", "feed": "cpu", "process_id": "64376-1790000000137-lightwatch-hotpath", "window": { "...": "as in a snapshot" } }
{ "type": "window", "session_id": "lightphotos-64376-1790000000000", "feed": "memory", "process_id": "64376-1790000000000-lightwatch-probe", "window": { "...": "as in a snapshot" } }
```

There is no `/api/sessions/{id}/snapshot`. A session names its process ids and
each one already has a snapshot, so a second route would only be the two of
them concatenated.

A feed that connects after the socket is open does not join it. Re-read
`/api/sessions` and reconnect when a session gains its other half.

## `GET /`

Serves the web UI embedded from `crates/lightwatch-daemon/web/`: a flame graph
of the call tree and a force-directed call graph, over a shared axis of CPU
self time and live bytes per bucket, driven by `/api/sessions/{id}/stream` and
falling back to snapshot polling whenever that socket is down. Plain ES modules, no build step, but `include_dir!` reads the
directory at compile time, so an edit under `web/` needs a rebuild to be served.

Any path the bundle does not contain falls through to its `index.html`, so a
single-page UI keeps its own routing. A build whose `web/` holds no
`index.html` answers `200` with a plain-text page naming these routes instead.

## Refusals at the boundary

A `hello` whose `schema` is not `lightwatch_proto::SCHEMA_VERSION` (currently
`2`) is refused
with a logged reason and the connection closes; nothing is registered. A stream
whose first line is not a parseable `hello` is refused the same way. After the
handshake the daemon is forgiving: a line that does not parse is counted and
skipped, and an event naming an unregistered id is counted and dropped. A
client that sends more than 4MiB without a newline has its connection closed
rather than buffered for.

## Checking it by hand

`scripts/feed-fixture.sh` starts a daemon, feeds it a deliberately hostile
fixture over `nc -U`, and prints every response above, including the WebSocket
push. Run it from the repository root.
