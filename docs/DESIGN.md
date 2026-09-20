# How lightwatch is put together

## What problem it solves

Profiling a desktop app is normally a batch ritual. You run it, it exits, you
read a table. That cannot answer the question that matters while you are using
the program, which is what it is doing right now, during the interaction that
feels slow.

lightwatch attaches to a running process and shows two things. A graph of the
functions being called, with the edges that were actually taken. A census of the
objects alive at this instant, by type.

## What hotpath already provides

The `hotpath` crate does more than a batch profiler. `#[hotpath::main]` starts an
HTTP JSON server inside the profiled process on `127.0.0.1:6770`. It serves
`/functions_timing`, `/functions_alloc`, per-function logs, threads, channels and
a profiler status. `hotpath::json` and `hotpath::parse_duration` are public under
its `json` feature alone, so a client gets typed deserialization without linking
the profiling runtime.

A `JsonFunctionEntry` is already a function item. Listing functions, sorting them
by call count and showing their source locations needs no new instrumentation.

## What it does not provide, and why lightwatch exists

Call edges. `caller_stack.rs` compiles only under the SQL and HTTP features, and
it attributes a query to its nearest measured caller. Nothing in hotpath records
that one measured function called another.

Live objects. `hotpath-alloc` counts bytes allocated per function. There is no
type, no liveness and no free. How many `Thumbnail` values exist right now is
unanswerable at every feature combination.

Those two gaps are the whole design.

## The protocol is the product

`crates/lightwatch-proto` is the contract. A process opens a socket, sends one
`hello`, then one JSON object per line per window until it exits.

Three decisions keep observation from costing more than the thing observed.

Frames carry aggregate deltas per window, not individual hits. A per-hit stream
is the obvious design and the wrong one, because its cost grows with the
program's own workload.

Names are interned and sent once. A steady-state frame is integers.

Distributions travel as raw values or as log-linear buckets, and the daemon folds
both into the same form. Raw exists so a client in another language emits a
distribution without porting the bucketing function. That is what makes "works
against any application" a contract rather than a claim.

`Calls` and `Edge` are deltas that accumulate. `Census` is an absolute reading
that replaces the previous one. Accumulating a census as though it were a delta
is the easiest bug to write against this protocol, so the daemon's store keeps
the two in separate types.

## The hotpath bridge is a client, not a module

`lightwatch-hotpath` is a standalone process. It polls hotpath's HTTP server and
re-emits what it finds as lightwatch frames over the same socket any other
emitter uses.

Putting it inside the daemon would have been less code. Keeping it outside makes
the ingest protocol the only integration point, so the genericity claim is tested
by the first adapter rather than asserted about later ones.

It is a degraded source and says so. No edges, no census, and about four
significant figures on durations, because hotpath serves preformatted strings.
Its job is to light up the interface against an unmodified application. The Rust
probe is the full-fidelity path.

## The probe supplies the missing two

Edges come from a thread-local stack of function ids. Entering a measured
function reads the current top as its parent, records the pair in a per-thread
map and pushes itself. Only edges where both ends are instrumented are visible,
so an uninstrumented frame between two measured functions collapses into a direct
edge. That is the correct reading of which function depends on which.

Runtime capture beats compile-time analysis here on both accuracy and reach. It
sees dynamic dispatch, which static analysis cannot resolve, and it needs no
build-time tooling, so the same technique ports to any language that can push and
pop a stack.

Census comes from an injected field. `#[lightwatch::track]` is an attribute macro
rather than a derive, because Rust will not let you derive `Drop` and a tracked
type may already have one. The injected `Census<Self>` increments a per-type
counter and a size bucket on construction, and decrements the same bucket on
drop. The result is a histogram of live objects, which is what the interface
draws, rather than a histogram of constructions.

A type owning heap must override `Measured::bytes`. For a photo application the
interesting objects essentially are their heap buffers, so the `size_of` default
would report a uniformly useless number.

With the feature off, `Census<T>` is zero-sized and the measure macro returns the
function body unchanged, following the same discipline as hotpath's `lib_off`.

## The interface

Functions list on the left, sorted by a windowed call rate rather than a lifetime
total, or the ordering never moves. Each row has a toggle.

Toggling a row adds its node to the graph. Placement is computed, never manual,
by a layered top-down layout, because a call graph is mostly a directed acyclic
graph and reads as one. Right-clicking a node toggles on its callers or callees,
which is how a view grows without scrolling a long list.

The selection persists as a set of names, not coordinates. A set of names
survives a rebuild that moves every function.

Layout re-runs only when the visible set changes. Metric updates mutate style
alone. At ten frames a second, re-laying out on every update would make the graph
twitch and become unreadable.
