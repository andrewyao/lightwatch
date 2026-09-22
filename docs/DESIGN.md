# How lightwatch is put together

## What problem it solves

Profiling a desktop app is normally a batch ritual. You run it, it exits, you
read a table. That cannot answer the question that matters while you are using
the program, which is what it is doing right now, during the interaction that
feels slow.

lightwatch attaches to a running process and shows what it was doing during a
window of its own recent past: a flame graph of the call tree, a graph of the
functions with the edges actually taken, and a census of the objects alive, all
over one time axis you can scrub.

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

Call edges, and the tree they belong to. `caller_stack.rs` compiles only under
the SQL and HTTP features, and it attributes a query to its nearest measured
caller. Nothing in hotpath records that one measured function called another,
let alone through which chain of callers.

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

`Calls` and `Stack` are deltas that accumulate. `Census` is an absolute reading
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

The call tree comes from a thread-local stack of function ids. Entering a
measured function interns the pair `(the context it was called from, itself)`
and pushes that context; leaving it reports the time it spent outside any
measured callee. Only frames that are instrumented appear, so an uninstrumented
frame between two measured ones collapses and its cost is charged to the nearest
measured caller. That is the correct reading of which function depends on which.

Self time rather than inclusive time, because self time adds up. Sum a subtree
and you have its inclusive time with no nanosecond counted twice; sum the whole
tree and you have what the process spent. Inclusive time on the wire would give
a client numbers it has to subtract before it can draw anything, and one dropped
window would make the subtraction wrong rather than merely incomplete.

A graph edge is then a reading of the tree rather than a second set of counters:
a context names a function and the context that reached it, so the pair is
already there. Two mechanisms for one fact can disagree; one cannot.

Recursion gets a single shared context hanging off the function's own outermost
node, so a thousand levels deep is two contexts rather than a thousand. Hanging
it off the outermost node rather than the nearest one bounds a cycle through
several functions too. The cost is real and worth stating: `a -> b -> a` reports
`a -> a` and loses `b -> a`.

Runtime capture beats compile-time analysis here on both accuracy and reach. It
sees dynamic dispatch, which static analysis cannot resolve, and it needs no
build-time tooling, so the same technique ports to any language that can push and
pop a stack.

The tree is process-wide, not per-thread. Two threads walking the same chain of
calls share its contexts and their times merge, which is what a flame graph
wants and is also why a context can never answer *which thread*. It follows that
a window can hold more self time than it holds wall clock, and an interface that
divides by the clock will draw a busy program at several hundred percent.

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

One time axis, and two tabs that are both readings of it. The axis is two
strips over the same buckets: self time by module, and live bytes by type.
Clicking a bucket pins it, and the flame graph and the call graph both show
that bucket.

The axis is above the tabs rather than inside one of them, because a scrubber
you cannot reach from the view it drives is not a scrubber.

Bucket size is a client-side fold over the daemon's windows, not a server
setting. Changing it regroups nanoseconds already in the browser: instant, no
refetch, and the same total however it is grouped. The daemon keeps its own two
resolutions, a minute at 100ms and a quarter of an hour at one second, and the
axis picks which to read.

The flame graph puts the root at the top. Depth grows downward, so the row
being read does not move when the tree gets deeper between one bucket and the
next. Width is a share of the bucket's own total, never of wall clock, for the
threading reason above.

Colour goes by module, not by a hash of the function name. A hash gives every
box its own colour and says nothing, because boxes then differ where their
names differ rather than where the work does. Eight hues in a fixed order; a
ninth module is grey rather than a repeat of the first pretending to be
distinct.

The call graph is laid out by a force simulation, inside a centred box with a
capped aspect. A panel spanning a wide window gives its canvas about five to
one, and a force layout in a letterbox is not a graph: the ideal distance comes
out taller than the space and every node clamps against an edge.

Positions persist by function id across buckets, and the simulation stops when
it settles. Scrubbing one bucket forward changes what is on screen and not
where it is; re-running the layout on every update would make the graph twitch
and become unreadable.
