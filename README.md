# lightwatch

A live view of what a running program is doing: which functions are being
called right now, how they call each other, and which objects are alive.

Attaches to any process that emits the lightwatch protocol. A process opens a
socket, sends one `hello`, then one JSON frame per window. That is the entire
contract, so a client in any language is a few lines rather than an SDK.

`crates/lightwatch-proto` is the protocol. Everything else is a producer or a
consumer of it.
