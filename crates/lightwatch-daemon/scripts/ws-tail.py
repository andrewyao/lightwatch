#!/usr/bin/env python3
"""Prints what /api/processes/<id>/stream pushes, until a deadline.

A hand-rolled RFC 6455 client so the fixture run needs nothing installed.
Server-to-client frames are never masked, which is why unmasking is missing.
"""
import base64
import json
import os
import socket
import struct
import sys
import time

host, port, path, seconds = sys.argv[1], int(sys.argv[2]), sys.argv[3], float(sys.argv[4])

sock = socket.create_connection((host, port), timeout=seconds)
key = base64.b64encode(os.urandom(16)).decode()
sock.sendall(
    (
        "GET %s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n" % (path, host, port, key)
    ).encode()
)

buf = b""
while b"\r\n\r\n" not in buf:
    chunk = sock.recv(4096)
    if not chunk:
        sys.exit("the daemon closed before the handshake finished")
    buf += chunk
head, buf = buf.split(b"\r\n\r\n", 1)
print("handshake:", head.split(b"\r\n")[0].decode(), flush=True)

sock.settimeout(0.25)
deadline = time.time() + seconds


def need(count):
    global buf
    while len(buf) < count:
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            return False
        if not chunk:
            return False
        buf += chunk
    return True


while time.time() < deadline:
    if not need(2):
        continue
    opcode = buf[0] & 0x0F
    length = buf[1] & 0x7F
    offset = 2
    if length == 126:
        if not need(4):
            continue
        length = struct.unpack(">H", buf[2:4])[0]
        offset = 4
    elif length == 127:
        if not need(10):
            continue
        length = struct.unpack(">Q", buf[2:10])[0]
        offset = 10
    if not need(offset + length):
        continue
    payload, buf = buf[offset : offset + length], buf[offset + length :]
    if opcode == 1:
        message = json.loads(payload)
        if message["type"] == "window":
            window = message["window"]
            print(
                "  push window=%d calls=%s edges=%s census=%s"
                % (window["index"], window["calls"], window["edges"], window["census"]),
                flush=True,
            )
        else:
            print("  push %s" % json.dumps(message), flush=True)
    elif opcode == 8:
        print("  server closed the socket", flush=True)
        break
