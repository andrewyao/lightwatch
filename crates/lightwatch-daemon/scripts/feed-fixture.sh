#!/usr/bin/env bash
# Feeds a fixture stream into a running daemon over its unix socket and prints
# what the HTTP API says about it. The fixture is deliberately hostile: a line
# that is not JSON, an event naming a function nobody registered, a context
# hanging off a parent nobody registered, a stack naming a context that does
# not exist, a census that rises and falls, and a second connection speaking
# the wrong schema version. None of it may close the connection; all of it has
# to show up in the counts.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
port="${LIGHTWATCH_PORT:-7700}"
sock_dir="${LIGHTWATCH_SOCK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/lightwatch-fixture.XXXXXX")}"
sock="$sock_dir/lightwatch.sock"
export LIGHTWATCH_SOCK_DIR="$sock_dir"
export LIGHTWATCH_PORT="$port"

cargo build --manifest-path "$root/Cargo.toml" -p lightwatch-daemon >&2

daemon_log="$sock_dir/daemon.log"
"$root/target/debug/lightwatch" >"$daemon_log" 2>&1 &
daemon=$!
trap 'kill "$daemon" 2>/dev/null || true; wait "$daemon" 2>/dev/null || true' EXIT

for _ in $(seq 1 100); do
  if [ -S "$sock" ] && curl -fsS "http://127.0.0.1:$port/api/processes" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

fixture="$sock_dir/stream.jsonl"
cat >"$fixture" <<'STREAM'
{"msg":"hello","schema":2,"app":"lightphotos","pid":3218,"started_unix_ms":1700000000000,"window_ms":100,"source":"lightwatch-probe"}
{"msg":"frame","seq":0,"t_ns":0,"registers":[{"kind":"function","id":1,"name":"thumbnail::get_or_make","module":"lightphotos","location":{"file":"src/thumbnail.rs","line":407,"column":12}},{"kind":"function","id":2,"name":"decode::jpeg","module":"lightphotos"},{"kind":"type","id":1,"name":"Thumbnail"},{"kind":"path","id":1,"parent":0,"func":1},{"kind":"path","id":2,"parent":1,"func":2},{"kind":"path","id":3,"parent":0,"func":2},{"kind":"path","id":4,"parent":3,"func":1}]}
{"msg":"frame","seq":1,"t_ns":100000000,"events":[{"kind":"calls","func":1,"count":16,"ns":{"enc":"raw","v":[2110000,5750000]}},{"kind":"stack","path":2,"count":16,"self_ns":0},{"kind":"census","ty":1,"live":3,"bytes":900,"sizes":{"enc":"raw","v":[300,300,300]}}]}
this line is not json and must not kill the connection
{"msg":"frame","seq":2,"t_ns":200000000,"events":[{"kind":"calls","func":1,"count":24,"ns":{"enc":"raw","v":[1900000,2400000]}},{"kind":"stack","path":2,"count":24,"self_ns":0},{"kind":"census","ty":1,"live":9,"bytes":2700,"sizes":{"enc":"raw","v":[300,300,300,300,300,300,300,300,300]}}]}
{"msg":"frame","seq":3,"t_ns":300000000,"registers":[{"kind":"path","id":9,"parent":7,"func":1},{"kind":"path","id":10,"parent":0,"func":98}],"events":[{"kind":"calls","func":99,"count":1000,"ns":{"enc":"raw","v":[1]}},{"kind":"stack","path":97,"count":5,"self_ns":1000},{"kind":"census","ty":1,"live":4,"bytes":1200,"sizes":{"enc":"raw","v":[300,300,300,300]}}]}
{"msg":"frame","seq":7,"t_ns":400000000,"events":[{"kind":"calls","func":2,"count":8,"ns":{"enc":"raw","v":[9100000]}},{"kind":"stack","path":4,"count":2,"self_ns":0},{"kind":"census","ty":1,"live":5,"bytes":1500,"sizes":{"enc":"raw","v":[300,300,300,300,300]}}]}
STREAM

echo "== feeding $(wc -l <"$fixture" | tr -d ' ') lines into $sock"
nc -U "$sock" <"$fixture" &
feeder=$!
sleep 0.5
kill "$feeder" 2>/dev/null || true
wait "$feeder" 2>/dev/null || true

echo '{"msg":"hello","schema":99,"app":"from-the-future","pid":1,"started_unix_ms":1,"window_ms":100,"source":"bad-client"}' \
  | nc -U "$sock" >/dev/null 2>&1 &
wrong_schema=$!
sleep 0.3
kill "$wrong_schema" 2>/dev/null || true
wait "$wrong_schema" 2>/dev/null || true
sleep 0.2

echo
echo "== GET /api/processes"
curl -fsS "http://127.0.0.1:$port/api/processes" | python3 -m json.tool

id=$(curl -fsS "http://127.0.0.1:$port/api/processes" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["processes"][0]["id"])')

echo
echo "== GET /api/processes/$id/snapshot"
curl -fsS "http://127.0.0.1:$port/api/processes/$id/snapshot" | python3 -m json.tool

echo
echo "== GET /api/processes/$id/stream (websocket, one message per closed window)"
python3 "$(dirname "${BASH_SOURCE[0]}")/ws-tail.py" 127.0.0.1 "$port" "/api/processes/$id/stream" 3 &
tail_pid=$!
sleep 0.5

more="$sock_dir/more.jsonl"
cat >"$more" <<'STREAM'
{"msg":"hello","schema":2,"app":"lightphotos","pid":3218,"started_unix_ms":1700000000000,"window_ms":100,"source":"lightwatch-probe"}
{"msg":"frame","seq":8,"t_ns":500000000,"events":[{"kind":"calls","func":1,"count":5,"ns":{"enc":"raw","v":[3000000]}},{"kind":"census","ty":1,"live":6,"bytes":1800}]}
{"msg":"frame","seq":9,"t_ns":600000000,"events":[{"kind":"calls","func":1,"count":7,"ns":{"enc":"raw","v":[4000000]}},{"kind":"census","ty":1,"live":2,"bytes":600}]}
{"msg":"frame","seq":10,"t_ns":700000000,"events":[{"kind":"calls","func":2,"count":1,"ns":{"enc":"raw","v":[500000]}}]}
STREAM
nc -U "$sock" <"$more" &
feeder=$!
sleep 1
kill "$feeder" 2>/dev/null || true
wait "$feeder" 2>/dev/null || true
wait "$tail_pid" 2>/dev/null || true

echo
echo "== GET / (no web bundle embedded)"
curl -fsS "http://127.0.0.1:$port/"

echo
echo "== GET /api/processes/does-not-exist/snapshot"
curl -s -o /dev/null -w 'HTTP %{http_code}\n' "http://127.0.0.1:$port/api/processes/does-not-exist/snapshot"

echo
echo "== daemon log"
cat "$daemon_log"
