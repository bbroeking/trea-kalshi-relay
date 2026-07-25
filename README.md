# TREA Kalshi relay

Zero-cost hosted collector for the TREA Market Lab. The container continuously
records public market WebSockets and reconciles them against official REST
snapshots; a scheduled GitHub Actions snapshot remains the fallback:

`https://raw.githubusercontent.com/bbroeking/trea-kalshi-relay/main/data/tonight.json`

The relay cannot place orders. It records executable top-of-book bids and asks,
observation time, volume, the official MLB slate, and the complement-parity
calculation used by the dashboard. Polymarket metadata comes from Gamma and
executable depth comes from the public CLOB. Kalshi REST reconciliation is
public; continuous Kalshi L2 uses a read-capable API key because Kalshi requires
authentication for its order-book WebSocket.

## Run the snapshot collector locally

```bash
python3 collector.py --output data/tonight.json
```

## Run the continuous relay

```bash
python3 service.py --port 8080 --refresh-seconds 30
curl http://localhost:8080/healthz
curl http://localhost:8080/data/tonight.json
```

The service refreshes in one background thread, preserves the last successful
snapshot during transient upstream errors, and reports readiness only while the
last REST success and required WebSocket messages are within
`MAXIMUM_AGE_SECONDS` (120 seconds by default). It uses
the Python standard library, `websockets`, `cryptography`, and public market-data
endpoints only. Hosted
containers maintain dynamic subscriptions to the public Polymarket market
WebSocket and identify themselves as `continuous-websocket` in source health;
scheduled snapshots retain the `github-actions` label.

Polymarket is receipt-ordered, not provider-sequenced. Every
`POLYMARKET_RECONCILE_SECONDS` (60 by default), the relay journals the official
batch REST books. When REST is ahead, it waits up to three seconds for the
WebSocket to reach the same documented book hash. Passing that source
timestamp without matching, timing out, or omitting a subscribed token closes
the session and reconnects from fresh full books. `best_bid_ask` remains in the
raw archive but cannot mint a fresh executable state or borrow stale sizes.
An image-level acceptance using the production Docker volume layout processed
2,833 WebSocket messages, matched all four periodic REST hashes, archived
2,888 records with zero drops, and had zero reconciliation failures or
reconnects.

Every successful REST snapshot and every received Polymarket/Kalshi order-book
event is also written to an append-only SQLite journal. Socket readers enqueue
records into a bounded non-blocking queue; any dropped record makes readiness
red. Consumers can checkpoint and resume incremental downloads with:

```bash
curl 'http://localhost:8080/archive/events?after_id=0&limit=1000'
```

The response includes `nextAfterId` and the database-wide `maximumId`.
Containers default to `/data/relay.sqlite` with archival required. The hosted
service must mount a persistent volume at `/data`; an ephemeral container
filesystem is not sufficient evidence for the shadow program.

For continuous Kalshi depth, configure:

```bash
export KALSHI_API_KEY_ID="..."
export KALSHI_PRIVATE_KEY_B64="$(base64 < private-key.pem | tr -d '\n')"
export REQUIRE_KALSHI_WEBSOCKET=1
python3 service.py --port 8080 --refresh-seconds 30
```

`KALSHI_PRIVATE_KEY_PATH` may be used instead of the base64 secret for local
runs. The relay signs only the WebSocket handshake, explicitly requests
`use_yes_price=true`, subscribes to public trades alongside L2, requires an
initial snapshot, and reconnects on any sequence gap. Every trade is retained
in the append-only archive. `/healthz` exposes Kalshi configuration,
connection state, message count, reconnects, and sequence gaps separately from
Polymarket. With
`REQUIRE_KALSHI_WEBSOCKET=1`, readiness stays red until a fresh complete Kalshi
book has been reconstructed.

Polymarket game slugs are treated as the authoritative local game date for
discovery. This retains evening games whose UTC `gameStartTime` falls on the
following date.

Build the included container with:

```bash
docker build -t trea-relay .
docker run --rm -p 8080:8080 trea-relay
```

The image pins its Python base-image digest and exact cryptography/WebSocket
versions, copies every runtime module, and requires both the append-only archive
and clock quality by default. A container that cannot write `/data` or obtain a
healthy bounded-uncertainty clock returns HTTP 503.

`railway.json` selects the Dockerfile, waits for `/healthz` to return 200, and
restarts the service after a crash. After authenticating the Railway CLI:

```bash
railway login
railway link
railway up
```

Create and mount a Railway volume at `/data` before deployment. Confirm
`serviceHealth.archive.healthy=true`, `dropped=0`, and that `maximumId`
continues increasing across a deliberate service restart. The mount must be
writable by container UID 10001; an unwritable volume makes `/healthz` return
503 rather than silently dropping the archive.

The 2026-07-25 production-layout acceptance built image
`sha256:b0912d0773efb7aa5bc0ba88736ef93b83e40126dc3b1cf6c3ec28f1b84b8bde`,
subscribed all 30 active Polymarket outcome tokens, required clock quality,
recorded zero archive drops, and reused the same named `/data` volume across a
container replacement. The archive maximum ID advanced from 87 to 2,415 after
replacement, proving that the journal survived process and container identity.

## Operational behavior

- Collection runs every five minutes and can also be manually dispatched.
- The container service refreshes every 30 seconds by default; the GitHub
  Actions snapshot remains a low-frequency fallback.
- Polymarket WebSocket subscriptions reconnect after errors, when REST
  discovery changes the active token set, or when hash reconciliation fails.
- Kalshi WebSocket subscriptions reconnect after errors, market-set changes, or
  any missing/out-of-order sequence and rebuild from a fresh snapshot.
- HTTP 429 responses honor `Retry-After` and use bounded retries.
- Output is written atomically so readers never receive partial JSON.
- The dashboard treats stale relay data as degraded, never as an executable
  signal.
- `/healthz` returns HTTP 503 before the first successful refresh or whenever
  the last successful refresh is stale.

The container acceptance test on 2026-07-24 discovered all 15 MLB moneylines,
subscribed 30 outcome tokens, applied 339 public WebSocket messages without a
reconnect, and returned HTTP 200 with both REST and WebSocket health green.
