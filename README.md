# TREA Kalshi relay

Zero-cost hosted collector for the TREA Market Lab. A scheduled GitHub Actions
runner reads official public MLB, Kalshi, and Polymarket market-data endpoints
every five minutes and publishes a single JSON snapshot:

`https://raw.githubusercontent.com/bbroeking/trea-kalshi-relay/main/data/tonight.json`

The relay has no exchange credentials and cannot place orders. It records
executable top-of-book bids and asks, observation time, volume, the official MLB
slate, and the complement-parity calculation used by the dashboard. Polymarket
metadata comes from Gamma and executable depth comes from the public CLOB.

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
last success is within `MAXIMUM_AGE_SECONDS` (120 seconds by default). It uses
only the Python standard library and public market-data endpoints. Hosted
containers identify themselves as `continuous-http` in source health; scheduled
snapshots retain the `github-actions` label.

Build the included container with:

```bash
docker build -t trea-relay .
docker run --rm -p 8080:8080 trea-relay
```

`railway.json` selects the Dockerfile, waits for `/healthz` to return 200, and
restarts the service after a crash. After authenticating the Railway CLI:

```bash
railway login
railway link
railway up
```

## Operational behavior

- Collection runs every five minutes and can also be manually dispatched.
- The container service refreshes every 30 seconds by default; the GitHub
  Actions snapshot remains a low-frequency fallback.
- HTTP 429 responses honor `Retry-After` and use bounded retries.
- Output is written atomically so readers never receive partial JSON.
- The dashboard treats stale relay data as degraded, never as an executable
  signal.
- `/healthz` returns HTTP 503 before the first successful refresh or whenever
  the last successful refresh is stale.
