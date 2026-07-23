# TREA Kalshi relay

Zero-cost hosted collector for the TREA Market Lab. A scheduled GitHub Actions
runner reads official public MLB, Kalshi, and Polymarket market-data endpoints
every five minutes and publishes a single JSON snapshot:

`https://raw.githubusercontent.com/bbroeking/trea-kalshi-relay/main/data/tonight.json`

The relay has no exchange credentials and cannot place orders. It records
executable top-of-book bids and asks, observation time, volume, the official MLB
slate, and the complement-parity calculation used by the dashboard. Polymarket
metadata comes from Gamma and executable depth comes from the public CLOB.

## Run locally

```bash
python3 collector.py --output data/tonight.json
```

## Operational behavior

- Collection runs every five minutes and can also be manually dispatched.
- HTTP 429 responses honor `Retry-After` and use bounded retries.
- Output is written atomically so readers never receive partial JSON.
- The dashboard treats stale relay data as degraded, never as an executable
  signal.
