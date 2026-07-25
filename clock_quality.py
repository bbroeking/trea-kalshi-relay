"""Conservative wall/monotonic clock evidence for the standalone relay."""

from __future__ import annotations

import time
import urllib.request
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable


POLYMARKET_TIME_URL = "https://clob.polymarket.com/time"


def utc_from_ns(value: int) -> str:
    return (
        datetime.fromtimestamp(value / 1_000_000_000, timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


@dataclass(frozen=True)
class ClockSample:
    collector_boot_id: str
    reference: str
    request_started_at: str
    response_received_at: str
    request_started_monotonic_ns: int
    response_received_monotonic_ns: int
    offset_ms: float | None
    uncertainty_ms: float | None
    rtt_ms: float | None
    wall_step_ms: float | None
    healthy: bool
    error: str | None
    raw: dict[str, Any]

    @property
    def key(self) -> str:
        return (
            f"{self.collector_boot_id}:"
            f"{self.response_received_monotonic_ns}"
        )

    def payload(self) -> dict[str, Any]:
        return {
            "sampleKey": self.key,
            **asdict(self),
        }


def fetch_polymarket_server_second(
    *,
    timeout: float = 5.0,
    opener: Callable[..., Any] = urllib.request.urlopen,
) -> tuple[float, float, dict[str, Any]]:
    request = urllib.request.Request(
        f"{POLYMARKET_TIME_URL}?nonce={uuid.uuid4().hex}",
        headers={
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "User-Agent": "TREA-relay/1.0 (+public clock-quality probe)",
        },
    )
    with opener(request, timeout=timeout) as response:
        body = response.read().decode().strip()
        headers = dict(response.headers.items())
    return (
        float(int(body)),
        1.0,
        {
            "body": body,
            "date": headers.get("Date"),
            "age": headers.get("Age"),
        },
    )


def sample_clock(
    *,
    collector_boot_id: str,
    probes: int = 3,
    maximum_uncertainty_ms: float = 750.0,
    maximum_wall_step_ms: float = 100.0,
    fetch_server_time: Callable[
        [], tuple[float, float, dict[str, Any]]
    ] = fetch_polymarket_server_second,
    wall_time_ns: Callable[[], int] = time.time_ns,
    monotonic_ns: Callable[[], int] = time.monotonic_ns,
) -> ClockSample:
    if probes <= 0:
        raise ValueError("probes must be positive")
    first_wall: int | None = None
    first_mono: int | None = None
    last_wall: int | None = None
    last_mono: int | None = None
    lower_bounds: list[float] = []
    upper_bounds: list[float] = []
    rtts_ms: list[float] = []
    wall_steps_ms: list[float] = []
    raw_probes: list[dict[str, Any]] = []
    try:
        for _ in range(probes):
            start_wall = wall_time_ns()
            start_mono = monotonic_ns()
            server_lower, resolution_seconds, metadata = fetch_server_time()
            end_mono = monotonic_ns()
            end_wall = wall_time_ns()
            if first_wall is None:
                first_wall = start_wall
                first_mono = start_mono
            last_wall = end_wall
            last_mono = end_mono
            server_lower_ns = server_lower * 1_000_000_000
            server_upper_ns = (
                server_lower + resolution_seconds
            ) * 1_000_000_000
            lower_ms = (server_lower_ns - end_wall) / 1_000_000
            upper_ms = (server_upper_ns - start_wall) / 1_000_000
            rtt_ms = (end_mono - start_mono) / 1_000_000
            wall_step_ms = abs(
                (end_wall - start_wall) - (end_mono - start_mono)
            ) / 1_000_000
            lower_bounds.append(lower_ms)
            upper_bounds.append(upper_ms)
            rtts_ms.append(rtt_ms)
            wall_steps_ms.append(wall_step_ms)
            raw_probes.append(
                {
                    "serverLowerEpoch": server_lower,
                    "resolutionSeconds": resolution_seconds,
                    "offsetLowerMs": lower_ms,
                    "offsetUpperMs": upper_ms,
                    "rttMs": rtt_ms,
                    "wallStepMs": wall_step_ms,
                    "metadata": metadata,
                }
            )
        assert first_wall is not None
        assert first_mono is not None
        assert last_wall is not None
        assert last_mono is not None
        intersection_lower = max(lower_bounds)
        intersection_upper = min(upper_bounds)
        consistent = intersection_lower <= intersection_upper
        if consistent:
            offset_ms = (intersection_lower + intersection_upper) / 2
            uncertainty_ms = (
                intersection_upper - intersection_lower
            ) / 2
        else:
            offset_ms = (min(lower_bounds) + max(upper_bounds)) / 2
            uncertainty_ms = (
                max(upper_bounds) - min(lower_bounds)
            ) / 2
        wall_step_ms = max(wall_steps_ms)
        healthy = (
            consistent
            and uncertainty_ms <= maximum_uncertainty_ms
            and wall_step_ms <= maximum_wall_step_ms
        )
        reasons = []
        if not consistent:
            reasons.append("probe offset intervals do not intersect")
        if uncertainty_ms > maximum_uncertainty_ms:
            reasons.append(
                f"uncertainty {uncertainty_ms:.3f}ms exceeds "
                f"{maximum_uncertainty_ms:.3f}ms"
            )
        if wall_step_ms > maximum_wall_step_ms:
            reasons.append(
                f"wall step {wall_step_ms:.3f}ms exceeds "
                f"{maximum_wall_step_ms:.3f}ms"
            )
        return ClockSample(
            collector_boot_id=collector_boot_id,
            reference="polymarket-public-time",
            request_started_at=utc_from_ns(first_wall),
            response_received_at=utc_from_ns(last_wall),
            request_started_monotonic_ns=first_mono,
            response_received_monotonic_ns=last_mono,
            offset_ms=offset_ms,
            uncertainty_ms=uncertainty_ms,
            rtt_ms=max(rtts_ms),
            wall_step_ms=wall_step_ms,
            healthy=healthy,
            error="; ".join(reasons) or None,
            raw={"probes": raw_probes},
        )
    except Exception as exc:
        current_wall = wall_time_ns()
        current_mono = monotonic_ns()
        return ClockSample(
            collector_boot_id=collector_boot_id,
            reference="polymarket-public-time",
            request_started_at=utc_from_ns(first_wall or current_wall),
            response_received_at=utc_from_ns(current_wall),
            request_started_monotonic_ns=first_mono or current_mono,
            response_received_monotonic_ns=current_mono,
            offset_ms=None,
            uncertainty_ms=None,
            rtt_ms=None,
            wall_step_ms=None,
            healthy=False,
            error=f"{type(exc).__name__}: {exc}",
            raw={"probes": raw_probes},
        )
