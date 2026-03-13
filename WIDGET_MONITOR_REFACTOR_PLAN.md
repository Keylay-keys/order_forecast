# Desktop Widget Monitor Refactor Plan

## Goal
Stop false "API down" notifications in the desktop widget without rewriting the whole widget.

The target is a narrow refactor of the monitoring path in `desktop_widget.py` so that:
- health probes use fresh results
- state transitions are deterministic
- notifications fire only on real state changes
- UI rendering is separated from monitor evaluation

## Current Problem
The widget currently mixes three concerns in one flow:
- probing service health
- updating failure/success counters
- rendering UI / triggering notifications

The concrete bug is in `refresh_status()`:
- a background thread starts `check_server_health()`
- `refresh_status()` immediately evaluates `self.server_health`
- failure/success counters can update from stale data
- after enough cycles, the widget can mark the API down even while `/api/health` is returning `200`

This is a monitor design problem, not an API availability problem.

## Scope
In scope:
- `order_forecast/desktop_widget.py`
- internal monitor state and probe flow
- server API health probe behavior
- notification trigger behavior for service down / recovery

Out of scope:
- full UI redesign
- menubar app redesign
- server API changes
- ntfy channel redesign
- archive sync / purge semantics

## Source Of Truth
Current server health path:
- `GET /api/health`

Current widget settings involved:
- `server_failure_threshold`
- `server_recovery_threshold`
- `server_check_timeout_seconds`
- `notification_cooldown_minutes`
- `notify_recovery`

Current problematic functions:
- `check_server_health()`
- `refresh_status()`
- `_on_service_down()`

## Design Direction
Do not do a total rewrite.

Refactor toward three layers:
1. Probe layer
2. State machine layer
3. UI rendering layer

### 1. Probe Layer
Each probe should return a completed result object, not mutate widget state directly.

Target shape:
```python
{
  "service": "server",
  "ok": True,
  "checkedAt": 1710000000.0,
  "latencyMs": 123,
  "status": "healthy",
  "error": None,
  "details": {...}
}
```

Requirements:
- no state transitions inside the probe
- no notifications inside the probe
- no UI writes inside the probe
- every result must be timestamped

### 2. State Machine Layer
Service state should be explicit and local.

For each monitored service, track:
- `last_result`
- `consecutive_failures`
- `consecutive_successes`
- `status`:
  - `up`
  - `retrying`
  - `down`
- `last_transition_at`
- `last_notified_at`
- `last_error_logged`

Transition rules:
- failed probe increments failures and resets successes
- successful probe increments successes and resets failures
- `up -> retrying` on first failed probe
- `retrying -> up` on first successful probe before failure threshold is hit
- `retrying -> down` only after `server_failure_threshold`
- `down -> up` only after `server_recovery_threshold`
- notifications fire only on transition to `down`
- optional recovery notification fires only on transition to `up`

### 3. UI Rendering Layer
The widget should render from the current monitor state only.

Rules:
- UI rows consume `status`, `info`, `last_result`
- UI should not infer health by itself
- UI should not trigger notifications
- `display_running` should be derived from monitor status, not ad hoc booleans

## Refactor Plan

### Phase 1: Server Probe Isolation
Refactor the server monitor first, because it is the source of the false API-down alerts.

Changes:
- extract a probe function that returns a result object
- stop updating `self.server_health` from a fire-and-forget thread and then immediately reading it
- keep the probe off the Qt main thread
- only process probe results after the background thread completes and returns fresh data
- use a lock-protected pending result or queue so there are no unsynchronized reads/writes

Acceptance:
- no state transitions occur without a fresh probe result
- repeated `200` health checks do not increment failure counters

### Phase 2: Explicit Server State Machine
Introduce a small per-service state model for the server row.

Changes:
- replace ad hoc fields:
  - `server_failures`
  - `server_successes`
  - `server_is_down`
  - `last_server_is_down`
  - `last_server_error`
- with a single `server_monitor` state object

Acceptance:
- down notifications only happen on threshold crossing
- recovery notifications only happen on threshold crossing
- no duplicate notifications inside cooldown window
- `retrying -> up` is deterministic and tested
- repeated identical probe errors are still deduplicated in logs

### Phase 3: Generalize To Other Services
Once the server row is stable, apply the same pattern to other monitored rows if useful.

Candidate rows:
- archive purge
- archive sync
- server-side workers if they gain active probing later

Important:
- do not broaden scope until the server row is fixed and verified
- archive sync / purge are not drop-in copies of the server probe path; they may remain event- or cache-driven

### Phase 4: Cleanup
After the state machine is in place:
- remove obsolete boolean bookkeeping
- remove any notification logic tied to raw probe failure
- reduce duplicate row update logic where safe

## Recommended Implementation Shape
Suggested internal additions in `desktop_widget.py`:

```python
@dataclass
class ProbeResult:
    service: str
    ok: bool
    checked_at: float
    latency_ms: int | None
    status: str
    error: str | None = None
    details: dict | None = None

@dataclass
class MonitorState:
    service: str
    status: str = "up"
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_result: ProbeResult | None = None
    last_transition_at: float | None = None
    last_notified_at: float | None = None
    last_error_logged: str | None = None

@dataclass
class TransitionInfo:
    old_status: str
    new_status: str
    did_transition: bool
    should_notify_down: bool = False
    should_notify_recovery: bool = False
```

Then:
- `probe_server_health(...) -> ProbeResult`
- `apply_probe_result(monitor_state, probe_result, settings) -> TransitionInfo`
- `render_server_row(monitor_state)`

## Notification Rules
Only notify on transitions.

Bad pattern:
- notify because a single probe failed

Good pattern:
- notify because `retrying -> down`

Rules:
- transition to `down`:
  - send "RouteSpark Service Down"
- transition to `up` and `notify_recovery=true`:
  - send "RouteSpark Service Up"
- recovery notifications use the same cooldown gate as down notifications
- no other transitions notify

Startup rule:
- first observation should not send an immediate alert by default
- the monitor starts collecting evidence and only notifies on a later threshold-crossing transition

## Verification Plan
### Local behavior checks
1. API healthy for 10+ refresh cycles
- expected:
  - no down notification
  - no failure counter growth

2. Force temporary failure shorter than threshold
- expected:
  - status becomes `retrying`
  - no down notification

3. Force failure beyond threshold
- expected:
  - exactly one down notification
  - status becomes `down`

4. Restore health
- expected:
  - requires configured success threshold
  - optional recovery notification only once

### Logging expectations
Add clear log lines for:
- probe result
- transition
- notification sent
- notification suppressed due to cooldown

Example:
- `server probe ok latency=120ms`
- `server transition up -> retrying`
- `server transition retrying -> down after 3 failures`
- `server recovery after 2 successes`

## Definition Of Done
Done means:
- widget no longer emits false API-down notifications while `/api/health` is healthy
- server state changes are driven only by fresh probe results
- notifications happen only on transitions
- recovery behavior is deterministic
- `desktop_widget.py` is simpler to reason about than the current race-prone flow

## Test Requirements
At minimum, cover the pure state machine with focused tests for:
- threshold crossing `retrying -> down`
- threshold crossing `down -> up`
- `retrying -> up` on early recovery
- cooldown suppression for repeated down notifications
- cooldown suppression for repeated recovery notifications

## Follow-On Work
After the server row is fixed, consider:
1. moving probe/state logic into a dedicated helper module
2. unifying row rendering from a shared monitor state contract
3. exposing a small debug section in the widget showing:
   - last probe time
   - last error
   - consecutive failures

That follow-on work is optional. The first objective is to stop false alerts.
