# Low-Quantity Notification Daemon Plan

## Goal

Send push notifications for low-quantity items that need to be ordered today.
Runs locally on Mac (always on), uses existing Python infrastructure.

## Why Server-Side?

- **Client-side scheduling fails** if app isn't opened before reminder time
- **Low-qty is dynamic** - changes with PCF uploads, expiry dates, visibility flags
- **Unlike fixed tasks**, low-qty requires live data recalculation
- **Server-side ensures reliability** even if user doesn't open app

---

## Architecture

**Snapshot Listener + Timer** (implemented)

Simpler than event-driven queueing. Always uses latest PCF data.

```
low_qty_notification_daemon.py

STARTUP:
├── Snapshot listener on users collection
│   └── Populates in-memory cache: { user_id: { route, reminder_time, timezone } }
│   └── Only includes users with orderReminders.enabled == true
│   └── Auto-updates cache when user settings change

RUNNING (every 60 seconds):
├── For each user in cache:
│   ├── Check if current time matches reminder_time (±2 min) in their timezone
│   ├── If matches:
│   │   ├── Compute low-qty items fresh from Firebase PCF data
│   │   ├── If items exist:
│   │   │   ├── Check dedup in DuckDB (skip if same items already sent today)
│   │   │   ├── Get FCM tokens (re-read for latest)
│   │   │   ├── Send via Expo Push API
│   │   │   └── Mark as sent in DuckDB
```

**Firebase Reads:**
- Startup: 1 snapshot listener (auto-updates)
- Per reminder check: 0 (uses in-memory cache)
- Per notification sent: PCF data + 1 read for FCM tokens

**Cost Impact:**
- Old polling approach: ~864,000 reads/month
- Snapshot + timer: ~100 initial + PCF reads only when sending
- Example: 1 route, 2 notifications/week = ~50 reads/month

---

## Data Sources

### User Reminder Settings
- Path: `users/{uid}/userSettings/notifications/orderReminders`
- Fields:
  - `enabled` (boolean)
  - `time` (object): `{ hour: number, minute: number, period: "AM" | "PM" }`
- Source: `firestore.ts` (line 137), `DashboardContext.tsx` (line 166)

### User Timezone
- Path: `users/{uid}/profile/timezone`
- Example: `"America/Denver"`

### Push Tokens (FCM via Expo)
- Path: `users/{uid}/fcmTokens` (array of Expo push tokens)
- Source: `NotificationService.ts` (line 162)

### Route → User Mapping
- Path: `routes/{routeNumber}` → `userId` field
- Notify route owner only (not team members, unless explicitly added later)

### Low-Qty Items
- Computed via `low_quantity_loader.get_items_for_order_date()`
- Uses PCF data from `routes/{route}/pcfs/{delivery}/containers/{container}`

---

## Deduplication

Prevent sending the same notification multiple times:

```python
# Hash of (route, date, sorted_saps)
dedup_key = f"{route_number}:{order_date}:{hash(tuple(sorted(saps)))}"

# Store in DuckDB or local file
# Check before sending, mark after sending
```

Tables (DuckDB):

```sql
-- Pending notifications (queued on delivery scan)
-- Settings/tokens NOT cached here - re-read at send time
CREATE TABLE IF NOT EXISTS pending_low_qty_notifications (
    id INTEGER PRIMARY KEY,
    route_number TEXT NOT NULL,
    user_id TEXT NOT NULL,
    saps TEXT NOT NULL,           -- JSON array (for dedup + payload)
    saps_hash TEXT NOT NULL,      -- MD5 hash for dedup comparison
    order_by_date TEXT NOT NULL,  -- YYYY-MM-DD
    timezone TEXT NOT NULL,
    queued_at TIMESTAMP NOT NULL,
    sent_at TIMESTAMP,            -- NULL until sent
    UNIQUE(route_number, order_by_date, saps_hash)  -- dedup: same route+date+items
);
```

**Dedup logic:**
- `UNIQUE(route_number, order_by_date, saps_hash)` prevents duplicate notifications
- If a second delivery on the same day changes items, a new row is inserted (different hash)
- `INSERT OR IGNORE` used to skip if exact same notification already queued
- Query only WHERE `sent_at IS NULL` to find pending notifications

---

## Notification Payload

```python
# Sent to each token in users/{uid}/fcmTokens array
# IMPORTANT: type must be "low_quantity" to match app handler
{
    "to": fcm_token,  # ExponentPushToken[...]
    "title": "Low Stock Alert",
    "body": f"{len(items)} item(s) need to be ordered today",
    "data": {
        "type": "low_quantity",  # Matches lowQuantityNotificationManager.ts:99
        "routeNumber": route_number,
        "orderDate": order_date,
        "saps": [item.sap for item in items],
    },
    "sound": "default",
    "priority": "high",
}
```

### Required App Change: Add handler in NotificationsWrapper.tsx

```typescript
// Add after the 'task' handler (around line 70):
} else if (data.type === 'low_quantity') {
  // Navigate to low-qty modal
  router.push({
    pathname: '/(app)/dashboard/(modals)/low-quantity',
    params: { 
      jumpToItem: data.saps?.[0],  // Optional: first item
      source: 'notification'
    }
  });
}
```

---

## Implementation Plan

### Part 1: Add to `delivery_manifest_listener.py` (on delivery scan)

```python
def queue_low_qty_notification(db, db_client, route_number: str, user_id: str):
    """Called after delivery is processed. Queues notification for later.
    
    1. Compute low-qty items using low_quantity_loader
    2. Get user's reminder settings + timezone + fcm_tokens
    3. Insert into DuckDB pending_low_qty_notifications table
    
    Note: We only store route_number + order_date here. Settings/tokens are
    re-read at send time to respect any changes made after queueing.
    """
    from low_quantity_loader import get_items_for_order_date, get_user_timezone
    from datetime import datetime
    import pytz
    
    # get_user_timezone takes (db, route_number) - see low_quantity_loader.py:40
    timezone = get_user_timezone(db, route_number)
    if not timezone:
        timezone = "America/Denver"  # fallback
    
    # Get today in user's timezone
    tz = pytz.timezone(timezone)
    today = datetime.now(tz).strftime("%Y-%m-%d")
    
    # get_items_for_order_date(db, route_number, order_date, db_client=None)
    # See low_quantity_loader.py:512
    items = get_items_for_order_date(db, route_number, today, db_client)
    if not items:
        return  # Nothing to notify
    
    # Queue in DuckDB (settings/tokens fetched at send time)
    import hashlib
    saps_list = sorted([i.sap for i in items])
    saps_json = json.dumps(saps_list)
    saps_hash = hashlib.md5(saps_json.encode()).hexdigest()
    
    # INSERT OR IGNORE: if same route+date+items already queued, skip
    db_client.query("""
        INSERT OR IGNORE INTO pending_low_qty_notifications 
        (route_number, user_id, saps, saps_hash, order_by_date, timezone, queued_at, sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
    """, [
        route_number, user_id, saps_json, saps_hash, today,
        timezone, datetime.utcnow().isoformat()
    ])
```

---

### Part 2: `low_qty_notification_daemon.py` (lightweight timer)

```python
"""Sends queued low-qty notifications at user's reminder time.

Runs every 60 seconds. Re-reads user settings at send time to respect changes.

Usage:
    python scripts/low_qty_notification_daemon.py --serviceAccount /path/to/sa.json
"""

def get_pending_notifications(db_client) -> List[Dict]:
    """Get unsent notifications from DuckDB."""
    return db_client.query("""
        SELECT route_number, user_id, saps, order_by_date, timezone, queued_at
        FROM pending_low_qty_notifications
        WHERE sent_at IS NULL
    """)

def is_reminder_time_now(db, user_id: str, timezone: str) -> bool:
    """Check if current time matches user's reminder time.
    
    Re-reads settings from Firebase to respect any changes.
    Returns False if reminders disabled.
    """
    user_doc = db.collection("users").document(user_id).get()
    if not user_doc.exists:
        return False
    
    settings = user_doc.to_dict().get("userSettings", {}).get("notifications", {})
    order_reminders = settings.get("orderReminders", {})
    
    if not order_reminders.get("enabled"):
        return False
    
    reminder_time = order_reminders.get("time", {})
    hour = reminder_time.get("hour", 8)
    minute = reminder_time.get("minute", 0)
    period = reminder_time.get("period", "AM")
    
    # Convert to 24h
    if period == "PM" and hour != 12:
        hour += 12
    elif period == "AM" and hour == 12:
        hour = 0
    
    # Check if now is within ±2 min of reminder time
    import pytz
    from datetime import datetime
    tz = pytz.timezone(timezone)
    now = datetime.now(tz)
    
    target_minutes = hour * 60 + minute
    current_minutes = now.hour * 60 + now.minute
    
    return abs(current_minutes - target_minutes) <= 2

def get_fcm_tokens(db, user_id: str) -> List[str]:
    """Get current FCM tokens from Firebase."""
    user_doc = db.collection("users").document(user_id).get()
    if not user_doc.exists:
        return []
    return user_doc.to_dict().get("fcmTokens", [])

def send_and_mark_sent(db, db_client, notification: Dict):
    """Send via Expo Push API, then mark as sent in DuckDB."""
    tokens = get_fcm_tokens(db, notification["user_id"])
    if not tokens:
        return
    
    saps = json.loads(notification["saps"])
    send_push_notification(
        fcm_tokens=tokens,
        title="Low Stock Alert",
        body=f"{len(saps)} item(s) need to be ordered today",
        data={
            "type": "low_quantity",
            "routeNumber": notification["route_number"],
            "orderDate": notification["order_by_date"],
            "saps": saps,
        }
    )
    
    db_client.query("""
        UPDATE pending_low_qty_notifications
        SET sent_at = ?
        WHERE route_number = ? AND order_by_date = ?
    """, [datetime.utcnow().isoformat(), 
          notification["route_number"], notification["order_by_date"]])

def main():
    db = init_firestore(sa_path)
    db_client = DBClient()
    
    while True:
        pending = get_pending_notifications(db_client)
        for notif in pending:
            if is_reminder_time_now(db, notif["user_id"], notif["timezone"]):
                send_and_mark_sent(db, db_client, notif)
        time.sleep(60)
```

**Firebase reads at send time:**
- 1 read for settings (check enabled + get reminder time)
- 1 read for FCM tokens
- Total: 2 reads per notification sent (not per check cycle)

---

## Expo Push API

Endpoint: `https://exp.host/--/api/v2/push/send`

```python
import requests

def send_push_notification(fcm_tokens: List[str], title: str, body: str, data: Dict):
    """Send to all user's FCM tokens (stored as Expo push tokens)."""
    messages = []
    for token in fcm_tokens:
        messages.append({
            "to": token,
            "title": title,
            "body": body,
            "data": data,
            "sound": "default",
            "priority": "high",
        })
    
    response = requests.post(
        "https://exp.host/--/api/v2/push/send",
        json=messages,
        headers={"Content-Type": "application/json"},
    )
    return response.json()
```

---

## Integration with Supervisor

Add to `supervisor.py`:
```python
{
    "name": "low_qty_notification_daemon",
    "script": "scripts/low_qty_notification_daemon.py",
    "args": ["--serviceAccount", SA_PATH],
    "enabled": True,
}
```

Also hook into `delivery_manifest_listener.py` to call `queue_low_qty_notification()` after processing.

---

## Edge Cases

1. **User has multiple devices** - send to all tokens in `fcmTokens` array (re-read at send time)
2. **Timezone stored at queue time** - use for time comparison; user profile change takes effect next delivery
3. **No low-qty items** - skip queueing, no notification
4. **Push token expired** - Expo returns error, log and continue (don't retry)
5. **Network failure** - retry with exponential backoff
6. **User disables reminders mid-day** - `is_reminder_time_now()` re-reads settings, returns False
7. **Time format conversion** - handle AM/PM period correctly (8 PM → 20:00)
8. **Second delivery same day with different items** - new row inserted (different saps_hash)
9. **Second delivery same day with same items** - `INSERT OR IGNORE` skips duplicate

---

## Success Criteria

- [ ] Notifications sent at correct user time (timezone-aware)
- [ ] AM/PM time conversion works correctly
- [ ] Dedup prevents duplicate notifications for same route+date+items
- [ ] Second delivery with changed items triggers new notification
- [ ] Correct item count in notification body
- [ ] Deep link data allows app to navigate to low-qty view
- [ ] Settings re-read at send time (respects mid-day changes)
- [ ] Logs show success/failure for each route

---

## GPT Audit Findings (Resolved)

### Round 1
| Issue | Severity | Resolution |
|-------|----------|------------|
| Reminder settings path wrong | High | Fixed: `orderReminders` with `{hour, minute, period}` object |
| Push tokens path wrong | High | Fixed: `fcmTokens` array |
| `reminder_days` not in schema | Medium | Removed: send daily if items exist |
| Dedup single-instance only | Medium | Acceptable: Mac always on, single daemon |

### Round 2
| Issue | Severity | Resolution |
|-------|----------|------------|
| Wrong function signatures | High | Fixed: `get_user_timezone(db, route_number)` and `get_items_for_order_date(db, route_number, order_date, db_client)` |
| Push type mismatch | High | Fixed: use `type: "low_quantity"` + add handler in NotificationsWrapper |
| Cached settings ignored changes | Medium | Fixed: re-read settings/tokens at send time |
| Script naming mismatch | Medium | Fixed: consistent `low_qty_notification_daemon.py` |
| Read count understated | Low | Clarified: 2 reads per notification sent (settings + tokens) |
| Dedup with saps hash | Low | Fixed: `UNIQUE(route_number, user_id, order_by_date, saps_hash)` |

### Round 3
| Issue | Severity | Resolution |
|-------|----------|------------|
| Watcher not stored (GC risk) | High | Fixed: store `_users_watcher` globally |
| Route selection (currentRoute) | High | Fixed: use `currentRoute` with fallback to `routeNumber` |
| Dedup conflict (multi-user) | Medium | Fixed: include `user_id` in dedup + only notify owner |
| Timezone mismatch | Medium | Fixed: only notify route owners (same tz as loader) |
| Token filter too strict | Low | Fixed: accept both `ExponentPushToken` and `ExpoPushToken` |
| query vs write | Medium | Fixed: use `db_client.write()` for INSERT |
| Owner check fail-open | Low | Fixed: fail closed (skip if owner unknown) |
| Expo batch limit | Low | Fixed: chunk to 100 messages per request |

### Round 4 (Accepted as-is)
| Issue | Severity | Decision |
|-------|----------|----------|
| Partial send marks as sent | Low | **Accepted**: "at least one device" is sufficient |
| Snapshot watches all users | Low | **Accepted**: fine for current scale, add filter later if needed |
| additionalRoutes ignored | Low | **Accepted**: currentRoute only for now, multi-route is future enhancement |
| ±2 min window (no backfill) | Low | **Accepted**: rare edge case, daemon restarts quickly |

## Notification Content Decision

- **Body**: Item count only (e.g., "3 items need to be ordered today")
- **Data payload**: Include SAPs for deep linking, app can fetch names if needed

---

## Task Assignments

### Claude (Cursor) Tasks

| # | Task | Status | Notes |
|---|------|--------|-------|
| C1 | Create `scripts/low_qty_notification_daemon.py` | ✅ Done | Snapshot listener + 60s timer |
| C2 | Add queue hook to delivery_manifest_listener.py | ❌ Cancelled | Not needed with snapshot approach |
| C3 | Add DuckDB table schema to `db_schema.py` | ✅ Done | `low_qty_notifications_sent` table |
| C4 | Add `low_quantity` handler to `NotificationsWrapper.tsx` | ✅ Done | Deep links to low-qty modal |
| C5 | Add daemon to `supervisor.py` | ✅ Done | Added to service list + status/stop |
| C6 | Test end-to-end locally | Pending | Awaiting user test |

### GPT Tasks

| # | Task | Status | Notes |
|---|------|--------|-------|
| G1 | Review `low_qty_notification_daemon.py` after implementation | Pending | Check for edge cases, error handling |
| G2 | Audit Expo Push API integration | Pending | Verify payload format, error responses |
| G3 | Review dedup logic | Pending | Confirm saps_hash approach is correct |
| G4 | Validate NotificationsWrapper handler | Pending | Check deep link path exists |
| G5 | Security review | Pending | Verify no sensitive data in push payload |
| G6 | Review timezone handling | Pending | Edge cases around DST, midnight crossings |

### Execution Order

```
1. Claude: C3 (schema) → C1 (daemon) → C2 (hook) → C5 (supervisor)
2. GPT: G1 + G2 + G3 (review implementation)
3. Claude: Fix any issues from GPT review
4. Claude: C4 (app handler)
5. GPT: G4 + G5 (app review)
6. Claude: C6 (end-to-end test)
7. GPT: G6 (final timezone audit)
```
