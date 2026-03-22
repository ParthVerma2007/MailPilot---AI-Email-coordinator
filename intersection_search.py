"""
intersection_search.py
Finds the first overlapping free slot for multiple participants using
Google Calendar freeBusy API. All times are normalized to UTC (ISO 8601).
"""

from datetime import datetime, timedelta, timezone, time as dt_time
import pytz
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from typing import Optional

SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]

# Working hours in LOCAL time — converted to UTC before querying
WORK_START = dt_time(9, 0)   # 09:00 local
WORK_END   = dt_time(17, 0)  # 17:00 local

def get_calendar_service(token_path: str = "token.json"):
    """Build authenticated Google Calendar service."""
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    return build("calendar", "v3", credentials=creds)


def normalize_to_utc(dt: datetime, local_tz_str: str) -> datetime:
    """
    Convert a naive or local datetime to UTC.
    All times stored and compared in UTC (ISO 8601).
    """
    local_tz = pytz.timezone(local_tz_str)
    if dt.tzinfo is None:
        dt = local_tz.localize(dt)
    return dt.astimezone(pytz.utc)


def get_work_window_utc(date: datetime.date, local_tz_str: str) -> tuple[datetime, datetime]:
    """
    Return today's 9am–5pm window in UTC for the given timezone.
    Example: 9am IST = 3:30am UTC
    """
    local_tz = pytz.timezone(local_tz_str)
    start_local = local_tz.localize(datetime.combine(date, WORK_START))
    end_local   = local_tz.localize(datetime.combine(date, WORK_END))
    return start_local.astimezone(pytz.utc), end_local.astimezone(pytz.utc)


def fetch_busy_slots(
    service,
    participants: list[str],
    window_start: datetime,
    window_end: datetime
) -> dict[str, list[dict]]:
    """
    Query Google Calendar freeBusy API for all participants.
    Returns a dict: { email -> [{"start": datetime, "end": datetime}, ...] }
    """
    body = {
        "timeMin": window_start.isoformat(),
        "timeMax": window_end.isoformat(),
        "timeZone": "UTC",
        "items": [{"id": email} for email in participants]
    }

    result = service.freebusy().query(body=body).execute()
    calendars = result.get("calendars", {})

    busy_map = {}
    for email in participants:
        busy_periods = calendars.get(email, {}).get("busy", [])
        busy_map[email] = [
            {
                "start": datetime.fromisoformat(p["start"].replace("Z", "+00:00")),
                "end":   datetime.fromisoformat(p["end"].replace("Z", "+00:00"))
            }
            for p in busy_periods
        ]
    return busy_map


def merge_busy_slots(busy_map: dict) -> list[dict]:
    """
    Merge all participants' busy slots into one unified busy list.
    If ANY participant is busy in a window, the whole window is blocked.
    """
    all_busy = []
    for slots in busy_map.values():
        all_busy.extend(slots)

    if not all_busy:
        return []

    # Sort by start time
    all_busy.sort(key=lambda x: x["start"])

    # Merge overlapping intervals
    merged = [all_busy[0]]
    for current in all_busy[1:]:
        last = merged[-1]
        if current["start"] <= last["end"]:
            # Overlapping — extend the end
            merged[-1]["end"] = max(last["end"], current["end"])
        else:
            merged.append(current)

    return merged


def find_free_slots(
    window_start: datetime,
    window_end: datetime,
    busy_slots: list[dict],
    duration_minutes: int = 60,
    slot_granularity_minutes: int = 30
) -> list[dict]:
    """
    Scan the work window and return all free slots of at least `duration_minutes`.
    Slots are aligned to `slot_granularity_minutes` (e.g. every 30 mins: 9:00, 9:30, 10:00...).

    Returns list of {"start": datetime, "end": datetime} in UTC.
    """
    free_slots = []
    cursor = window_start

    # Snap cursor to nearest granularity boundary
    if cursor.minute % slot_granularity_minutes != 0:
        snap_mins = slot_granularity_minutes - (cursor.minute % slot_granularity_minutes)
        cursor += timedelta(minutes=snap_mins)
        cursor = cursor.replace(second=0, microsecond=0)

    while cursor + timedelta(minutes=duration_minutes) <= window_end:
        slot_end = cursor + timedelta(minutes=duration_minutes)

        # Check if this slot conflicts with any busy period
        is_free = True
        for busy in busy_slots:
            # Conflict if slot overlaps busy window
            if cursor < busy["end"] and slot_end > busy["start"]:
                # Jump cursor to end of this busy block
                cursor = busy["end"]
                # Snap to next granularity
                extra = cursor.minute % slot_granularity_minutes
                if extra:
                    cursor += timedelta(minutes=slot_granularity_minutes - extra)
                cursor = cursor.replace(second=0, microsecond=0)
                is_free = False
                break

        if is_free:
            free_slots.append({"start": cursor, "end": slot_end})
            cursor += timedelta(minutes=slot_granularity_minutes)

    return free_slots


def intersection_search(
    participants: list[str],
    local_tz_str: str = "Asia/Kolkata",
    duration_minutes: int = 60,
    search_days: int = 5,
    token_path: str = "token.json"
) -> Optional[dict]:
    """
    MAIN ENTRY POINT.

    Finds the earliest common free slot for all participants within
    working hours (9am-5pm local) over the next `search_days` days.

    Args:
        participants:    List of email addresses to check
        local_tz_str:    IANA timezone string, e.g. "Asia/Kolkata", "America/New_York"
        duration_minutes: How long the meeting needs to be
        search_days:     How many working days to look ahead

    Returns:
        {"start": ISO8601_UTC_str, "end": ISO8601_UTC_str, "local_display": str}
        or None if no slot found
    """
    service = get_calendar_service(token_path)
    local_tz = pytz.timezone(local_tz_str)
    today = datetime.now(tz=pytz.utc).date()

    for day_offset in range(search_days):
        check_date = today + timedelta(days=day_offset)

        # Skip weekends
        if check_date.weekday() >= 5:
            continue

        window_start, window_end = get_work_window_utc(check_date, local_tz_str)

        # Fetch busy slots for all participants
        busy_map   = fetch_busy_slots(service, participants, window_start, window_end)
        busy_merged = merge_busy_slots(busy_map)

        # Find free slots
        free_slots = find_free_slots(
            window_start, window_end, busy_merged, duration_minutes
        )

        if free_slots:
            best = free_slots[0]  # Earliest slot wins
            local_start = best["start"].astimezone(local_tz)
            return {
                "start":         best["start"].isoformat(),       # UTC ISO 8601
                "end":           best["end"].isoformat(),         # UTC ISO 8601
                "local_display": local_start.strftime("%A, %d %b %Y at %I:%M %p %Z"),
                "timezone":      local_tz_str,
                "participants":  participants
            }

    return None  # No slot found in search window


# ── Usage example ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    result = intersection_search(
        participants=["alice@company.com", "bob@company.com", "carol@company.com"],
        local_tz_str="Asia/Kolkata",
        duration_minutes=60
    )
    if result:
        print(f"Best slot: {result['local_display']}")
        print(f"UTC start: {result['start']}")
    else:
        print("No common slot found in next 5 working days.")
