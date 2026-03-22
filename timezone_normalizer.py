"""
timezone_normalizer.py
Converts all meeting times to UTC (ISO 8601) and flags "Sleep Hour"
meetings for VIP clients — triggering Drafting Mode instead of auto-reply.
"""

from dataclasses import dataclass, field
from datetime import datetime, time as dt_time
from enum import Enum
from typing import Optional
import re
import pytz
from dateutil import parser as dateutil_parser

# ── Sleep hours definition (local time of the EMPLOYEE, not the sender) ───────
SLEEP_START = dt_time(22, 0)   # 10:00 PM
SLEEP_END   = dt_time(7, 0)    # 7:00 AM
# i.e. if a meeting would happen between 10pm and 7am for the employee, it's a sleep-hour request

# ── Processing mode ───────────────────────────────────────────────────────────
class ProcessingMode(Enum):
    AUTONOMOUS  = "autonomous"   # AI books automatically
    DRAFTING    = "drafting"     # AI drafts reply, waits for Telegram approval

@dataclass
class NormalizedTime:
    utc_iso:          str                    # "2025-04-10T03:30:00+00:00"
    local_display:    str                    # "Thursday, 10 Apr 2025 at 09:00 AM IST"
    employee_local:   str                    # "Thursday, 10 Apr 2025 at 03:30 AM IST"
    is_sleep_hour:    bool                   # True if falls in employee's sleep window
    is_weekend:       bool
    processing_mode:  ProcessingMode
    source_text:      str                    # Original text that was parsed
    sender_tz:        str                    # e.g. "America/New_York"
    employee_tz:      str                    # e.g. "Asia/Kolkata"
    confidence:       float = 1.0            # 0-1, how confident the parse was
    warnings:         list[str] = field(default_factory=list)


# ── Common timezone aliases (senders often write informal names) ──────────────
TZ_ALIASES = {
    "IST":  "Asia/Kolkata",
    "EST":  "America/New_York",
    "EDT":  "America/New_York",
    "PST":  "America/Los_Angeles",
    "PDT":  "America/Los_Angeles",
    "CST":  "America/Chicago",
    "CDT":  "America/Chicago",
    "GMT":  "UTC",
    "UTC":  "UTC",
    "CET":  "Europe/Paris",
    "CEST": "Europe/Paris",
    "JST":  "Asia/Tokyo",
    "AEST": "Australia/Sydney",
    "SGT":  "Asia/Singapore",
    "GST":  "Asia/Dubai",
}

# ── Time expression patterns to extract from free-form email text ─────────────
TIME_PATTERNS = [
    # "3:00 PM IST", "15:00 UTC", "9 AM PST"
    r'\b(\d{1,2}:\d{2}\s*(?:AM|PM)?)\s*([A-Z]{2,4})?\b',
    # "3 PM", "9am"
    r'\b(\d{1,2}\s*(?:AM|PM|am|pm))\s*([A-Z]{2,4})?\b',
]

DATE_PATTERNS = [
    # "April 10", "10 April", "Apr 10", "10/04/2025", "2025-04-10"
    r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|'
    r'Dec(?:ember)?)\s+\d{1,2}(?:st|nd|rd|th)?\b',
    r'\b\d{1,2}\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|'
    r'Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|'
    r'Dec(?:ember)?)\b',
    r'\b\d{4}-\d{2}-\d{2}\b',
    r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',
    # Relative: "tomorrow", "next Monday", "this Friday"
    r'\b(?:tomorrow|today|next\s+(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)|'
    r'this\s+(?:Monday|Tuesday|Wednesday|Thursday|Friday))\b',
]


def resolve_tz(tz_str: Optional[str], fallback: str = "UTC") -> str:
    """Resolve informal timezone names to IANA strings."""
    if not tz_str:
        return fallback
    # Try alias map first
    upper = tz_str.strip().upper()
    if upper in TZ_ALIASES:
        return TZ_ALIASES[upper]
    # Try pytz directly
    try:
        pytz.timezone(tz_str)
        return tz_str
    except pytz.exceptions.UnknownTimeZoneError:
        return fallback


def extract_datetime_from_text(text: str, sender_tz_str: str) -> tuple[Optional[datetime], float, str]:
    """
    Attempt to extract a datetime from free-form email text.
    Returns (datetime_utc, confidence, matched_text)
    Uses dateutil for robust parsing with fallback to regex extraction.
    """
    sender_tz = pytz.timezone(sender_tz_str)

    # First: try dateutil on the whole text (works for well-formatted strings)
    try:
        dt = dateutil_parser.parse(text, fuzzy=True, dayfirst=False)
        if dt.tzinfo is None:
            dt = sender_tz.localize(dt)
        return dt.astimezone(pytz.utc), 0.75, text[:60]
    except (ValueError, OverflowError):
        pass

    # Second: regex extraction of time + date fragments
    time_match = None
    for pattern in TIME_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            time_match = m
            break

    date_match = None
    for pattern in DATE_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            date_match = m
            break

    if time_match:
        time_str = time_match.group(1)
        tz_hint  = time_match.group(2) if len(time_match.groups()) > 1 else None
        resolved_tz = resolve_tz(tz_hint, sender_tz_str)

        # Combine with date if found
        combined = f"{date_match.group(0)} {time_str}" if date_match else f"today {time_str}"
        try:
            dt = dateutil_parser.parse(combined, fuzzy=True)
            tz = pytz.timezone(resolved_tz)
            if dt.tzinfo is None:
                dt = tz.localize(dt)
            matched_text = f"{date_match.group(0) if date_match else 'today'} {time_str} {tz_hint or ''}"
            return dt.astimezone(pytz.utc), 0.85, matched_text.strip()
        except (ValueError, OverflowError):
            pass

    return None, 0.0, ""


def is_in_sleep_hours(dt_utc: datetime, employee_tz_str: str) -> bool:
    """
    Check if a UTC datetime falls within the employee's sleep hours (10pm – 7am local).
    """
    employee_tz = pytz.timezone(employee_tz_str)
    local_dt = dt_utc.astimezone(employee_tz)
    local_time = local_dt.time()

    # Sleep window crosses midnight: 22:00 -> 23:59 or 00:00 -> 07:00
    if SLEEP_START <= SLEEP_END:
        return SLEEP_START <= local_time <= SLEEP_END
    else:
        return local_time >= SLEEP_START or local_time <= SLEEP_END


def normalize_meeting_time(
    raw_time_text: str,
    sender_email: str,
    sender_tz_str: str,
    employee_tz_str: str = "Asia/Kolkata",
    is_vip_sender: bool = False
) -> NormalizedTime:
    """
    MAIN ENTRY POINT.

    Parses a raw time expression from an email, converts to UTC,
    and determines whether to operate in AUTONOMOUS or DRAFTING mode.

    Args:
        raw_time_text:   The text snippet containing the time (e.g. "3 PM EST tomorrow")
        sender_email:    Email of the person requesting the meeting
        sender_tz_str:   Timezone of the sender (from contacts DB or email headers)
        employee_tz_str: The employee's own timezone (defaults to IST)
        is_vip_sender:   Whether the sender is a VIP/international client

    Returns:
        NormalizedTime dataclass with all fields populated
    """
    warnings = []
    sender_tz_resolved   = resolve_tz(sender_tz_str, "UTC")
    employee_tz_resolved = resolve_tz(employee_tz_str, "Asia/Kolkata")

    # ── Parse the datetime ────────────────────────────────────────────────
    dt_utc, confidence, matched_text = extract_datetime_from_text(
        raw_time_text, sender_tz_resolved
    )

    if dt_utc is None:
        warnings.append("Could not parse a specific time — entering DRAFTING mode for manual review.")
        return NormalizedTime(
            utc_iso="",
            local_display="Unknown",
            employee_local="Unknown",
            is_sleep_hour=False,
            is_weekend=False,
            processing_mode=ProcessingMode.DRAFTING,
            source_text=raw_time_text,
            sender_tz=sender_tz_resolved,
            employee_tz=employee_tz_resolved,
            confidence=0.0,
            warnings=warnings
        )

    # ── Compute display strings ───────────────────────────────────────────
    sender_tz_obj   = pytz.timezone(sender_tz_resolved)
    employee_tz_obj = pytz.timezone(employee_tz_resolved)

    sender_local_dt   = dt_utc.astimezone(sender_tz_obj)
    employee_local_dt = dt_utc.astimezone(employee_tz_obj)

    local_display   = sender_local_dt.strftime("%A, %d %b %Y at %I:%M %p %Z")
    employee_local  = employee_local_dt.strftime("%A, %d %b %Y at %I:%M %p %Z")
    is_weekend      = dt_utc.weekday() >= 5

    # ── Sleep hour check ──────────────────────────────────────────────────
    sleep_hour = is_in_sleep_hours(dt_utc, employee_tz_resolved)

    if sleep_hour:
        warnings.append(
            f"Meeting at {employee_local} falls within sleep hours (10 PM – 7 AM {employee_tz_resolved})."
        )

    if is_weekend:
        warnings.append("Requested time falls on a weekend.")

    # ── Determine processing mode ─────────────────────────────────────────
    # DRAFTING if: VIP sender AND sleep hours, OR confidence too low, OR weekend
    if (is_vip_sender and sleep_hour) or confidence < 0.5 or is_weekend:
        mode = ProcessingMode.DRAFTING
        if is_vip_sender and sleep_hour:
            warnings.append(
                "VIP international client requested a sleep-hour meeting. "
                "Entering DRAFTING MODE — awaiting manual Telegram approval before replying."
            )
    else:
        mode = ProcessingMode.AUTONOMOUS

    return NormalizedTime(
        utc_iso=dt_utc.isoformat(),
        local_display=local_display,
        employee_local=employee_local,
        is_sleep_hour=sleep_hour,
        is_weekend=is_weekend,
        processing_mode=mode,
        source_text=matched_text,
        sender_tz=sender_tz_resolved,
        employee_tz=employee_tz_resolved,
        confidence=confidence,
        warnings=warnings
    )


def notify_drafting_mode(normalized: NormalizedTime, sender_email: str, draft_reply: str):
    """
    When DRAFTING MODE is triggered, send a Telegram notification
    with the AI-drafted reply for manual approval before sending.
    """
    import requests, os
    token   = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["AUTHORIZED_CHAT_ID"]

    warning_text = "\n".join(f"⚠️ {w}" for w in normalized.warnings)
    message = (
        f"✍️ *Drafting Mode Activated*\n\n"
        f"{warning_text}\n\n"
        f"📧 *From:* {sender_email}\n"
        f"🕐 *Requested time (UTC):* `{normalized.utc_iso}`\n"
        f"🕐 *Your local time:* {normalized.employee_local}\n\n"
        f"*Draft reply:*\n```\n{draft_reply[:600]}\n```\n\n"
        f"Approve sending this reply?"
    )
    keyboard = {
        "inline_keyboard": [[
            {"text": "✅ Send it", "callback_data": f"DRAFT_SEND_{sender_email}"},
            {"text": "✏️ Edit manually", "callback_data": f"DRAFT_EDIT_{sender_email}"},
            {"text": "❌ Discard", "callback_data": f"DRAFT_DISCARD_{sender_email}"}
        ]]
    }
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "reply_markup": keyboard
        }
    )


# ── Usage example ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    result = normalize_meeting_time(
        raw_time_text="Can we meet at 3:00 PM EST tomorrow?",
        sender_email="john.doe@globalcorp.com",
        sender_tz_str="EST",
        employee_tz_str="Asia/Kolkata",
        is_vip_sender=True
    )

    print(f"UTC ISO:         {result.utc_iso}")
    print(f"Sender sees:     {result.local_display}")
    print(f"You see:         {result.employee_local}")
    print(f"Sleep hour:      {result.is_sleep_hour}")
    print(f"Mode:            {result.processing_mode.value}")
    print(f"Confidence:      {result.confidence:.0%}")
    for w in result.warnings:
        print(f"WARNING:         {w}")
