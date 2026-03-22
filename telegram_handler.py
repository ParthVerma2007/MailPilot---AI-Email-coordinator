"""
telegram_handler.py
Handles incoming Telegram webhook callbacks for two scenarios:
  - Decision Delegate: VIP head is busy, user approves/declines on behalf
  - Hierarchy Clash:   Boss wants to bump a peer's meeting

Run with: uvicorn telegram_handler:app --host 0.0.0.0 --port 8443
Set webhook: https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://yourserver.com/webhook
"""

import os
import json
import psycopg2
import requests
from fastapi import FastAPI, Request, HTTPException
from datetime import datetime
import pytz

app = FastAPI()

TELEGRAM_TOKEN    = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_API      = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
AUTHORIZED_CHAT_ID = int(os.environ["AUTHORIZED_CHAT_ID"])  # Your personal Telegram chat ID

# ── DB helper ─────────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        host="localhost", dbname="email_agent",
        user="agent_user", password="your_password"
    )

# ── Telegram message sender ───────────────────────────────────────────────────
def send_message(chat_id: int, text: str, reply_markup: dict = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    requests.post(f"{TELEGRAM_API}/sendMessage", json=payload)

def edit_message(chat_id: int, message_id: int, text: str):
    requests.post(f"{TELEGRAM_API}/editMessageText", json={
        "chat_id": chat_id, "message_id": message_id,
        "text": text, "parse_mode": "Markdown"
    })

def answer_callback(callback_query_id: str, text: str = ""):
    requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
        "callback_query_id": callback_query_id,
        "text": text
    })

# ── Pending decisions store (Postgres) ────────────────────────────────────────
def store_pending_decision(decision_id: str, payload: dict):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO pending_decisions (decision_id, payload, status, created_at)
                   VALUES (%s, %s, 'pending', NOW())
                   ON CONFLICT (decision_id) DO NOTHING""",
                (decision_id, json.dumps(payload))
            )
        conn.commit()

def get_pending_decision(decision_id: str) -> dict:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM pending_decisions WHERE decision_id = %s AND status = 'pending'",
                (decision_id,)
            )
            row = cur.fetchone()
            return json.loads(row[0]) if row else None

def resolve_decision(decision_id: str, outcome: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pending_decisions SET status = %s, resolved_at = NOW() WHERE decision_id = %s",
                (outcome, decision_id)
            )
        conn.commit()

# ── Public API: Trigger a Decision Delegate notification ──────────────────────
def notify_decision_delegate(
    decision_id: str,
    vip_name: str,
    vip_email: str,
    requested_time: str,
    existing_meeting: str,
    requester_email: str
):
    """
    Called by master_router when ActionLane == BUSY_VIP_HEAD.
    Sends a Telegram message with Yes/No buttons to the authorized user.
    """
    payload = {
        "type":             "decision_delegate",
        "vip_email":        vip_email,
        "requested_time":   requested_time,
        "existing_meeting": existing_meeting,
        "requester_email":  requester_email
    }
    store_pending_decision(decision_id, payload)

    message = (
        f"🔔 *Decision Required*\n\n"
        f"*{vip_name}* is busy at the requested time.\n\n"
        f"📅 *Requested:* {requested_time}\n"
        f"📌 *Conflicts with:* {existing_meeting}\n"
        f"📧 *Requester:* {requester_email}\n\n"
        f"Should the AI skip the existing meeting and book the new one?"
    )
    keyboard = {
        "inline_keyboard": [[
            {"text": "✅ Yes — Book it", "callback_data": f"DD_YES_{decision_id}"},
            {"text": "❌ No — Keep current", "callback_data": f"DD_NO_{decision_id}"}
        ]]
    }
    send_message(AUTHORIZED_CHAT_ID, message, keyboard)


# ── Public API: Trigger a Hierarchy Clash notification ────────────────────────
def notify_hierarchy_clash(
    decision_id: str,
    boss_name: str,
    boss_email: str,
    requested_time: str,
    bumped_meeting: str,
    bumped_person: str
):
    """
    Called by master_router when ActionLane == HIERARCHY_CLASH.
    Boss (Rank 3) wants a slot held by a Peer (Rank 1).
    """
    payload = {
        "type":          "hierarchy_clash",
        "boss_email":    boss_email,
        "requested_time": requested_time,
        "bumped_meeting": bumped_meeting,
        "bumped_person":  bumped_person
    }
    store_pending_decision(decision_id, payload)

    message = (
        f"⚠️ *Hierarchy Override Request*\n\n"
        f"👑 *{boss_name}* (Boss) wants to meet at *{requested_time}*.\n\n"
        f"This conflicts with:\n"
        f"📌 *{bumped_meeting}* with {bumped_person} (Peer)\n\n"
        f"Approve bumping the peer meeting and booking the boss?"
    )
    keyboard = {
        "inline_keyboard": [[
            {"text": "✅ Yes — Override", "callback_data": f"HC_YES_{decision_id}"},
            {"text": "❌ No — Decline boss", "callback_data": f"HC_NO_{decision_id}"}
        ]]
    }
    send_message(AUTHORIZED_CHAT_ID, message, keyboard)


# ── Webhook endpoint ──────────────────────────────────────────────────────────
@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()

    # Only handle callback_query (button taps)
    callback = data.get("callback_query")
    if not callback:
        return {"ok": True}

    chat_id          = callback["message"]["chat"]["id"]
    message_id       = callback["message"]["message_id"]
    callback_id      = callback["id"]
    callback_data    = callback["data"]

    # ── Security: only accept from authorized chat ────────────────────────
    if chat_id != AUTHORIZED_CHAT_ID:
        answer_callback(callback_id, "Unauthorized.")
        raise HTTPException(status_code=403, detail="Unauthorized chat ID")

    # ── Parse action type and decision_id ─────────────────────────────────
    # Format: "DD_YES_<id>", "DD_NO_<id>", "HC_YES_<id>", "HC_NO_<id>"
    parts = callback_data.split("_", 2)
    if len(parts) != 3:
        answer_callback(callback_id, "Invalid callback.")
        return {"ok": True}

    lane_prefix, outcome, decision_id = parts[0], parts[1], parts[2]
    decision = get_pending_decision(decision_id)

    if not decision:
        answer_callback(callback_id, "Decision already resolved or not found.")
        edit_message(chat_id, message_id, "⚠️ *This decision has already been handled.*")
        return {"ok": True}

    # ── Handle Decision Delegate (BUSY_VIP_HEAD) ──────────────────────────
    if lane_prefix == "DD":
        if outcome == "YES":
            resolve_decision(decision_id, "approved")
            answer_callback(callback_id, "Booking new meeting...")
            edit_message(chat_id, message_id,
                f"✅ *Approved.* Skipping `{decision['existing_meeting']}` "
                f"and booking with {decision['requester_email']} at {decision['requested_time']}."
            )
            # Trigger actual booking (import from booking.py)
            _execute_booking(decision, approved=True)

        elif outcome == "NO":
            resolve_decision(decision_id, "declined")
            answer_callback(callback_id, "Keeping current meeting.")
            edit_message(chat_id, message_id,
                f"❌ *Declined.* Keeping `{decision['existing_meeting']}`. "
                f"A polite decline will be sent to {decision['requester_email']}."
            )
            _execute_booking(decision, approved=False)

    # ── Handle Hierarchy Clash ────────────────────────────────────────────
    elif lane_prefix == "HC":
        if outcome == "YES":
            resolve_decision(decision_id, "override_approved")
            answer_callback(callback_id, "Override approved.")
            edit_message(chat_id, message_id,
                f"✅ *Override approved.* Rescheduling `{decision['bumped_meeting']}` "
                f"and booking boss at {decision['requested_time']}."
            )
            _execute_hierarchy_override(decision, approved=True)

        elif outcome == "NO":
            resolve_decision(decision_id, "override_declined")
            answer_callback(callback_id, "Override declined.")
            edit_message(chat_id, message_id,
                f"❌ *Override declined.* Sending polite decline to {decision['boss_email']}."
            )
            _execute_hierarchy_override(decision, approved=False)

    return {"ok": True}


# ── Booking executors (stubs — connect to your booking.py) ────────────────────
def _execute_booking(decision: dict, approved: bool):
    """
    Stub: Replace with actual Google Calendar create/decline logic.
    Import from your booking module and call the appropriate function.
    """
    if approved:
        print(f"[BOOKING] Creating event for {decision['requester_email']} at {decision['requested_time']}")
        # calendar_service.create_event(...)
        # gmail_service.send_confirmation(...)
    else:
        print(f"[BOOKING] Sending polite decline to {decision['requester_email']}")
        # gmail_service.send_decline(...)

def _execute_hierarchy_override(decision: dict, approved: bool):
    """
    Stub: Replace with actual reschedule + booking logic.
    """
    if approved:
        print(f"[OVERRIDE] Rescheduling {decision['bumped_meeting']}, booking boss slot")
        # calendar_service.delete_event(bumped_meeting_id)
        # calendar_service.create_event(boss_slot)
        # gmail_service.send_reschedule_notice(decision['bumped_person'])
    else:
        print(f"[OVERRIDE] Declining {decision['boss_email']}")
        # gmail_service.send_decline(decision['boss_email'])


# ── DB Schema for pending_decisions (run once) ────────────────────────────────
INIT_SQL = """
CREATE TABLE IF NOT EXISTS pending_decisions (
    decision_id  TEXT PRIMARY KEY,
    payload      JSONB NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending | approved | declined | override_approved | override_declined
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    resolved_at  TIMESTAMPTZ
);
"""
