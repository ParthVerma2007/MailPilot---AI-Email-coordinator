"""
master_router.py
Classifies incoming emails into one of 5 Action Lanes using local Llama 3 via Ollama.
"""

import json
import psycopg2
import requests
from dataclasses import dataclass
from enum import Enum
from typing import Optional

# ── Action Lane Enum ──────────────────────────────────────────────────────────
class ActionLane(Enum):
    STANDARD_ONE_ON_ONE = "standard_1on1"   # Single participant, book 9-5 window
    MULTI_HEAD_MEET     = "multi_head"      # Multiple participants, intersection search
    BUSY_VIP_HEAD       = "busy_vip"        # VIP is busy, delegate to Telegram
    INTERNAL_SYNC       = "internal_sync"   # Check RAG for redundancy
    HIERARCHY_CLASH     = "hierarchy_clash" # Boss bumps peer, Telegram confirm

# ── Email payload dataclass ───────────────────────────────────────────────────
@dataclass
class IncomingEmail:
    message_id: str          # Gmail messageId (used for idempotency)
    sender_email: str
    sender_name: str
    recipients: list[str]    # All To/CC addresses
    subject: str
    body: str                # Plain text body (PII-masked before passing in)
    received_at: str         # ISO 8601 UTC

# ── DB connection ─────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        host="localhost", dbname="email_agent",
        user="agent_user", password="your_password"
    )

# ── Idempotency check ─────────────────────────────────────────────────────────
def is_already_processed(message_id: str) -> bool:
    """Return True if this messageId has already been handled."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM processed_emails WHERE message_id = %s",
                (message_id,)
            )
            return cur.fetchone() is not None

def mark_as_processed(message_id: str, lane: ActionLane):
    """Stamp this messageId so it's never double-processed."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO processed_emails (message_id, lane, processed_at)
                   VALUES (%s, %s, NOW()) ON CONFLICT DO NOTHING""",
                (message_id, lane.value)
            )
        conn.commit()

# ── Hierarchy lookup ──────────────────────────────────────────────────────────
def get_rank(email: str) -> int:
    """
    Fetch rank from PostgreSQL hierarchy table.
    Returns: 3=Boss, 2=Manager, 1=Peer, 0=Unknown/External
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rank FROM user_hierarchy WHERE email = %s",
                (email.lower(),)
            )
            row = cur.fetchone()
            return row[0] if row else 0

def is_vip(email: str) -> bool:
    return get_rank(email) >= 3

def is_internal(email: str, company_domain: str = "yourcompany.com") -> bool:
    return email.lower().endswith(f"@{company_domain}")

# ── Ollama LLM call ───────────────────────────────────────────────────────────
def llm_classify(email: IncomingEmail) -> dict:
    """
    Ask local Llama 3 to classify the email intent.
    Returns a dict with keys: is_scheduling, is_update_request, confidence
    """
    prompt = f"""You are an email classifier. Analyze this email and respond ONLY with valid JSON.

Email Subject: {email.subject}
Email Body: {email.body[:800]}
Sender: {email.sender_email}
Recipients: {', '.join(email.recipients)}

Respond with exactly this JSON structure:
{{
  "is_scheduling_request": true or false,
  "is_update_request": true or false,
  "proposed_times_mentioned": true or false,
  "participant_count": <number of people involved>,
  "confidence": <0.0 to 1.0>,
  "reasoning": "<one sentence>"
}}"""

    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "llama3",
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.1}  # Low temp for classification
        }
    )
    raw = response.json().get("response", "{}")
    try:
        # Strip markdown fences if model adds them
        clean = raw.strip().lstrip("```json").rstrip("```").strip()
        return json.loads(clean)
    except json.JSONDecodeError:
        return {"is_scheduling_request": False, "confidence": 0.0}

# ── RAG redundancy check ──────────────────────────────────────────────────────
def is_redundant_meeting(email: IncomingEmail) -> bool:
    """
    Use PGVector to find semantically similar past meetings.
    If a nearly identical meeting was held recently, flag as redundant.
    """
    # Get embedding from Ollama
    emb_response = requests.post(
        "http://localhost:11434/api/embeddings",
        json={"model": "llama3", "prompt": f"{email.subject} {email.body[:400]}"}
    )
    embedding = emb_response.json().get("embedding", [])
    if not embedding:
        return False

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) FROM meeting_embeddings
                   WHERE embedding <=> %s::vector < 0.15
                   AND created_at > NOW() - INTERVAL '14 days'
                   AND participants @> %s""",
                (embedding, json.dumps(email.recipients))
            )
            count = cur.fetchone()[0]
            return count > 0

# ── MASTER ROUTER ─────────────────────────────────────────────────────────────
def route_email(email: IncomingEmail) -> tuple[ActionLane, dict]:
    """
    Master Router — classifies email into one of 5 Action Lanes.

    Returns:
        (ActionLane, metadata_dict)
    """

    # ── Step 0: Idempotency guard ─────────────────────────────────────────
    if is_already_processed(email.message_id):
        print(f"[ROUTER] Skipping {email.message_id} — already processed.")
        return None, {}

    # ── Step 1: LLM classification ────────────────────────────────────────
    classification = llm_classify(email)

    if not classification.get("is_scheduling_request"):
        # Not a scheduling email — handle as update request separately
        return None, {"reason": "not_scheduling", "llm": classification}

    sender_rank   = get_rank(email.sender_email)
    sender_is_vip = sender_rank >= 3
    internal_recipients = [r for r in email.recipients if is_internal(r)]
    participant_count = len(set(internal_recipients + [email.sender_email]))

    # ── Lane 3: Internal Sync — RAG redundancy check ──────────────────────
    # Run before booking to avoid duplicating recurring syncs
    if all(is_internal(r) for r in email.recipients):
        if is_redundant_meeting(email):
            mark_as_processed(email.message_id, ActionLane.INTERNAL_SYNC)
            return ActionLane.INTERNAL_SYNC, {
                "reason": "redundant_meeting_detected",
                "participants": internal_recipients
            }

    # ── Lane 5: Hierarchy Clash — Boss bumps a Peer ───────────────────────
    if sender_rank == 3:  # Boss-level sender
        # Check if any recipient has a lower-ranked existing meeting
        # (Calendar conflict check happens in intersection_search.py)
        mark_as_processed(email.message_id, ActionLane.HIERARCHY_CLASH)
        return ActionLane.HIERARCHY_CLASH, {
            "sender_rank": sender_rank,
            "sender": email.sender_email,
            "requires_telegram_confirm": True
        }

    # ── Lane 2: Busy VIP Head ─────────────────────────────────────────────
    if sender_is_vip and participant_count > 1:
        mark_as_processed(email.message_id, ActionLane.BUSY_VIP_HEAD)
        return ActionLane.BUSY_VIP_HEAD, {
            "vip": email.sender_email,
            "delegate_to_telegram": True
        }

    # ── Lane 1: Multi-Head Meet ───────────────────────────────────────────
    if participant_count > 2:
        mark_as_processed(email.message_id, ActionLane.MULTI_HEAD_MEET)
        return ActionLane.MULTI_HEAD_MEET, {
            "participants": internal_recipients,
            "requires_intersection_search": True
        }

    # ── Lane 0: Standard 1-on-1 (default) ────────────────────────────────
    mark_as_processed(email.message_id, ActionLane.STANDARD_ONE_ON_ONE)
    return ActionLane.STANDARD_ONE_ON_ONE, {
        "sender": email.sender_email,
        "booking_window": "09:00-17:00"
    }


# ── DB Schema (run once) ──────────────────────────────────────────────────────
INIT_SQL = """
CREATE TABLE IF NOT EXISTS processed_emails (
    message_id   TEXT PRIMARY KEY,
    lane         TEXT NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_hierarchy (
    email  TEXT PRIMARY KEY,
    name   TEXT,
    rank   INT NOT NULL DEFAULT 1   -- 3=Boss, 2=Manager, 1=Peer
);

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS meeting_embeddings (
    id           SERIAL PRIMARY KEY,
    message_id   TEXT,
    embedding    vector(4096),
    participants JSONB,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);
"""
