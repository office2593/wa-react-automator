"""
WhatsApp Reaction → Gmail Automator
Flask backend: receives GREEN API webhooks, applies rules, sends Gmail.
Data stored in PostgreSQL (Railway) or local JSON files (development).
"""
import json
import os
import re
import secrets
import base64
import logging
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

from flask import Flask, request, jsonify, send_from_directory
import requests as http_requests

# ── Google OAuth ──────────────────────────────────────────────────────────────
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import Flow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_LIBS = True
except ImportError:
    GOOGLE_LIBS = False

# ── OpenAI ────────────────────────────────────────────────────────────────────
try:
    from openai import OpenAI as _OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# ── PostgreSQL ────────────────────────────────────────────────────────────────
try:
    import psycopg2
    import psycopg2.extras
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

app = Flask(__name__, static_folder="static")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# write credentials from env vars if provided (Railway deployment)
_client_secrets_env = os.environ.get("GOOGLE_CLIENT_SECRETS")
if _client_secrets_env:
    (DATA_DIR / "client_secrets.json").write_text(_client_secrets_env, encoding="utf-8")

RULES_FILE  = DATA_DIR / "rules.json"
LOG_FILE    = DATA_DIR / "log.json"
TASKS_FILE  = DATA_DIR / "tasks.json"
GROUPS_FILE = DATA_DIR / "groups.json"
BUTTON_MSGS_FILE = DATA_DIR / "button_messages.json"
CONFIG_FILE = DATA_DIR / "config.json"
TOKEN_FILE          = DATA_DIR / "gmail_token.json"
CALENDAR_TOKEN_FILE = DATA_DIR / "calendar_token.json"

# Two separate Google connections:
# - Gmail token: sends mail AND writes task events onto its OWN calendar (it's
#   the account that actually owns that calendar, so it always has write access).
# - Calendar token: a possibly different "viewer" account used only to check
#   free/busy across several calendars — read-only, never writes events.
GMAIL_SCOPES    = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/calendar.events",
]
CALENDAR_SCOPES = ["https://www.googleapis.com/auth/calendar"]

DATABASE_URL = os.environ.get("DATABASE_URL")

# ── personal task scheduling: defaults ──────────────────────────────────────────

DEFAULT_WORK_HOURS = {
    "sunday":    {"active": True,  "start": "09:00", "end": "18:00"},
    "monday":    {"active": True,  "start": "09:00", "end": "18:00"},
    "tuesday":   {"active": True,  "start": "09:00", "end": "18:00"},
    "wednesday": {"active": True,  "start": "09:00", "end": "18:00"},
    "thursday":  {"active": True,  "start": "09:00", "end": "18:00"},
    "friday":    {"active": False, "start": "09:00", "end": "14:00"},
    "saturday":  {"active": False, "start": "09:00", "end": "18:00"},
}

DAY_NAMES_HE = {
    "ראשון": "sunday", "שני": "monday", "שלישי": "tuesday",
    "רביעי": "wednesday", "חמישי": "thursday", "שישי": "friday", "שבת": "saturday",
}

# Python's date.weekday(): Monday=0 ... Sunday=6
WEEKDAY_TO_KEY = {6: "sunday", 0: "monday", 1: "tuesday", 2: "wednesday", 3: "thursday", 4: "friday", 5: "saturday"}

SCHEDULING_LOOKAHEAD_DAYS = 14


# ── database helpers ──────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    if not DATABASE_URL or not POSTGRES_AVAILABLE:
        return
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log.error("DB init error: %s", e)


def db_get(key, default):
    if not DATABASE_URL or not POSTGRES_AVAILABLE:
        return default
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT value FROM kv_store WHERE key = %s", (key,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return json.loads(row[0]) if row else default
    except Exception as e:
        log.error("DB get error: %s", e)
        return default


def db_set(key, value):
    if not DATABASE_URL or not POSTGRES_AVAILABLE:
        return
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO kv_store (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (key, json.dumps(value, ensure_ascii=False)))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log.error("DB set error: %s", e)


# initialize DB on startup
init_db()


# ── helpers ───────────────────────────────────────────────────────────────────

def load_json(path, default):
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_rules():
    if DATABASE_URL and POSTGRES_AVAILABLE:
        return db_get("rules", [])
    return load_json(RULES_FILE, [])


def save_rules(data):
    if DATABASE_URL and POSTGRES_AVAILABLE:
        db_set("rules", data)
    else:
        save_json(RULES_FILE, data)


def load_config():
    if DATABASE_URL and POSTGRES_AVAILABLE:
        return db_get("config", {})
    return load_json(CONFIG_FILE, {})


def save_config_data(data):
    if DATABASE_URL and POSTGRES_AVAILABLE:
        db_set("config", data)
    else:
        save_json(CONFIG_FILE, data)


def get_work_hours(cfg=None):
    cfg = cfg if cfg is not None else load_config()
    configured = cfg.get("work_hours") or {}
    merged = {k: dict(v) for k, v in DEFAULT_WORK_HOURS.items()}
    for day, entry in configured.items():
        if day in merged:
            merged[day].update(entry)
    return merged


def get_timezone_name(cfg=None):
    cfg = cfg if cfg is not None else load_config()
    return cfg.get("timezone") or "Asia/Jerusalem"


def get_busy_calendar_ids(cfg=None):
    """Calendars checked for busy time when looking for a free slot. Defaults to
    just the business calendar if no extra calendars were configured — but you
    can list several (e.g. a personal calendar you also don't want double-booked).
    New events are always written to `business_calendar_id` alone, regardless."""
    cfg = cfg if cfg is not None else load_config()
    ids = cfg.get("busy_calendar_ids")
    if isinstance(ids, list) and ids:
        return [c for c in ids if c]
    return [cfg.get("business_calendar_id") or "primary"]


def load_log():
    if DATABASE_URL and POSTGRES_AVAILABLE:
        return db_get("log", [])
    return load_json(LOG_FILE, [])


def append_log(entry: dict):
    entries = load_log()
    entries.insert(0, entry)
    entries = entries[:200]
    if DATABASE_URL and POSTGRES_AVAILABLE:
        db_set("log", entries)
    else:
        save_json(LOG_FILE, entries)


def load_tasks():
    if DATABASE_URL and POSTGRES_AVAILABLE:
        return db_get("tasks", [])
    return load_json(TASKS_FILE, [])


def save_tasks(data):
    if DATABASE_URL and POSTGRES_AVAILABLE:
        db_set("tasks", data)
    else:
        save_json(TASKS_FILE, data)


def append_task(entry: dict):
    tasks = load_tasks()
    tasks.insert(0, entry)
    save_tasks(tasks)


def load_groups():
    if DATABASE_URL and POSTGRES_AVAILABLE:
        return db_get("groups", [])
    return load_json(GROUPS_FILE, [])


def save_groups(data):
    if DATABASE_URL and POSTGRES_AVAILABLE:
        db_set("groups", data)
    else:
        save_json(GROUPS_FILE, data)


def load_button_messages():
    if DATABASE_URL and POSTGRES_AVAILABLE:
        return db_get("button_messages", [])
    return load_json(BUTTON_MSGS_FILE, [])


def save_button_messages(data):
    if DATABASE_URL and POSTGRES_AVAILABLE:
        db_set("button_messages", data)
    else:
        save_json(BUTTON_MSGS_FILE, data)


def load_gmail_token():
    if DATABASE_URL and POSTGRES_AVAILABLE:
        token = db_get("gmail_token", None)
        if token:
            save_json(TOKEN_FILE, token)
    return TOKEN_FILE


def save_gmail_token(token_dict):
    save_json(TOKEN_FILE, token_dict)
    if DATABASE_URL and POSTGRES_AVAILABLE:
        db_set("gmail_token", token_dict)


def load_calendar_token():
    if DATABASE_URL and POSTGRES_AVAILABLE:
        token = db_get("calendar_token", None)
        if token:
            save_json(CALENDAR_TOKEN_FILE, token)
    return CALENDAR_TOKEN_FILE


def save_calendar_token(token_dict):
    save_json(CALENDAR_TOKEN_FILE, token_dict)
    if DATABASE_URL and POSTGRES_AVAILABLE:
        db_set("calendar_token", token_dict)


# ── AI email generation ───────────────────────────────────────────────────────

def generate_email_content(sender_name: str, message: str, employee_name: str) -> dict:
    """Call OpenAI API to generate a human-sounding email subject and body."""
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key or not OPENAI_AVAILABLE:
        log.warning("OpenAI API key missing or library not installed — using fallback")
        return {
            "subject": f"{sender_name} — הודעה חדשה",
            "body": f"היי {employee_name},\n\n{sender_name} שלח הודעה:\n{message}"
        }
    try:
        client = _OpenAI(api_key=api_key)
        prompt = (
            f'אתה עוזר של אורן דולב, רואה חשבון.\n'
            f'אורן קיבל הודעת וואטסאפ מהלקוח {sender_name} עם התוכן הבא:\n'
            f'"{message}"\n\n'
            f'שם הלקוח הוא {sender_name} — כלול אותו בתחילת המשפט בגוף המייל.\n\n'
            f'כתוב דוא"ל קצר בעברית עסקית-ידידותית ל{employee_name}.\n'
            f'מבנה הדוא"ל:\n'
            f'שורה 1: פנייה בלבד ("היי {employee_name}," או "שלום {employee_name},")\n'
            f'שורה ריקה\n'
            f'שורה 3: משפט אחד או שניים שמתחיל בשם הלקוח ומתאר את הבקשה בלשון עבר קצרה.\n'
            f'לא לכתוב בגוף ראשון. לא לכתוב חתימה.\n'
            f'החזר JSON בלבד עם שני שדות: "subject" ו-"body".\n'
            f'subject = שם השולח + תמצות קצר של הבקשה.\n'
            f'body = הטקסט המלא כפי שתואר, עם ירידת שורה (\n) בין הפנייה לגוף.'
        )
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=300,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}]
        )
        raw = resp.choices[0].message.content.strip()
        return json.loads(raw)
    except Exception as e:
        log.error("AI email generation failed: %s", e)
        return {
            "subject": f"{sender_name} — הודעה חדשה",
            "body": f"היי {employee_name},\n\n{sender_name} שלח הודעה:\n{message}"
        }


def suggest_duration_minutes(task_text: str, personal_email: str):
    """Suggests a duration for a new personal task, based on similar past tasks
    with a confirmed estimate. Returns (minutes, reason) or (None, None) if
    there isn't enough history yet, nothing similar was found, or the AI call
    failed — callers should fall back to the plain duration question either way."""
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key or not OPENAI_AVAILABLE:
        return None, None

    history = [
        t for t in load_tasks()
        if t.get("email_to") == personal_email and t.get("estimated_minutes") is not None
    ]
    if len(history) < 3:
        return None, None

    examples = "\n".join(
        f'- "{(t.get("original_text") or "")[:80]}" → {t["estimated_minutes"]} דקות'
        for t in history[:20]
    )
    prompt = (
        f'אלה משימות עבר עם משך הזמן שאושר בפועל על ידי המשתמש:\n{examples}\n\n'
        f'המשימה החדשה: "{task_text}"\n\n'
        f'אם אחת מהדוגמאות דומה מספיק כדי להעריך בביטחון סביר את משך הזמן למשימה החדשה, '
        f'החזר JSON {{"minutes": מספר, "reason": "נימוק קצר בעברית, עד 10 מילים"}}. '
        f'אם אין דמיון מספק — החזר {{"minutes": null, "reason": null}}. החזר JSON בלבד, בלי טקסט נוסף.'
    )
    try:
        client = _OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=120,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}]
        )
        data = json.loads(resp.choices[0].message.content.strip())
        minutes = data.get("minutes")
        if isinstance(minutes, (int, float)) and minutes > 0:
            return int(minutes), (data.get("reason") or "")
        return None, None
    except Exception as e:
        log.warning("AI duration suggestion failed: %s", e)
        return None, None


def parse_negotiation_action(task_text: str, user_reply: str):
    """Uses OpenAI to interpret a free-text reply to an already-scheduled
    task's confirmation message. Returns a dict {"action": "confirm"|
    "reschedule"|"cancel", "when_iso": "<ISO datetime>"|None, "confident": bool}
    or None if the AI call failed — callers should ask the user to clarify."""
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key or not OPENAI_AVAILABLE:
        return None
    now_str = datetime.now(ZoneInfo(get_timezone_name())).isoformat()
    prompt = (
        f'המשימה "{task_text}" מתוזמנת ביומן. המשתמש כתב בתגובה: "{user_reply}"\n'
        f'הזמן הנוכחי הוא: {now_str}\n\n'
        f'החזר JSON בלבד: {{"action": "confirm"|"reschedule"|"cancel", '
        f'"when_iso": "YYYY-MM-DDTHH:MM:SS" או null, "confident": true/false}}.\n'
        f'action="confirm" אם המשתמש רק מאשר או לא רוצה לשנות כלום.\n'
        f'action="cancel" אם המשתמש רוצה לבטל/למחוק את המשימה מהיומן.\n'
        f'action="reschedule" אם המשתמש רוצה זמן אחר — when_iso הוא הפירוש שלך לזמן החדש (יחסית לזמן הנוכחי).\n'
        f'confident=true רק אם אתה בטוח מאוד בפירוש של when_iso (למשל תאריך/שעה מפורשים); '
        f'אם הניסוח מעורפל (כמו "מחר בבוקר") — עדיין תן את הפירוש הכי סביר ב-when_iso אבל עם confident=false.\n'
        f'החזר JSON בלבד, בלי טקסט נוסף.'
    )
    try:
        client = _OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=150,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}]
        )
        return json.loads(resp.choices[0].message.content.strip())
    except Exception as e:
        log.warning("Negotiation parse failed: %s", e)
        return None


# ── Gmail send ────────────────────────────────────────────────────────────────

def get_gmail_service():
    if not GOOGLE_LIBS:
        raise RuntimeError("google-auth libraries not installed")
    load_gmail_token()
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), GMAIL_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_gmail_token(json.loads(creds.to_json()))
        else:
            raise RuntimeError("Gmail not authorized — please connect via the dashboard")
    return build("gmail", "v1", credentials=creds)


def get_calendar_service():
    """Separate OAuth token from Gmail — used only to READ free/busy. This
    account may only have viewer access to some calendars, so it must never be
    used to write events (see get_calendar_write_service)."""
    if not GOOGLE_LIBS:
        raise RuntimeError("google-auth libraries not installed")
    load_calendar_token()
    creds = None
    if CALENDAR_TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(CALENDAR_TOKEN_FILE), CALENDAR_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_calendar_token(json.loads(creds.to_json()))
        else:
            raise RuntimeError("Google Calendar not authorized — please connect via the dashboard")
    return build("calendar", "v3", credentials=creds)


def get_calendar_write_service():
    """Uses the Gmail token (which includes calendar.events scope) to create
    and delete task events — this is the account that actually owns its own
    calendar, so it always has write access, unlike a separate viewer account."""
    if not GOOGLE_LIBS:
        raise RuntimeError("google-auth libraries not installed")
    load_gmail_token()
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), GMAIL_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_gmail_token(json.loads(creds.to_json()))
        else:
            raise RuntimeError("Gmail not authorized — please connect via the dashboard")
    return build("calendar", "v3", credentials=creds)


def build_email_html(body_text: str, complete_url: str = "") -> str:
    """Build Outlook-compatible HTML email with David font and purple gradient design."""
    msg = (body_text or "").replace("\n", "<br>")
    button_row = ""
    if complete_url:
        button_row = f"""
  <!-- COMPLETE TASK BUTTON -->
  <tr>
    <td align="center" style="padding:0 5% 28px;background:#ffffff;text-align:center">
      <!--[if mso]>
      <v:roundrect xmlns:v="urn:schemas-microsoft-com:vml" href="{complete_url}" style="height:46px;v-text-anchor:middle;width:320px" arcsize="22%" fillcolor="#7B2D8B" stroke="f">
      <center style="color:#ffffff;font-family:David,Arial,sans-serif;font-size:15px;font-weight:bold">יש ללחוץ כאן לאישור ביצוע המשימה</center>
      </v:roundrect>
      <![endif]-->
      <!--[if !mso]><!-->
      <table cellpadding="0" cellspacing="0" border="0" align="center" style="margin:0 auto">
        <tr>
          <td align="center" bgcolor="#7B2D8B" style="background:linear-gradient(135deg,#7B2D8B 0%,#4a0a6b 100%);border-radius:10px">
            <a href="{complete_url}" target="_blank" style="display:inline-block;color:#ffffff;font-family:David,Arial,sans-serif;font-size:15px;font-weight:bold;text-decoration:none;padding:14px 32px;border-radius:10px;text-align:center">יש ללחוץ כאן לאישור ביצוע המשימה</a>
          </td>
        </tr>
      </table>
      <!--<![endif]-->
    </td>
  </tr>
"""
    return f"""<!DOCTYPE html>
<html dir="rtl" lang="he" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<!--[if mso]>
<noscript><xml><o:OfficeDocumentSettings><o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml></noscript>
<![endif]-->
<style>
  body,table,td{{margin:0;padding:0;font-family:David,Arial,sans-serif;direction:rtl;text-align:right}}
</style>
</head>
<body style="margin:0;padding:20px 0;background:#f4f0f8;direction:ltr">
<table width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#f4f0f8" style="background:#f4f0f8;direction:ltr">
<tr><td align="center" style="padding:20px 0">

<table width="100%" cellpadding="0" cellspacing="0" border="0" align="center" style="max-width:600px;background:#ffffff;border:1px solid #e0d0f0">

  <!-- HEADER -->
  <tr>
    <td align="center" bgcolor="#2d0050" style="padding:0;background:linear-gradient(135deg,#4a0a6b 0%,#1a0030 100%)">
      <!--[if mso]>
      <v:rect xmlns:v="urn:schemas-microsoft-com:vml" fill="t" stroke="f" style="width:600px;height:130px;display:block">
      <v:fill type="gradient" color="#4a0a6b" color2="#1a0030" angle="90"/>
      <v:textbox inset="0,0,0,0">
      <![endif]-->
      <table width="100%" cellpadding="0" cellspacing="0" border="0">
      <tr><td align="center" valign="middle" style="padding:30px 24px;font-family:David,Arial,sans-serif;text-align:center">
      <p style="margin:0;font-size:26px;font-weight:bold;color:#ffffff;font-family:David,Arial,sans-serif;text-align:center;direction:rtl;line-height:1.2">אורן דולב</p>
      <p style="margin:8px 0 0 0;font-size:15px;color:#c084d8;font-family:David,Arial,sans-serif;text-align:center;letter-spacing:2px;direction:rtl;line-height:1.2">רואה חשבון</p>
      </td></tr>
      </table>
      <!--[if mso]>
      </v:textbox></v:rect>
      <![endif]-->
    </td>
  </tr>

  <!-- ACCENT BAR -->
  <tr>
    <td height="5" style="padding:0;font-size:0;line-height:0">
      <!--[if mso]>
      <v:rect xmlns:v="urn:schemas-microsoft-com:vml" fill="true" stroke="false" style="width:600px;height:5px;">
      <v:fill type="gradient" color="#7B2D8B" color2="#c084d8" angle="0"/>
      <v:textbox inset="0,0,0,0"><p style="margin:0;font-size:0">&nbsp;</p></v:textbox>
      </v:rect>
      <![endif]-->
      <!--[if !mso]><!-->
      <div style="background:linear-gradient(90deg,#7B2D8B 0%,#c084d8 100%);height:5px;line-height:5px;font-size:0">&nbsp;</div>
      <!--<![endif]-->
    </td>
  </tr>

  <!-- BODY -->
  <tr>
    <td align="right" style="padding:24px 5%;background:#ffffff;direction:rtl;text-align:right;font-family:David,Arial,sans-serif;font-size:17px;color:#2d2d2d;line-height:1.9">
      {msg}
    </td>
  </tr>
{button_row}
  <!-- DIVIDER -->
  <tr>
    <td style="padding:0 32px;font-size:0;line-height:0;height:1px;background:#e0d0f0">&nbsp;</td>
  </tr>

  <!-- FOOTER -->
  <tr>
    <td align="center" bgcolor="#1a0030" style="padding:0;background:linear-gradient(135deg,#1a0030 0%,#4a0a6b 100%)">
      <!--[if mso]>
      <v:rect xmlns:v="urn:schemas-microsoft-com:vml" fill="t" stroke="f" style="width:600px;height:56px;display:block">
      <v:fill type="gradient" color="#1a0030" color2="#4a0a6b" angle="90"/>
      <v:textbox inset="0,0,0,0">
      <![endif]-->
      <table width="100%" cellpadding="0" cellspacing="0" border="0">
      <tr><td align="center" valign="middle" style="padding:16px 24px;font-family:David,Arial,sans-serif;text-align:center">
      <p style="margin:0;font-size:12px;color:#c084d8;font-family:David,Arial,sans-serif;text-align:center;direction:rtl;line-height:1.5">אורן דולב — רואה חשבון &nbsp;|&nbsp; נשלח אוטומטית ממערכת WhatsApp</p>
      </td></tr>
      </table>
      <!--[if mso]>
      </v:textbox></v:rect>
      <![endif]-->
    </td>
  </tr>

</table>

</td></tr>
</table>
</body>
</html>"""


def send_gmail(to: str, subject: str, body_text: str, complete_url: str = ""):
    service = get_gmail_service()
    msg = MIMEMultipart("alternative")
    msg["To"]      = to
    msg["Subject"] = subject
    msg["From"]    = "me"

    plain = body_text + (f"\n\nלאישור ביצוע המשימה: {complete_url}" if complete_url else "")
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(build_email_html(body_text, complete_url), "html", "utf-8"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()


# ── GREEN API helper ──────────────────────────────────────────────────────────

def green_api_url(config, method):
    return (
        f"https://api.green-api.com/waInstance{config['green_instance_id']}"
        f"/{method}/{config['green_api_token']}"
    )


def green_api_send_message(chat_id: str, text: str) -> dict:
    """Send a WhatsApp text message via GREEN API (same instance/token used to
    receive the webhook — no template restrictions, unlike the Twilio channel
    used for the monthly group broadcasts). Returns the API response, which
    includes idMessage on success — used to match future quoted replies."""
    if not chat_id:
        return {}
    cfg = load_config()
    if not cfg.get("green_instance_id") or not cfg.get("green_api_token"):
        log.warning("GREEN API not configured — cannot send message to %s", chat_id)
        return {}
    try:
        url = green_api_url(cfg, "sendMessage")
        r = http_requests.post(url, json={"chatId": chat_id, "message": text}, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error("Failed to send GREEN API message to %s: %s", chat_id, e)
        return {}


def fetch_and_save_contacts() -> dict:
    """Fetch all contacts from GREEN API and save to DB. Returns phone→name dict."""
    try:
        cfg = load_config()
        if not cfg.get("green_instance_id") or not cfg.get("green_api_token"):
            return {}
        url = green_api_url(cfg, "getContacts")
        r = http_requests.get(url, timeout=15)
        log.info("getContacts status: %s body: %s", r.status_code, r.text[:300])
        contacts_list = r.json()
        contacts = {}
        for c in contacts_list:
            chat_id = c.get("id", "")
            name = c.get("contactName") or c.get("name") or c.get("pushname") or ""
            if chat_id and name:
                contacts[chat_id] = name
            # log first 3 contacts to see field structure
        if contacts_list:
            log.info("Sample contact fields: %s", json.dumps(contacts_list[0])[:200])
        db_set("contacts", contacts)
        db_set("contacts_updated", datetime.utcnow().isoformat())
        log.info("Contacts synced: %d contacts", len(contacts))
        return contacts
    except Exception as e:
        log.warning("Could not fetch contacts: %s", e)
        return {}


def get_message_from_history(chat_id: str, stanza_id: str = "") -> str:
    """Fetch message from chat history. If stanza_id given, look for that message; else return last."""
    if not chat_id:
        return ""
    try:
        cfg = load_config()
        if not cfg.get("green_instance_id") or not cfg.get("green_api_token"):
            return ""
        url = green_api_url(cfg, "getChatHistory")
        r = http_requests.post(url, json={"chatId": chat_id, "count": 100}, timeout=15)
        log.info("getChatHistory status=%s count=%d", r.status_code, len(r.json()) if r.ok else 0)
        messages = r.json()
        if not isinstance(messages, list):
            return ""

        def extract_text(m):
            # getChatHistory puts fields at top level OR inside messageData
            md = m.get("messageData", m)  # fallback to message itself
            return (
                m.get("textMessage", "")
                or (m.get("extendedTextMessageData") or {}).get("text", "")
                or (m.get("textMessageData") or {}).get("textMessage", "")
                or (md.get("extendedTextMessageData") or {}).get("text", "")
                or (md.get("textMessageData") or {}).get("textMessage", "")
                or m.get("caption", "")
                or ""
            )

        def get_type(m):
            return m.get("typeMessage", "") or m.get("messageData", {}).get("typeMessage", "")

        # find by stanzaId if provided
        if stanza_id:
            for msg in messages:
                if (msg.get("idMessage") == stanza_id or
                    msg.get("stanzaId") == stanza_id):
                    text = extract_text(msg)
                    if text:
                        return text

        # fallback: last non-reaction message
        for msg in reversed(messages):
            if get_type(msg) == "reactionMessage":
                continue
            text = extract_text(msg)
            if text:
                return text
        return ""
    except Exception as e:
        log.warning("Could not fetch message from history: %s", e)
        return ""


def get_contact_name(chat_id: str) -> str:
    """Get contact name from local DB cache."""
    contacts = db_get("contacts", {})
    name = (
        contacts.get(chat_id) or
        contacts.get(chat_id.replace("@c.us", "") + "@c.us") or
        contacts.get(chat_id.replace("@c.us", "")) or
        ""
    )
    log.info("get_contact_name chatId=%s found=%r (total contacts: %d, sample keys: %s)",
             chat_id, name, len(contacts), list(contacts.keys())[:3])
    return name


# ── personal task scheduling: text parsers ──────────────────────────────────────

_DURATION_WORDS = {
    "רבע שעה": 15, "חצי שעה": 30, "שלושת רבעי שעה": 45,
    "שעה וחצי": 90, "שעתיים": 120, "שעה": 60,
}

_AFFIRMATIVE_WORDS = {"כן", "מאשר", "מאשרת", "אישור", "אוקי", "אוקיי", "בסדר", "yes", "ok", "okay"}


def _is_affirmative(text: str) -> bool:
    """True if the reply *starts with* an affirmative word — so 'כן' and
    'כן עד יום שלישי' (confirm + deadline together) both count."""
    words = (text or "").strip().lower().split()
    return bool(words) and words[0] in _AFFIRMATIVE_WORDS


def parse_duration_minutes(text: str):
    """Parse a free-text WhatsApp reply into a minute count, or None if not understood."""
    t = (text or "").strip()
    if not t:
        return None
    for phrase, minutes in sorted(_DURATION_WORDS.items(), key=lambda kv: -len(kv[0])):
        if phrase in t:
            return minutes
    m = re.search(r'(\d+)\s*(שע(ות)?|hour|hr)', t)
    if m:
        return int(m.group(1)) * 60
    m = re.search(r'(\d+)', t)
    if m:
        return int(m.group(1))
    return None


_KEY_TO_WEEKDAY = {v: k for k, v in WEEKDAY_TO_KEY.items()}
_DEADLINE_DAY_RE = re.compile(r'(?:עד|דדליין)\s+(?:יום\s+)?(ראשון|שני|שלישי|רביעי|חמישי|שישי|שבת)')
_DEADLINE_DATE_RE = re.compile(r'(?:עד|דדליין)\s+(?:ה-)?(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?')


def parse_deadline(text: str, today: date = None):
    """Parse a free-text deadline phrase like 'עד יום שלישי' or 'עד 20/8' into a
    date — always the NEXT such day/date strictly after today (never today or
    in the past), since 'עד יום שלישי' said on a Tuesday means next Tuesday."""
    text = (text or "").strip()
    if today is None:
        today = datetime.now(ZoneInfo(get_timezone_name())).date()

    m = _DEADLINE_DATE_RE.search(text)
    if m:
        day, month = int(m.group(1)), int(m.group(2))
        year = int(m.group(3)) if m.group(3) else today.year
        if year < 100:
            year += 2000
        try:
            d = date(year, month, day)
        except ValueError:
            return None
        if d < today:
            try:
                d = date(year + 1, month, day)
            except ValueError:
                return None
        return d

    m = _DEADLINE_DAY_RE.search(text)
    if m:
        target_weekday = _KEY_TO_WEEKDAY[DAY_NAMES_HE[m.group(1)]]
        days_ahead = (target_weekday - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        return today + timedelta(days=days_ahead)

    return None


_WORK_HOURS_RE = re.compile(
    r'(ראשון|שני|שלישי|רביעי|חמישי|שישי|שבת)\s+'
    r'(?:(\d{1,2}(?::\d{2})?)\s*(?:עד|-|–)\s*(\d{1,2}(?::\d{2})?)|(סגור|לא עובד))'
)


def _normalize_hhmm(s: str) -> str:
    s = s.strip()
    if ":" in s:
        h, m = s.split(":")
        return f"{int(h):02d}:{int(m):02d}"
    return f"{int(s):02d}:00"


def parse_work_hours_command(text: str):
    """Parse a Hebrew day+hours command like 'שלישי 9 עד 20' or 'שבת סגור'.
    Returns (day_key, updated_entry) or None if the text doesn't match."""
    m = _WORK_HOURS_RE.search((text or "").strip())
    if not m:
        return None
    day_key = DAY_NAMES_HE[m.group(1)]
    if m.group(4):
        return day_key, {"active": False, "start": "09:00", "end": "18:00"}
    return day_key, {"active": True, "start": _normalize_hhmm(m.group(2)), "end": _normalize_hhmm(m.group(3))}


# ── personal task scheduling: engine ─────────────────────────────────────────────

def _update_task_fields(task_id: str, **fields):
    tasks = load_tasks()
    for i, t in enumerate(tasks):
        if t["id"] == task_id:
            t.update(fields)
            tasks[i] = t
            save_tasks(tasks)
            return t
    return None


def _delete_calendar_event(event_id: str):
    if not event_id:
        return
    cfg = load_config()
    calendar_id = cfg.get("business_calendar_id") or "primary"
    try:
        service = get_calendar_write_service()
        service.events().delete(calendarId=calendar_id, eventId=event_id).execute()
    except Exception as e:
        log.warning("Could not delete calendar event %s: %s", event_id, e)


def _move_scheduled_task(task: dict, new_start: datetime):
    """Moves an already-scheduled task's calendar event to a new start time
    (keeping its original duration), and updates scheduled_at to match."""
    cfg = load_config()
    calendar_id = cfg.get("business_calendar_id") or "primary"
    duration = timedelta(minutes=task.get("estimated_minutes") or 30)
    new_end = new_start + duration
    service = get_calendar_write_service()
    service.events().patch(calendarId=calendar_id, eventId=task["calendar_event_id"], body={
        "start": {"dateTime": new_start.isoformat()},
        "end": {"dateTime": new_end.isoformat()},
    }).execute()
    _update_task_fields(task["id"], scheduled_at=new_start.isoformat())


def _compute_free_windows(cfg, days_ahead=SCHEDULING_LOOKAHEAD_DAYS):
    """Free windows (tz-aware datetime pairs) within configured work hours over the
    next `days_ahead` days, with existing Google Calendar busy blocks subtracted."""
    tz = ZoneInfo(get_timezone_name(cfg))
    now = datetime.now(tz)
    work_hours = get_work_hours(cfg)
    busy_calendar_ids = get_busy_calendar_ids(cfg)

    range_start = now
    range_end = (now + timedelta(days=days_ahead)).replace(hour=23, minute=59, second=59, microsecond=0)

    service = get_calendar_service()
    fb = service.freebusy().query(body={
        "timeMin": range_start.isoformat(),
        "timeMax": range_end.isoformat(),
        "timeZone": get_timezone_name(cfg),
        "items": [{"id": cid} for cid in busy_calendar_ids],
    }).execute()
    # merge busy blocks from every calendar into one combined "when am I actually free" view
    busy_raw = []
    for cid in busy_calendar_ids:
        busy_raw.extend(fb.get("calendars", {}).get(cid, {}).get("busy", []))
    busy = [
        (datetime.fromisoformat(b["start"]).astimezone(tz), datetime.fromisoformat(b["end"]).astimezone(tz))
        for b in busy_raw
    ]

    windows = []
    for day_offset in range(days_ahead + 1):
        day = (now + timedelta(days=day_offset)).date()
        hours = work_hours.get(WEEKDAY_TO_KEY[day.weekday()], {})
        if not hours.get("active"):
            continue
        start_h, start_m = map(int, hours["start"].split(":"))
        end_h, end_m = map(int, hours["end"].split(":"))
        day_start = datetime(day.year, day.month, day.day, start_h, start_m, tzinfo=tz)
        day_end   = datetime(day.year, day.month, day.day, end_h, end_m, tzinfo=tz)
        if day_start < now:
            day_start = now
        if day_end <= day_start:
            continue

        free_segments = [(day_start, day_end)]
        for b_start, b_end in busy:
            if b_end <= day_start or b_start >= day_end:
                continue
            new_segments = []
            for seg_start, seg_end in free_segments:
                if b_end <= seg_start or b_start >= seg_end:
                    new_segments.append((seg_start, seg_end))
                    continue
                if b_start > seg_start:
                    new_segments.append((seg_start, b_start))
                if b_end < seg_end:
                    new_segments.append((b_end, seg_end))
            free_segments = new_segments
        windows.extend(free_segments)

    return windows


DEADLINE_URGENCY_K = 7.0    # tuning knob: urgency contribution == this when 1 day remains
DEADLINE_URGENCY_DUE = 30.0  # fixed high urgency once the deadline is today or already passed


def _deadline_urgency(task: dict, now: datetime) -> float:
    deadline_str = task.get("deadline")
    if not deadline_str:
        return 0.0
    deadline_d = date.fromisoformat(deadline_str)
    days_left = (deadline_d - now.date()).days
    if days_left <= 0:
        return DEADLINE_URGENCY_DUE
    return DEADLINE_URGENCY_K / days_left


def _wsjf_score(task: dict, now: datetime) -> float:
    """Weighted-Shortest-Job-First: urgency / estimated duration. Urgency = days
    waited (short fresh tasks can outrank a big old one, but an old task's score
    keeps climbing until it eventually wins — never starved forever) PLUS a
    deadline-proximity term that spikes as a deadline approaches or passes."""
    created = datetime.fromisoformat(task["time"])
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    age_days = max((now - created).total_seconds() / 86400.0, 0.01)
    duration = max(task.get("estimated_minutes") or 1, 1)
    urgency = age_days + _deadline_urgency(task, now)
    return urgency / duration


def try_schedule_pending_tasks():
    """Schedule every 'unscheduled' personal task (estimate confirmed, no calendar
    slot yet) into the business calendar: WSJF order, placed into the earliest
    free window (chronologically) that still fits its duration — not the
    tightest-fitting one, so a task never skips over real free time today/
    tomorrow just to leave room for a hypothetically bigger task later. Runs
    immediately after an estimate is confirmed, and again periodically to catch
    tasks that had no slot back then."""
    cfg = load_config()
    chat_id = cfg.get("personal_whatsapp_chat_id")
    if not chat_id:
        return

    tasks = load_tasks()
    pending = [t for t in tasks if t.get("schedule_status") == "unscheduled"]
    if not pending:
        return

    try:
        windows = _compute_free_windows(cfg)
    except Exception as e:
        log.error("Could not compute free calendar windows: %s", e)
        return

    tz = ZoneInfo(get_timezone_name(cfg))
    now = datetime.now(tz)
    pending.sort(key=lambda t: _wsjf_score(t, now), reverse=True)

    calendar_id = cfg.get("business_calendar_id") or "primary"
    service = get_calendar_write_service()

    for task in pending:
        duration = timedelta(minutes=task.get("estimated_minutes") or 30)
        candidates = [(i, w) for i, w in enumerate(windows) if (w[1] - w[0]) >= duration]
        if not candidates:
            continue
        idx, (w_start, w_end) = min(candidates, key=lambda iw: iw[1][0])
        event_start, event_end = w_start, w_start + duration

        try:
            event = service.events().insert(calendarId=calendar_id, body={
                "summary": f"📋 משימה: {(task.get('original_text') or '')[:60]}",
                "description": f"נוצר אוטומטית ממערכת המשימות. task_id={task['id']}",
                "start": {"dateTime": event_start.isoformat()},
                "end": {"dateTime": event_end.isoformat()},
                "colorId": "5",
                "extendedProperties": {"private": {"task_id": task["id"]}},
            }).execute()
        except Exception as e:
            log.error("Failed to create calendar event for task %s: %s", task["id"], e)
            continue

        remaining = []
        if event_start > w_start:
            remaining.append((w_start, event_start))
        if event_end < w_end:
            remaining.append((event_end, w_end))
        windows[idx:idx + 1] = remaining

        # Sent per-task (not merged) so each confirmation has its own quotable
        # message id — that's the entry point for negotiating this one task later.
        summary_text = f"תוזמן לך ביומן: {event_start.strftime('%d/%m %H:%M')} — {(task.get('original_text') or '')[:60]}"
        confirm_resp = green_api_send_message(chat_id, summary_text)

        for i, t in enumerate(tasks):
            if t["id"] == task["id"]:
                t["schedule_status"] = "scheduled"
                t["scheduled_at"] = event_start.isoformat()
                t["calendar_event_id"] = event.get("id")
                t["schedule_confirmation_message_id"] = confirm_resp.get("idMessage")
                tasks[i] = t
                break

    save_tasks(tasks)


# background contact refresh (every hour)
import threading, time as _time

def _contact_refresh_loop():
    while True:
        interval_mins = db_get("contacts_interval_mins", 60)
        try:
            interval_mins = int(interval_mins)
        except Exception:
            interval_mins = 60
        _time.sleep(max(1, interval_mins) * 60)
        fetch_and_save_contacts()

threading.Thread(target=_contact_refresh_loop, daemon=True).start()


# ── scheduled group sending (monthly WhatsApp button messages) ───────────────

def send_whatsapp_message(to_phone: str, text: str) -> dict:
    """Send a WhatsApp message via Twilio. If Twilio credentials aren't configured
    yet (e.g. while awaiting Meta business verification), logs a dry-run instead
    of failing — so the scheduling logic can be tested end-to-end in advance."""
    cfg = load_config()
    sid         = cfg.get("twilio_account_sid", "")
    token       = cfg.get("twilio_auth_token", "")
    from_number = cfg.get("twilio_whatsapp_from", "")
    if not (sid and token and from_number):
        log.info("Twilio not configured — dry-run send to %s: %s", to_phone, text[:120])
        return {"ok": True, "dry_run": True}
    try:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
        resp = http_requests.post(url, auth=(sid, token), data={
            "From": f"whatsapp:{from_number}",
            "To":   f"whatsapp:{to_phone}",
            "Body": text,
        }, timeout=15)
        resp.raise_for_status()
        return {"ok": True, "dry_run": False}
    except Exception as e:
        log.error("Failed to send WhatsApp message to %s: %s", to_phone, e)
        return {"ok": False, "error": str(e)}


def run_scheduled_group_sends(force: bool = False):
    """Check every group with a scheduled button-message and send it once per month
    on the configured day. `force=True` ignores the day-of-month / already-sent checks
    (used by the manual test trigger)."""
    today      = datetime.utcnow()
    month_key  = today.strftime("%Y-%m")
    groups     = load_groups()
    msgs_by_id = {m["id"]: m for m in load_button_messages()}
    contacts   = db_get("contacts", {})
    changed    = False

    for g in groups:
        if not g.get("button_message_id") or not g.get("send_day"):
            continue
        if not force:
            if g.get("last_sent_month") == month_key:
                continue
            if today.day != g["send_day"]:
                continue
        msg = msgs_by_id.get(g["button_message_id"])
        if not msg:
            continue
        for phone in g.get("members", []):
            plain_phone = phone.replace("@c.us", "")
            name = contacts.get(phone, "")
            text = (msg.get("text", "") or "").replace("{{שם}}", name or "")
            result = send_whatsapp_message(plain_phone, text)
            append_log({
                "time": datetime.utcnow().isoformat(), "channel": "whatsapp_cloud",
                "group_name": g.get("name", ""), "message_name": msg.get("name", ""),
                "to": plain_phone, "status": "sent" if result.get("ok") else "error",
                "dry_run": result.get("dry_run", False), "error": result.get("error", "")
            })
        if not force:
            g["last_sent_month"] = month_key
            changed = True

    if changed:
        save_groups(groups)


def _scheduled_send_loop():
    while True:
        _time.sleep(6 * 60 * 60)
        try:
            run_scheduled_group_sends()
        except Exception as e:
            log.error("Scheduled group send error: %s", e)

threading.Thread(target=_scheduled_send_loop, daemon=True).start()


def _poll_calendar_changes():
    """Detects manual changes to already-scheduled task events: a moved time is
    synced silently (no message — you already know you moved it), while a
    deleted event is ambiguous, so we ask once whether it means the task is
    done or needs rescheduling."""
    cfg = load_config()
    chat_id = cfg.get("personal_whatsapp_chat_id")
    if not chat_id:
        return
    calendar_id = cfg.get("business_calendar_id") or "primary"
    try:
        service = get_calendar_write_service()
    except Exception as e:
        log.warning("Could not get calendar service for polling: %s", e)
        return

    for t in load_tasks():
        if t.get("schedule_status") != "scheduled" or not t.get("calendar_event_id"):
            continue
        try:
            event = service.events().get(calendarId=calendar_id, eventId=t["calendar_event_id"]).execute()
        except HttpError as e:
            if e.resp.status == 404:
                q = green_api_send_message(
                    chat_id,
                    f"ראיתי שהסרת את '{(t.get('original_text') or '')[:60]}' מהיומן — בוצע או לתזמן מחדש?"
                )
                _update_task_fields(
                    t["id"],
                    pending_question_type="removed_confirmation",
                    pending_question_message_id=q.get("idMessage"),
                )
            else:
                log.warning("Could not check calendar event for task %s: %s", t["id"], e)
            continue
        except Exception as e:
            log.warning("Could not check calendar event for task %s: %s", t["id"], e)
            continue

        event_start_str = (event.get("start") or {}).get("dateTime")
        if not event_start_str:
            continue
        try:
            actual = datetime.fromisoformat(event_start_str)
            stored = datetime.fromisoformat(t["scheduled_at"]) if t.get("scheduled_at") else None
            if stored is None or actual != stored:
                _update_task_fields(t["id"], scheduled_at=actual.isoformat())
        except Exception:
            pass


def _scheduling_catchup_loop():
    """Retries scheduling for personal tasks left 'unscheduled' (e.g. no free
    slot was available last time) — the calendar may have opened up since.
    Also polls for manual calendar changes on already-scheduled tasks first."""
    while True:
        _time.sleep(60 * 60)
        try:
            _poll_calendar_changes()
        except Exception as e:
            log.error("Calendar polling error: %s", e)
        try:
            try_schedule_pending_tasks()
        except Exception as e:
            log.error("Scheduling catch-up error: %s", e)

threading.Thread(target=_scheduling_catchup_loop, daemon=True).start()


# ── webhook ───────────────────────────────────────────────────────────────────

_DONE_WORDS = {"בוצע", "נעשה", "סיימתי", "done"}


def _looks_like_done(text: str) -> bool:
    words = (text or "").strip().lower().split()
    return bool(words) and words[0] in _DONE_WORDS


def _handle_pending_question_reply(t: dict, text: str, chat_id: str):
    """Dispatches a reply to task t's pending_question_type: duration answer,
    'was the removed event done or should it be rescheduled', or a yes/no
    confirmation for a proposed reschedule/cancel."""
    qtype = t.get("pending_question_type")

    if qtype == "duration":
        minutes = parse_duration_minutes(text)
        if minutes is None and t.get("suggested_minutes") and _is_affirmative(text):
            minutes = t["suggested_minutes"]
        if minutes is None:
            green_api_send_message(chat_id, "לא הבנתי — תוכל לתת מספר דקות? (למשל 30)")
            return
        deadline = parse_deadline(text)
        fields = {
            "estimated_minutes": minutes, "schedule_status": "unscheduled",
            "pending_question_type": None, "pending_question_message_id": None,
        }
        if deadline:
            fields["deadline"] = deadline.isoformat()
        _update_task_fields(t["id"], **fields)
        deadline_note = f" עד {deadline.strftime('%d/%m')}" if deadline else ""
        green_api_send_message(chat_id, f"נרשם: {minutes} דקות{deadline_note}. אני אשבץ לך זמן ואעדכן.")
        try:
            try_schedule_pending_tasks()
        except Exception as e:
            log.error("Scheduling after estimate failed: %s", e)
        return

    if qtype == "removed_confirmation":
        _update_task_fields(t["id"], pending_question_type=None, pending_question_message_id=None)
        if _looks_like_done(text):
            _update_task_fields(t["id"], status="done", completed_at=datetime.utcnow().isoformat())
            green_api_send_message(chat_id, "נרשם כבוצע ✓")
        else:
            _update_task_fields(
                t["id"], schedule_status="unscheduled", calendar_event_id=None,
                scheduled_at=None, schedule_confirmation_message_id=None,
            )
            green_api_send_message(chat_id, "בסדר, אשבץ מחדש.")
            try:
                try_schedule_pending_tasks()
            except Exception as e:
                log.error("Rescheduling after removal failed: %s", e)
        return

    if qtype == "reschedule_confirm":
        target = t.get("pending_reschedule_target")
        _update_task_fields(
            t["id"], pending_question_type=None, pending_question_message_id=None,
            pending_reschedule_target=None,
        )
        if not _is_affirmative(text):
            green_api_send_message(chat_id, "בסדר, לא שיניתי כלום.")
            return
        if target == "CANCEL":
            _delete_calendar_event(t.get("calendar_event_id"))
            _update_task_fields(t["id"], schedule_status="cancelled", calendar_event_id=None)
            green_api_send_message(chat_id, "בוטל ונמחק מהיומן.")
            return
        try:
            new_start = datetime.fromisoformat(target)
            _move_scheduled_task(t, new_start)
            green_api_send_message(chat_id, f"הועבר ל-{new_start.strftime('%d/%m %H:%M')} ✓")
        except Exception as e:
            log.error("Failed to apply confirmed reschedule for task %s: %s", t["id"], e)
            green_api_send_message(chat_id, "לא הצלחתי להזיז את זה, נסה שוב מאוחר יותר.")
        return


def _handle_negotiation_reply(t: dict, text: str, chat_id: str):
    """Interprets a free-text reply quoting an already-scheduled task's
    confirmation message: confirm / reschedule / cancel. Only commits a real
    calendar change directly when the AI is confident about the exact new
    time — anything ambiguous gets a yes/no confirmation first."""
    result = parse_negotiation_action(t.get("original_text") or "", text)
    if not result:
        green_api_send_message(chat_id, "לא הצלחתי להבין — אפשר לנסח מחדש?")
        return

    action = result.get("action")
    if action == "confirm":
        green_api_send_message(chat_id, "בסדר, נשאר כמו שקבענו.")
        return

    if action == "cancel":
        q = green_api_send_message(
            chat_id, f"לבטל את '{(t.get('original_text') or '')[:60]}' ולמחוק מהיומן — לאשר?"
        )
        _update_task_fields(
            t["id"], pending_question_type="reschedule_confirm",
            pending_question_message_id=q.get("idMessage"), pending_reschedule_target="CANCEL",
        )
        return

    if action == "reschedule":
        when_iso = result.get("when_iso")
        new_start = None
        if when_iso:
            try:
                new_start = datetime.fromisoformat(when_iso)
                if new_start.tzinfo is None:
                    new_start = new_start.replace(tzinfo=ZoneInfo(get_timezone_name()))
            except Exception:
                new_start = None
        if new_start is None:
            green_api_send_message(chat_id, "לא הבנתי מתי בדיוק תרצה — תוכל לפרט תאריך/שעה?")
            return

        if result.get("confident"):
            try:
                _move_scheduled_task(t, new_start)
                green_api_send_message(chat_id, f"הועבר ל-{new_start.strftime('%d/%m %H:%M')} ✓")
            except Exception as e:
                log.error("Failed to reschedule task %s: %s", t["id"], e)
                green_api_send_message(chat_id, "לא הצלחתי להזיז את זה, נסה שוב מאוחר יותר.")
            return

        q = green_api_send_message(
            chat_id,
            f"להעביר את '{(t.get('original_text') or '')[:60]}' ל-{new_start.strftime('%d/%m %H:%M')} — לאשר?"
        )
        _update_task_fields(
            t["id"], pending_question_type="reschedule_confirm",
            pending_question_message_id=q.get("idMessage"), pending_reschedule_target=new_start.isoformat(),
        )
        return


def _handle_personal_text_message(payload, body, msg_data_outer, type_webhook):
    """Handle a plain (non-reaction) WhatsApp text from the user's own chat with
    the bot: either a reply-to-quote answering a pending duration question, or a
    work-hours command like 'שלישי 9 עד 20' / 'שבת סגור'.

    Accepts both directions (not just incoming): if personal_whatsapp_chat_id is
    WhatsApp's own "Message Yourself" self-chat, a reply you send is delivered
    by the very same account the GREEN API instance is connected to, so GREEN
    API reports it as outgoingMessageReceived rather than incoming — same
    reasoning the reaction handler above already applies for is_reaction.

    GREEN API is also inconsistent about the outer typeMessage for a quoted
    reply — observed values include "textMessage", "extendedTextMessage", and
    "quotedMessage" (with the *original* quoted message's own typeMessage/
    textMessageData nested one level deeper under quotedMessage). We don't gate
    on that value at all; we just try every known text/stanzaId path, and if
    nothing is found we log the full payload so the exact shape is visible
    without needing another blind guess."""
    if type_webhook not in ("incomingMessageReceived", "outgoingMessageReceived"):
        return
    type_message = msg_data_outer.get("typeMessage", "")
    if type_message == "reactionMessage":
        return

    cfg = load_config()
    personal_chat = cfg.get("personal_whatsapp_chat_id", "")
    chat_id = payload.get("senderData", {}).get("chatId", "") or body.get("senderData", {}).get("chatId", "")
    if not personal_chat or chat_id != personal_chat:
        return

    # GREEN API isn't consistent about where quotedMessage lives — try known paths
    # (same reasoning as the dual-path original_text extraction above in webhook()).
    quoted = (
        msg_data_outer.get("quotedMessage")
        or (msg_data_outer.get("extendedTextMessageData") or {}).get("quotedMessage")
        or {}
    )
    stanza_id = quoted.get("stanzaId", "")

    text = (
        (msg_data_outer.get("textMessageData") or {}).get("textMessage", "")
        or (msg_data_outer.get("extendedTextMessageData") or {}).get("text", "")
        or ""
    ).strip()
    if not text:
        log.warning(
            "Personal chat message: could not extract text (typeMessage=%r, stanza_id=%r) — full payload: %s",
            type_message, stanza_id, json.dumps(payload)
        )
        return
    log.info("Personal chat message: typeMessage=%r text=%r stanza_id=%r", type_message, text, stanza_id)

    if stanza_id:
        # (1) a reply to one of OUR pending questions — duration / removed-confirmation / reschedule-confirm
        for t in load_tasks():
            if t.get("pending_question_message_id") == stanza_id:
                _handle_pending_question_reply(t, text, chat_id)
                return

        # (2) a reply quoting a task's (persistent) scheduling confirmation — negotiation
        for t in load_tasks():
            if t.get("schedule_confirmation_message_id") == stanza_id and t.get("schedule_status") == "scheduled":
                _handle_negotiation_reply(t, text, chat_id)
                return

    wh = parse_work_hours_command(text)
    if wh:
        day_key, entry = wh
        if entry["active"] and entry["start"] >= entry["end"]:
            green_api_send_message(chat_id, "שעת הסיום חייבת להיות אחרי שעת ההתחלה — לא נשמר.")
            return
        work_hours = get_work_hours(cfg)
        work_hours[day_key] = entry
        cfg["work_hours"] = work_hours
        save_config_data(cfg)
        label = "סגור" if not entry["active"] else f"{entry['start']}–{entry['end']}"
        green_api_send_message(chat_id, f"עודכן: {label}")


def find_button_by_label(label: str):
    """Search all button-message drafts for a button matching the given label.
    Returns (message, button) or (None, None)."""
    for m in load_button_messages():
        for b in m.get("buttons", []):
            if b.get("label") == label:
                return m, b
    return None, None


@app.route("/twilio-webhook", methods=["POST"])
def twilio_webhook():
    """Twilio WhatsApp channel webhook — handles plain text replies and
    quick-reply button clicks, and triggers the configured email action."""
    from_number    = request.form.get("From", "")
    body_text      = request.form.get("Body", "")
    button_text    = request.form.get("ButtonText", "")
    button_payload = request.form.get("ButtonPayload", "")
    timestamp      = datetime.utcnow().isoformat()
    reply_label    = button_text or button_payload

    log.info("Twilio webhook: From=%r Body=%r ButtonText=%r ButtonPayload=%r",
             from_number, body_text, button_text, button_payload)

    if not reply_label:
        twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response><Message>קיבלתי ✓ — כתבת: {body_text}</Message></Response>"""
        return twiml, 200, {"Content-Type": "text/xml; charset=utf-8"}

    msg, button = find_button_by_label(reply_label)
    append_log({
        "time": timestamp, "channel": "whatsapp_cloud", "sender": from_number,
        "button_clicked": reply_label, "message_name": msg.get("name", "") if msg else "",
        "status": "button_reply" if msg else "button_reply_unmatched"
    })

    if button and button.get("email_to"):
        to_email = button["email_to"]
        employee_name = button.get("employee_name", "")
        ai_result = generate_email_content(
            from_number,
            f'הלקוח השיב "{reply_label}" להודעה "{msg.get("name", "")}"',
            employee_name
        )
        subject  = ai_result.get("subject", f"תגובת לקוח — {reply_label}")
        body_txt = ai_result.get("body", f"הלקוח {from_number} בחר: {reply_label}")
        try:
            send_gmail(to=to_email, subject=subject, body_text=body_txt)
            append_log({
                "time": timestamp, "channel": "whatsapp_cloud", "sender": from_number,
                "button_clicked": reply_label, "message_name": msg.get("name", ""),
                "email_to": to_email, "status": "sent"
            })
        except Exception as e:
            log.error("Failed to send email for button reply: %s", e)
            append_log({
                "time": timestamp, "channel": "whatsapp_cloud", "sender": from_number,
                "button_clicked": reply_label, "message_name": msg.get("name", ""),
                "email_to": to_email, "status": "error", "error": str(e)
            })

    twiml = """<?xml version="1.0" encoding="UTF-8"?>
<Response><Message>תודה רבה על תגובתך! 🙏</Message></Response>"""
    return twiml, 200, {"Content-Type": "text/xml; charset=utf-8"}


@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.get_json(silent=True) or {}
    log.info("Webhook received: %s", json.dumps(payload)[:3000])

    body = payload.get("body", {})
    type_ = payload.get("typeWebhook", "")

    # GREEN API: messageData can be at top level OR inside body
    msg_data_outer = payload.get("messageData", {}) or body.get("messageData", {})
    type_message = msg_data_outer.get("typeMessage", "")
    log.info("typeWebhook=%s typeMessage=%s", type_, type_message)
    is_reaction = (
        type_ in ("outgoingMessageReaction", "incomingMessageReaction")
        or type_message == "reactionMessage"
        or (type_ in ("outgoingMessageReceived", "incomingMessageReceived")
            and type_message == "reactionMessage")
    )
    if not is_reaction:
        try:
            _handle_personal_text_message(payload, body, msg_data_outer, type_)
        except Exception as e:
            log.error("Error handling personal text message: %s", e)
        return jsonify({"ok": True}), 200

    # extract reaction emoji (try all known paths)
    _reaction_field = body.get("reaction", {})
    reaction = (
        _reaction_field.get("reaction", "")
        if isinstance(_reaction_field, dict)
        else str(_reaction_field) if _reaction_field else ""
    )
    if not reaction:
        reaction = (
            msg_data_outer.get("reactionMessage", {}).get("reaction", "")
            or msg_data_outer.get("extendedTextMessageData", {}).get("text", "")
        )
    log.info("Reaction emoji detected: %r", reaction)
    msg_data   = body.get("messageData", {})
    sender      = payload.get("senderData", {}).get("sender", "") or body.get("senderData", {}).get("sender", "")
    chat_id     = payload.get("senderData", {}).get("chatId", "") or body.get("senderData", {}).get("chatId", "")
    # get contact name (the person in the chat, not the reactor)
    contact_name = get_contact_name(chat_id) if chat_id else ""
    sender_name  = contact_name or payload.get("senderData", {}).get("senderName", "") or body.get("senderData", {}).get("senderName", "")
    sender_name  = re.sub(r'^(לקוח|לקוחה)\s*|\s*(לקוח|לקוחה)$', '', sender_name).strip()
    # extract original message from webhook (quotedMessage contains the reacted-to message)
    quoted = msg_data_outer.get("quotedMessage", {})
    log.info("quotedMessage: %s", json.dumps(quoted)[:300])
    orig_text = (
        quoted.get("textMessage", "")
        or quoted.get("caption", "")
        or quoted.get("text", "")
        or (quoted.get("textMessageData") or {}).get("textMessage", "")
        or (quoted.get("extendedTextMessageData") or {}).get("text", "")
        or ""
    )
    log.info("orig_text from webhook: %r", orig_text[:100] if orig_text else "")
    if not orig_text and chat_id:
        stanza_id = quoted.get("stanzaId", "")
        orig_text = get_message_from_history(chat_id, stanza_id)
        log.info("Fetched from history (stanzaId=%r): %r", stanza_id, orig_text[:100] if orig_text else "")
    timestamp  = datetime.utcnow().isoformat()

    rules = load_rules()
    log.info("Rules loaded: %d rules. Looking for emoji: %r", len(rules), reaction)
    for r in rules:
        log.info("Rule emoji: %r active: %s match: %s", r.get("emoji"), r.get("active"), r.get("emoji") == reaction)
    matched = [r for r in rules if r.get("emoji") == reaction and r.get("active", True)]
    log.info("Matched rules: %d", len(matched))

    if not matched:
        append_log({
            "time": timestamp, "emoji": reaction, "sender": sender,
            "chat": chat_id, "original_text": orig_text,
            "status": "no_rule", "message": "No matching rule"
        })
        return jsonify({"ok": True}), 200

    cfg = load_config()
    personal_email = cfg.get("personal_email", "office@odcpa.co.il")
    personal_chat  = cfg.get("personal_whatsapp_chat_id", "")

    errors = []
    for rule in matched:
        to_email = rule.get("email_to", "")
        employee_name = rule.get("employee_name", "")
        ai_result = generate_email_content(sender_name or sender, orig_text, employee_name)
        subject   = ai_result.get("subject", f"{sender_name or sender} — הודעה חדשה")
        body_txt  = ai_result.get("body", orig_text)
        task_id    = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        complete_token = secrets.token_urlsafe(24)
        complete_url   = f"{request.host_url.rstrip('/')}/task-done/{task_id}?token={complete_token}"
        try:
            send_gmail(to=to_email, subject=subject, body_text=body_txt, complete_url=complete_url)
            append_log({
                "time": timestamp, "emoji": reaction, "sender": sender,
                "sender_name": sender_name, "chat": chat_id, "original_text": orig_text,
                "rule_name": rule.get("name", ""), "email_to": to_email, "status": "sent"
            })
            task_record = {
                "id": task_id,
                "time": timestamp, "emoji": reaction,
                "sender_name": sender_name or sender, "chat": chat_id,
                "original_text": orig_text, "rule_name": rule.get("name", ""),
                "employee_name": employee_name, "email_to": to_email,
                "subject": subject, "body": body_txt,
                "status": "pending", "completed_at": None,
                "complete_token": complete_token
            }
            is_mine = (to_email == personal_email)
            if is_mine:
                task_record.update({
                    "schedule_status": "awaiting_estimate",
                    "estimated_minutes": None,
                    "pending_question_type": None,
                    "pending_question_message_id": None,
                    "pending_reschedule_target": None,
                    "schedule_confirmation_message_id": None,
                    "scheduled_at": None,
                    "calendar_event_id": None,
                    "deadline": None,
                    "suggested_minutes": None,
                })
            append_task(task_record)
            if is_mine and personal_chat:
                suggested_minutes, suggested_reason = suggest_duration_minutes(
                    orig_text or subject, personal_email
                )
                if suggested_minutes:
                    reason_note = f" ({suggested_reason})" if suggested_reason else ""
                    q_text = (
                        f"נרשם: '{(orig_text or subject)[:120]}'. מעריך כ-{suggested_minutes} דק'{reason_note} — "
                        f"מאשר? (או תן מספר אחר, ואפשר גם דדליין כמו 'כן עד יום שלישי')"
                    )
                else:
                    q_text = (
                        f"נרשם: '{(orig_text or subject)[:120]}'. כמה זמן זה ייקח? "
                        f"(למשל: 15 / חצי שעה / שעה — אפשר גם דדליין, למשל: 30 דקות עד יום שלישי)"
                    )
                resp = green_api_send_message(personal_chat, q_text)
                if resp.get("idMessage"):
                    fields = {
                        "pending_question_type": "duration",
                        "pending_question_message_id": resp["idMessage"],
                    }
                    if suggested_minutes:
                        fields["suggested_minutes"] = suggested_minutes
                    _update_task_fields(task_id, **fields)
            log.info("Email sent to %s for reaction %s", to_email, reaction)
        except Exception as e:
            errors.append(str(e))
            append_log({
                "time": timestamp, "emoji": reaction, "sender": sender,
                "chat": chat_id, "original_text": orig_text,
                "rule_name": rule.get("name", ""), "email_to": to_email,
                "status": "error", "message": str(e)
            })
            log.error("Failed to send email: %s", e)

    return jsonify({"ok": not errors, "errors": errors}), 200


# ── REST API ──────────────────────────────────────────────────────────────────

@app.route("/api/rules", methods=["GET"])
def get_rules():
    return jsonify(load_rules())


@app.route("/api/rules", methods=["POST"])
def create_rule():
    rule = request.get_json()
    rule["id"] = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    rule.setdefault("active", True)
    rules = load_rules()
    rules.append(rule)
    save_rules(rules)
    return jsonify(rule), 201


@app.route("/api/rules/<rid>", methods=["PUT"])
def update_rule(rid):
    rules = load_rules()
    for i, r in enumerate(rules):
        if r["id"] == rid:
            rules[i] = {**r, **request.get_json(), "id": rid}
            save_rules(rules)
            return jsonify(rules[i])
    return jsonify({"error": "not found"}), 404


@app.route("/api/rules/<rid>", methods=["DELETE"])
def delete_rule(rid):
    rules = [r for r in load_rules() if r["id"] != rid]
    save_rules(rules)
    return jsonify({"ok": True})


@app.route("/api/log", methods=["GET"])
def get_log():
    return jsonify(load_log())


@app.route("/api/tasks", methods=["GET"])
def get_tasks():
    return jsonify(load_tasks())


@app.route("/api/tasks/<tid>", methods=["PUT"])
def update_task(tid):
    tasks = load_tasks()
    body = request.get_json() or {}
    for i, t in enumerate(tasks):
        if t["id"] == tid:
            if "status" in body:
                t["status"] = body["status"]
                t["completed_at"] = datetime.utcnow().isoformat() if body["status"] == "done" else None
                if body["status"] == "done" and t.get("calendar_event_id"):
                    _delete_calendar_event(t["calendar_event_id"])
            tasks[i] = t
            save_tasks(tasks)
            return jsonify(t)
    return jsonify({"error": "not found"}), 404


def _confirm_page(title: str, message: str) -> str:
    return f"""<!DOCTYPE html>
<html dir="rtl" lang="he"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title></head>
<body style="margin:0;padding:0;background:#f4f0f8;font-family:David,Arial,sans-serif;direction:rtl">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="min-height:100vh">
<tr><td align="center" valign="middle" style="padding:40px 16px">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:440px;background:#ffffff;border-radius:14px;overflow:hidden;border:1px solid #e0d0f0;box-shadow:0 4px 20px rgba(123,45,139,0.12)">
<tr><td align="center" style="padding:0;background:linear-gradient(135deg,#4a0a6b 0%,#1a0030 100%)">
<p style="margin:0;padding:26px 20px 22px;font-size:22px;font-weight:bold;color:#ffffff;text-align:center">אורן דולב — רואה חשבון</p>
</td></tr>
<tr><td style="padding:0;font-size:0;line-height:0"><div style="background:linear-gradient(90deg,#7B2D8B 0%,#c084d8 100%);height:5px"></div></td></tr>
<tr><td align="center" style="padding:36px 28px;text-align:center">
<p style="margin:0;font-size:18px;font-weight:bold;color:#4a0a6b">{title}</p>
<p style="margin:14px 0 0;font-size:15px;color:#2d2d2d;line-height:1.7">{message}</p>
</td></tr>
</table>
</td></tr>
</table>
</body></html>"""


@app.route("/task-done/<task_id>", methods=["GET"])
def task_done(task_id):
    token = request.args.get("token", "")
    tasks = load_tasks()
    for i, t in enumerate(tasks):
        if t["id"] == task_id:
            if not token or not secrets.compare_digest(token, t.get("complete_token", "")):
                return _confirm_page("קישור לא תקין", "הקישור שגוי או שאינו תואם למשימה."), 403
            if t.get("status") == "done":
                return _confirm_page("המשימה כבר סומנה כבוצעה ✓",
                                     "המשימה כבר סומנה כבוצעה בעבר. תודה רבה!")
            t["status"] = "done"
            t["completed_at"] = datetime.utcnow().isoformat()
            if t.get("calendar_event_id"):
                _delete_calendar_event(t["calendar_event_id"])
            tasks[i] = t
            save_tasks(tasks)
            return _confirm_page("המשימה סומנה כבוצעה ✓", "תודה! העדכון נשמר במערכת ואורן יראה אותו בפאנל הניהול.")
    return _confirm_page("המשימה לא נמצאה", "ייתכן שהמשימה נמחקה מהמערכת."), 404


@app.route("/api/config", methods=["GET"])
def get_config():
    cfg = load_config()
    if cfg.get("green_api_token"):
        cfg["green_api_token"] = cfg["green_api_token"][:6] + "***"
    return jsonify(cfg)


@app.route("/api/config", methods=["POST"])
def save_config():
    data = request.get_json()
    cfg = load_config()
    if data.get("green_api_token", "").endswith("***"):
        data["green_api_token"] = cfg.get("green_api_token", "")
    cfg.update(data)
    save_config_data(cfg)
    return jsonify({"ok": True})


@app.route("/api/work-hours", methods=["GET"])
def get_work_hours_api():
    """Always returns all 7 days merged with defaults — same values the
    scheduling engine and the WhatsApp work-hours commands actually use, so the
    dashboard never shows a day as blank just because it was never touched."""
    return jsonify(get_work_hours())


@app.route("/api/work-hours", methods=["POST"])
def save_work_hours_api():
    data = request.get_json() or {}
    cfg = load_config()
    work_hours = get_work_hours(cfg)
    rejected = []
    for day, entry in data.items():
        if day not in work_hours or not isinstance(entry, dict):
            continue
        candidate = {
            "active": bool(entry.get("active", work_hours[day]["active"])),
            "start": entry.get("start") or work_hours[day]["start"],
            "end": entry.get("end") or work_hours[day]["end"],
        }
        if candidate["active"] and candidate["start"] >= candidate["end"]:
            rejected.append(day)
            continue
        work_hours[day] = candidate
    cfg["work_hours"] = work_hours
    save_config_data(cfg)
    if rejected:
        return jsonify({
            "ok": False, "work_hours": work_hours, "rejected": rejected,
            "error": "שעת הסיום חייבת להיות אחרי שעת ההתחלה — לא נשמר עבור: " + ", ".join(rejected),
        }), 400
    return jsonify({"ok": True, "work_hours": work_hours})


_railway_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN")


def _redirect_uri(callback_path: str) -> str:
    return (
        f"https://{_railway_domain}{callback_path}"
        if _railway_domain
        else f"http://localhost:5000{callback_path}"
    )


GMAIL_REDIRECT_URI    = _redirect_uri("/api/gmail/callback")
CALENDAR_REDIRECT_URI = _redirect_uri("/api/calendar/callback")

_oauth_flow: dict = {}          # Gmail OAuth in-flight flow
_oauth_flow_calendar: dict = {}  # Calendar OAuth in-flight flow — separate Google account


@app.route("/api/gmail/auth-url", methods=["GET"])
def gmail_auth_url():
    if not GOOGLE_LIBS:
        return jsonify({"error": "google-auth not installed"}), 500
    client_secrets = DATA_DIR / "client_secrets.json"
    if not client_secrets.exists():
        return jsonify({"error": "client_secrets.json not found in data/"}), 400
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    flow = Flow.from_client_secrets_file(
        str(client_secrets), GMAIL_SCOPES,
        redirect_uri=GMAIL_REDIRECT_URI
    )
    auth_url, state = flow.authorization_url(access_type="offline", prompt="consent")
    _oauth_flow["flow"] = flow
    return jsonify({"url": auth_url})


@app.route("/api/gmail/callback")
def gmail_callback():
    if not GOOGLE_LIBS:
        return "google-auth not installed", 500
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    flow = _oauth_flow.get("flow")
    if not flow:
        return "Session expired, please try again.", 400
    flow.fetch_token(authorization_response=request.url)
    creds = flow.credentials
    save_gmail_token(json.loads(creds.to_json()))
    _oauth_flow.clear()
    return """<html><body style="font-family:sans-serif;text-align:center;padding:60px">
    <h2>✅ Gmail connected!</h2><p>You can close this tab.</p></body></html>"""


@app.route("/api/gmail/status", methods=["GET"])
def gmail_status():
    load_gmail_token()
    if not TOKEN_FILE.exists():
        return jsonify({"connected": False})
    try:
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), GMAIL_SCOPES)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_gmail_token(json.loads(creds.to_json()))
        return jsonify({"connected": creds.valid})
    except Exception:
        return jsonify({"connected": False})


@app.route("/api/gmail/disconnect", methods=["POST"])
def gmail_disconnect():
    if TOKEN_FILE.exists():
        TOKEN_FILE.unlink()
    if DATABASE_URL and POSTGRES_AVAILABLE:
        db_set("gmail_token", None)
    return jsonify({"ok": True})


# ── Google Calendar OAuth — deliberately a separate connection from Gmail;   ──
# ── the calendars you want to schedule against may live in a different      ──
# ── Google account than the one Gmail sends from.                          ──

@app.route("/api/calendar/auth-url", methods=["GET"])
def calendar_auth_url():
    if not GOOGLE_LIBS:
        return jsonify({"error": "google-auth not installed"}), 500
    client_secrets = DATA_DIR / "client_secrets.json"
    if not client_secrets.exists():
        return jsonify({"error": "client_secrets.json not found in data/"}), 400
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    flow = Flow.from_client_secrets_file(
        str(client_secrets), CALENDAR_SCOPES,
        redirect_uri=CALENDAR_REDIRECT_URI
    )
    auth_url, state = flow.authorization_url(access_type="offline", prompt="consent")
    _oauth_flow_calendar["flow"] = flow
    return jsonify({"url": auth_url})


@app.route("/api/calendar/callback")
def calendar_callback():
    if not GOOGLE_LIBS:
        return "google-auth not installed", 500
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    flow = _oauth_flow_calendar.get("flow")
    if not flow:
        return "Session expired, please try again.", 400
    flow.fetch_token(authorization_response=request.url)
    creds = flow.credentials
    save_calendar_token(json.loads(creds.to_json()))
    _oauth_flow_calendar.clear()
    return """<html><body style="font-family:sans-serif;text-align:center;padding:60px">
    <h2>✅ Google Calendar connected!</h2><p>You can close this tab.</p></body></html>"""


@app.route("/api/calendar/status", methods=["GET"])
def calendar_status():
    load_calendar_token()
    if not CALENDAR_TOKEN_FILE.exists():
        return jsonify({"connected": False})
    try:
        creds = Credentials.from_authorized_user_file(str(CALENDAR_TOKEN_FILE), CALENDAR_SCOPES)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_calendar_token(json.loads(creds.to_json()))
        return jsonify({"connected": creds.valid})
    except Exception:
        return jsonify({"connected": False})


@app.route("/api/calendar/disconnect", methods=["POST"])
def calendar_disconnect():
    if CALENDAR_TOKEN_FILE.exists():
        CALENDAR_TOKEN_FILE.unlink()
    if DATABASE_URL and POSTGRES_AVAILABLE:
        db_set("calendar_token", None)
    return jsonify({"ok": True})


@app.route("/api/calendars", methods=["GET"])
def list_calendars():
    """All calendars visible in the connected Calendar account, so the dashboard
    can offer a picker instead of making the user type in raw calendar IDs."""
    try:
        service = get_calendar_service()
        result = service.calendarList().list().execute()
        calendars = [
            {
                "id": c["id"],
                "summary": c.get("summary", c["id"]),
                "primary": c.get("primary", False),
            }
            for c in result.get("items", [])
        ]
        return jsonify({"ok": True, "calendars": calendars})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/contacts/interval", methods=["POST"])
def set_contacts_interval():
    data = request.get_json()
    mins = int(data.get("mins", 60))
    db_set("contacts_interval_mins", mins)
    return jsonify({"ok": True, "mins": mins})


@app.route("/api/contacts/interval", methods=["GET"])
def get_contacts_interval():
    mins = db_get("contacts_interval_mins", 60)
    return jsonify({"mins": mins})


@app.route("/api/contacts/refresh", methods=["POST"])
def contacts_refresh():
    contacts = fetch_and_save_contacts()
    updated = db_get("contacts_updated", "")
    return jsonify({"ok": True, "count": len(contacts), "updated": updated})


@app.route("/api/contacts/status", methods=["GET"])
def contacts_status():
    contacts = db_get("contacts", {})
    updated  = db_get("contacts_updated", "")
    return jsonify({"count": len(contacts), "updated": updated})


@app.route("/api/contacts", methods=["GET"])
def get_contacts():
    contacts = db_get("contacts", {})
    return jsonify([{"phone": phone, "name": name} for phone, name in contacts.items()])


@app.route("/api/groups", methods=["GET"])
def get_groups():
    return jsonify(load_groups())


@app.route("/api/groups", methods=["POST"])
def create_group():
    body = request.get_json() or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    group = {
        "id": datetime.utcnow().strftime("%Y%m%d%H%M%S%f"),
        "name": name,
        "members": body.get("members") or [],
        "button_message_id": body.get("button_message_id") or "",
        "send_day": body.get("send_day") or None,
        "updated_at": datetime.utcnow().isoformat(),
    }
    groups = load_groups()
    groups.insert(0, group)
    save_groups(groups)
    return jsonify(group)


@app.route("/api/groups/<gid>", methods=["PUT"])
def update_group(gid):
    body = request.get_json() or {}
    groups = load_groups()
    for i, g in enumerate(groups):
        if g["id"] == gid:
            if "name" in body:
                g["name"] = (body["name"] or "").strip()
            if "members" in body:
                g["members"] = body["members"] or []
            if "button_message_id" in body:
                g["button_message_id"] = body["button_message_id"] or ""
            if "send_day" in body:
                g["send_day"] = body["send_day"] or None
            g["updated_at"] = datetime.utcnow().isoformat()
            groups[i] = g
            save_groups(groups)
            return jsonify(g)
    return jsonify({"error": "not found"}), 404


@app.route("/api/groups/<gid>", methods=["DELETE"])
def delete_group(gid):
    groups = load_groups()
    new_groups = [g for g in groups if g["id"] != gid]
    if len(new_groups) == len(groups):
        return jsonify({"error": "not found"}), 404
    save_groups(new_groups)
    return jsonify({"ok": True})


@app.route("/api/button-messages", methods=["GET"])
def get_button_messages():
    return jsonify(load_button_messages())


def _parse_buttons(raw):
    """Normalize incoming button definitions into {label, email_to, employee_name} dicts."""
    buttons = []
    for b in (raw or [])[:3]:
        label = (b.get("label") or "").strip()
        if not label:
            continue
        buttons.append({
            "label": label,
            "email_to": (b.get("email_to") or "").strip(),
            "employee_name": (b.get("employee_name") or "").strip(),
        })
    return buttons


@app.route("/api/button-messages", methods=["POST"])
def create_button_message():
    body = request.get_json() or {}
    name = (body.get("name") or "").strip()
    text = (body.get("text") or "").strip()
    buttons = _parse_buttons(body.get("buttons"))
    if not name or not text or not buttons:
        return jsonify({"error": "name, text and at least one button are required"}), 400
    msg = {
        "id": datetime.utcnow().strftime("%Y%m%d%H%M%S%f"),
        "name": name,
        "text": text,
        "buttons": buttons,
        "updated_at": datetime.utcnow().isoformat(),
    }
    msgs = load_button_messages()
    msgs.insert(0, msg)
    save_button_messages(msgs)
    return jsonify(msg)


@app.route("/api/button-messages/<mid>", methods=["PUT"])
def update_button_message(mid):
    body = request.get_json() or {}
    msgs = load_button_messages()
    for i, m in enumerate(msgs):
        if m["id"] == mid:
            if "name" in body:
                m["name"] = (body["name"] or "").strip()
            if "text" in body:
                m["text"] = (body["text"] or "").strip()
            if "buttons" in body:
                m["buttons"] = _parse_buttons(body["buttons"])
            m["updated_at"] = datetime.utcnow().isoformat()
            msgs[i] = m
            save_button_messages(msgs)
            return jsonify(m)
    return jsonify({"error": "not found"}), 404


@app.route("/api/button-messages/<mid>", methods=["DELETE"])
def delete_button_message(mid):
    msgs = load_button_messages()
    new_msgs = [m for m in msgs if m["id"] != mid]
    if len(new_msgs) == len(msgs):
        return jsonify({"error": "not found"}), 404
    save_button_messages(new_msgs)
    return jsonify({"ok": True})


@app.route("/api/groups/test-send", methods=["POST"])
def groups_test_send():
    """Manually trigger the scheduled-send check, ignoring the day-of-month and
    once-per-month guards — useful for testing the flow without waiting."""
    try:
        run_scheduled_group_sends(force=True)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/green/test", methods=["POST"])
def green_test():
    cfg = load_config()
    if not cfg.get("green_instance_id") or not cfg.get("green_api_token"):
        return jsonify({"ok": False, "error": "GREEN API not configured"}), 400
    try:
        url = green_api_url(cfg, "getStateInstance")
        r = http_requests.get(url, timeout=10)
        data = r.json()
        return jsonify({"ok": True, "state": data.get("stateInstance", "unknown")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── static files ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/favicon.ico")
def favicon():
    return send_from_directory("static", "favicon.ico", mimetype="image/x-icon")


@app.route("/<path:path>")
def static_files(path):
    return send_from_directory("static", path)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("RAILWAY_ENVIRONMENT") is None
    app.run(host="0.0.0.0", debug=debug, port=port)
