"""
WhatsApp Reaction → Gmail Automator
Flask backend: receives GREEN API webhooks, applies rules, sends Gmail.
Data stored in PostgreSQL (Railway) or local JSON files (development).
"""
import json
import os
import base64
import logging
from datetime import datetime
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
CONFIG_FILE = DATA_DIR / "config.json"
TOKEN_FILE  = DATA_DIR / "gmail_token.json"

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

DATABASE_URL = os.environ.get("DATABASE_URL")


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


# ── Gmail send ────────────────────────────────────────────────────────────────

def get_gmail_service():
    if not GOOGLE_LIBS:
        raise RuntimeError("google-auth libraries not installed")
    load_gmail_token()
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_gmail_token(json.loads(creds.to_json()))
        else:
            raise RuntimeError("Gmail not authorized — please connect via the dashboard")
    return build("gmail", "v1", credentials=creds)


def build_email_html(body_text: str) -> str:
    """Build Outlook-compatible HTML email with David font and purple gradient design."""
    msg = (body_text or "").replace("\n", "<br>")
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
    <td align="center" bgcolor="#2d0050" style="padding:30px 24px;background:linear-gradient(135deg,#4a0a6b 0%,#1a0030 100%);text-align:center">
      <!--[if mso]>
      <v:rect xmlns:v="urn:schemas-microsoft-com:vml" fill="true" stroke="false" style="width:552px;height:130px;display:block">
      <v:fill type="gradient" color="#4a0a6b" color2="#1a0030" angle="90"/>
      <v:textbox inset="0,0,0,0" style="mso-fit-shape-to-text:false">
      <table width="552" height="130" cellpadding="0" cellspacing="0" border="0"><tr><td align="center" valign="middle" style="padding:0">
      <p style="margin:0;font-size:26px;font-weight:bold;color:#ffffff;font-family:David,Arial,sans-serif;text-align:center">אורן דולב</p>
      <p style="margin:8px 0 0 0;font-size:15px;color:#c084d8;font-family:David,Arial,sans-serif;text-align:center;letter-spacing:2px">רואה חשבון</p>
      </td></tr></table>
      </v:textbox></v:rect>
      <![endif]-->
      <!--[if !mso]><!-->
      <div style="font-size:26px;font-weight:bold;color:#ffffff;font-family:David,Arial,sans-serif;text-align:center;direction:rtl;line-height:1.2">אורן דולב</div>
      <div style="font-size:15px;color:#c084d8;font-family:David,Arial,sans-serif;text-align:center;letter-spacing:2px;direction:rtl;margin-top:8px;line-height:1.2">רואה חשבון</div>
      <!--<![endif]-->
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

  <!-- DIVIDER -->
  <tr>
    <td style="padding:0 32px;font-size:0;line-height:0;height:1px;background:#e0d0f0">&nbsp;</td>
  </tr>

  <!-- FOOTER -->
  <tr>
    <td align="center" bgcolor="#1a0030" style="padding:16px 24px;background:linear-gradient(135deg,#1a0030 0%,#4a0a6b 100%);text-align:center">
      <!--[if mso]>
      <v:rect xmlns:v="urn:schemas-microsoft-com:vml" fill="true" stroke="false" style="width:552px;height:48px;display:block">
      <v:fill type="gradient" color="#1a0030" color2="#4a0a6b" angle="135"/>
      <v:textbox inset="0,14pt,0,14pt" style="mso-fit-shape-to-text:false">
      <table width="552" cellpadding="0" cellspacing="0" border="0"><tr><td align="center" valign="middle">
      <p style="margin:0;font-size:12px;color:#c084d8;font-family:David,Arial,sans-serif;text-align:center">אורן דולב &#8212; רואה חשבון &nbsp;|&nbsp; נשלח אוטומטית ממערכת WhatsApp</p>
      </td></tr></table>
      </v:textbox></v:rect>
      <![endif]-->
      <!--[if !mso]><!-->
      <div style="font-size:13px;color:#c084d8;font-family:David,Arial,sans-serif;text-align:center;direction:rtl;line-height:1.5">אורן דולב — רואה חשבון</div>
      <div style="font-size:12px;color:#c084d8;font-family:David,Arial,sans-serif;text-align:center;direction:rtl;line-height:1.5;margin-top:4px">נשלח אוטומטית ממערכת WhatsApp</div>
      <!--<![endif]-->
    </td>
  </tr>

</table>

</td></tr>
</table>
</body>
</html>"""


def send_gmail(to: str, subject: str, body_text: str):
    service = get_gmail_service()
    msg = MIMEMultipart("alternative")
    msg["To"]      = to
    msg["Subject"] = subject
    msg["From"]    = "me"

    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    msg.attach(MIMEText(build_email_html(body_text), "html", "utf-8"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()


# ── GREEN API helper ──────────────────────────────────────────────────────────

def green_api_url(config, method):
    return (
        f"https://api.green-api.com/waInstance{config['green_instance_id']}"
        f"/{method}/{config['green_api_token']}"
    )


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


# ── webhook ───────────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.get_json(silent=True) or {}
    log.info("Webhook received: %s", json.dumps(payload)[:500])

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

    errors = []
    for rule in matched:
        to_email = rule.get("email_to", "")
        employee_name = rule.get("employee_name", "")
        ai_result = generate_email_content(sender_name or sender, orig_text, employee_name)
        subject   = ai_result.get("subject", f"{sender_name or sender} — הודעה חדשה")
        body_txt  = ai_result.get("body", orig_text)
        try:
            send_gmail(to=to_email, subject=subject, body_text=body_txt)
            append_log({
                "time": timestamp, "emoji": reaction, "sender": sender,
                "sender_name": sender_name, "chat": chat_id, "original_text": orig_text,
                "rule_name": rule.get("name", ""), "email_to": to_email, "status": "sent"
            })
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


_railway_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
REDIRECT_URI = (
    f"https://{_railway_domain}/api/gmail/callback"
    if _railway_domain
    else "http://localhost:5000/api/gmail/callback"
)

_oauth_flow: dict = {}


@app.route("/api/gmail/auth-url", methods=["GET"])
def gmail_auth_url():
    if not GOOGLE_LIBS:
        return jsonify({"error": "google-auth not installed"}), 500
    client_secrets = DATA_DIR / "client_secrets.json"
    if not client_secrets.exists():
        return jsonify({"error": "client_secrets.json not found in data/"}), 400
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    flow = Flow.from_client_secrets_file(
        str(client_secrets), SCOPES,
        redirect_uri=REDIRECT_URI
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
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
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
