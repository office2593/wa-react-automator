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


def build_email_html(emoji: str, subject: str, sender_name: str, timestamp: str, message: str, logo_url: str) -> str:
    """Build Outlook-compatible HTML email. In the future, AI can enhance subject/message here."""
    ts_formatted = timestamp.replace("T", " ")[:19]
    sn   = sender_name or "לא ידוע"
    msg  = (message or "(אין הודעה מקורית)").replace("\n", "<br>")

    return f"""<!DOCTYPE html>
<html dir="rtl" lang="he" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<!--[if mso]>
<noscript><xml><o:OfficeDocumentSettings><o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml></noscript>
<![endif]-->
<style>
  body,table,td{{margin:0;padding:0;font-family:Arial,sans-serif;direction:rtl}}
  img{{border:0;display:block}}
</style>
</head>
<body style="background:#f4f0f8;margin:0;padding:20px 0">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f4f0f8">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" border="0" style="max-width:600px;background:#ffffff;border:1px solid #e0d0f0">

  <!-- HEADER -->
  <tr>
    <td align="center" bgcolor="#4a0a6b" style="background:#4a0a6b;padding:28px 24px">
      <!--[if mso]>
      <table><tr><td style="background:#4a0a6b">
      <![endif]-->
      <img src="{logo_url}" width="140" alt="אורן דולב" style="display:block;margin:0 auto;max-width:140px;height:auto" />
      <!--[if mso]></td></tr></table><![endif]-->
    </td>
  </tr>

  <!-- ACCENT BAR -->
  <tr>
    <td height="5" bgcolor="#7B2D8B" style="background:#7B2D8B;font-size:0;line-height:0">&nbsp;</td>
  </tr>

  <!-- EMOJI + TITLE -->
  <tr>
    <td align="center" style="padding:28px 24px 12px;background:#ffffff">
      <table cellpadding="0" cellspacing="0" border="0" align="center">
        <tr>
          <td align="center" bgcolor="#f8f4fc" width="60" height="60"
              style="background:#f8f4fc;border:2px solid #c084d8;border-radius:30px;font-size:30px;text-align:center;line-height:60px;padding:0 8px">
            {emoji}
          </td>
        </tr>
        <tr>
          <td align="center" style="padding-top:12px;font-size:20px;font-weight:bold;color:#1a0030;font-family:Arial,sans-serif">
            {subject}
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- BODY -->
  <tr>
    <td style="padding:8px 32px 24px;background:#ffffff">

      <!-- מאת -->
      <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:16px">
        <tr>
          <td style="font-size:11px;font-weight:bold;color:#7B2D8B;text-transform:uppercase;letter-spacing:1px;padding-bottom:5px;font-family:Arial,sans-serif">
            מאת
          </td>
        </tr>
        <tr>
          <td bgcolor="#f8f4fc" style="background:#f8f4fc;border-right:4px solid #7B2D8B;padding:10px 14px;font-size:15px;color:#1a1a2e;font-family:Arial,sans-serif">
            {sn}
          </td>
        </tr>
      </table>

      <!-- תאריך -->
      <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:16px">
        <tr>
          <td style="font-size:11px;font-weight:bold;color:#7B2D8B;text-transform:uppercase;letter-spacing:1px;padding-bottom:5px;font-family:Arial,sans-serif">
            תאריך ושעה
          </td>
        </tr>
        <tr>
          <td bgcolor="#f8f4fc" style="background:#f8f4fc;border-right:4px solid #7B2D8B;padding:10px 14px;font-size:15px;color:#1a1a2e;font-family:Arial,sans-serif">
            {ts_formatted}
          </td>
        </tr>
      </table>

      <!-- הודעה -->
      <table width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
          <td style="font-size:11px;font-weight:bold;color:#7B2D8B;text-transform:uppercase;letter-spacing:1px;padding-bottom:5px;font-family:Arial,sans-serif">
            הודעה מקורית
          </td>
        </tr>
        <tr>
          <td bgcolor="#f0e8f8" style="background:#f0e8f8;padding:16px;font-size:15px;color:#2d2d2d;line-height:1.7;font-family:Arial,sans-serif">
            {msg}
          </td>
        </tr>
      </table>

    </td>
  </tr>

  <!-- FOOTER -->
  <tr>
    <td align="center" bgcolor="#1a0030" style="background:#1a0030;padding:16px;font-size:12px;color:#c084d8;font-family:Arial,sans-serif">
      אורן דולב — רואה חשבון &nbsp;|&nbsp; נשלח אוטומטית ממערכת WhatsApp
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def send_gmail(to: str, subject: str, body: str, sender_name: str = "",
               emoji: str = "", timestamp: str = "", message: str = "", logo_url: str = ""):
    service = get_gmail_service()
    msg = MIMEMultipart("alternative")
    msg["To"]      = to
    msg["Subject"] = subject
    msg["From"]    = "me"

    # plain text fallback
    plain = f"{subject}\nמאת: {sender_name}\nתאריך: {timestamp}\n\n{message}"
    msg.attach(MIMEText(plain, "plain", "utf-8"))

    # HTML version
    html_body = build_email_html(emoji, subject, sender_name, timestamp, message, logo_url)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

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


def get_original_message(chat_id: str, message_id: str) -> str:
    """Fetch original message text from GREEN API. Returns empty string on any failure."""
    if not chat_id or not message_id:
        return ""
    try:
        cfg = load_config()
        if not cfg.get("green_instance_id") or not cfg.get("green_api_token"):
            return ""
        url = green_api_url(cfg, "getChatHistory")
        r = http_requests.post(url, json={"chatId": chat_id, "count": 50}, timeout=10)
        messages = r.json()
        if not isinstance(messages, list):
            return ""
        for msg in messages:
            if msg.get("idMessage") == message_id:
                md = msg.get("messageData", {})
                return (
                    md.get("textMessageData", {}).get("textMessage", "")
                    or md.get("extendedTextMessageData", {}).get("text", "")
                    or md.get("imageMessageData", {}).get("caption", "")
                    or md.get("documentMessageData", {}).get("caption", "")
                    or ""
                )
        return ""
    except Exception as e:
        log.warning("Could not fetch original message: %s", e)
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
    # try to get original message text from webhook first, then fetch from GREEN API
    orig_text = (
        msg_data.get("extendedTextMessageData", {}).get("text", "")
        or msg_data.get("textMessageData", {}).get("textMessage", "")
        or msg_data.get("quotedMessage", {}).get("textMessage", "")
        or ""
    )
    if not orig_text:
        reaction_msg = msg_data_outer.get("reactionMessage", {})
        log.info("reactionMessage fields: %s", json.dumps(reaction_msg)[:300])
        reacted_msg_id = (
            reaction_msg.get("messageId", "")
            or reaction_msg.get("idMessage", "")
            or reaction_msg.get("stanzaId", "")
        )
        log.info("reacted_msg_id: %r", reacted_msg_id)
        if reacted_msg_id:
            orig_text = get_original_message(chat_id, reacted_msg_id)
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
        raw_subject = rule.get("email_subject", "{{sender}} — משימה חדשה")
        subject = raw_subject.replace("{{sender}}", sender_name or sender)

        def fill(t):
            return (t or "").replace("{{emoji}}", reaction).replace("{{sender}}", sender_name or sender).replace("{{chat}}", chat_id).replace("{{message}}", orig_text).replace("{{time}}", timestamp)

        # use pre-built HTML template from rule if available, else build from body
        railway_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
        logo_url = f"https://{railway_domain}/logo.png" if railway_domain else "/logo.png"
        html_template = rule.get("email_html_template", "")
        if html_template:
            body_txt = fill(html_template).replace("{{logo_url}}", logo_url)
        else:
            body_txt = fill(rule.get("email_body", ""))
        try:
            send_gmail(
                to=to_email, subject=subject, body=body_txt,
                sender_name=sender_name, emoji=reaction,
                timestamp=timestamp, message=orig_text, logo_url=logo_url
            )
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


@app.route("/<path:path>")
def static_files(path):
    return send_from_directory("static", path)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("RAILWAY_ENVIRONMENT") is None
    app.run(host="0.0.0.0", debug=debug, port=port)
