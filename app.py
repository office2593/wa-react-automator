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


def send_gmail(to: str, subject: str, body: str, sender_name: str = ""):
    service = get_gmail_service()
    msg = MIMEMultipart("alternative")
    msg["To"]      = to
    msg["Subject"] = subject
    msg["From"]    = sender_name or "me"
    msg.attach(MIMEText(body, "html" if "<" in body else "plain", "utf-8"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()


# ── GREEN API helper ──────────────────────────────────────────────────────────

def green_api_url(config, method):
    return (
        f"https://api.green-api.com/waInstance{config['green_instance_id']}"
        f"/{method}/{config['green_api_token']}"
    )


# ── webhook ───────────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.get_json(silent=True) or {}
    log.info("Webhook received: %s", json.dumps(payload)[:300])

    body = payload.get("body", {})
    type_ = payload.get("typeWebhook", "")

    # detect reaction webhooks (GREEN API sends several formats)
    msg_data_outer = body.get("messageData", {})
    type_message = msg_data_outer.get("typeMessage", "")
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
    sender     = body.get("senderData", {}).get("sender", "")
    chat_id    = body.get("senderData", {}).get("chatId", "")
    orig_text  = (
        msg_data.get("extendedTextMessageData", {}).get("text", "")
        or msg_data.get("textMessageData", {}).get("textMessage", "")
        or msg_data.get("quotedMessage", {}).get("textMessage", "")
        or ""
    )
    timestamp  = datetime.utcnow().isoformat()

    rules = load_rules()
    matched = [r for r in rules if r.get("emoji") == reaction and r.get("active", True)]

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
        subject  = rule.get("email_subject", f"WhatsApp reaction {reaction}")
        template = rule.get("email_body", "")
        body_txt = (
            template
            .replace("{{emoji}}", reaction)
            .replace("{{sender}}", sender)
            .replace("{{chat}}", chat_id)
            .replace("{{message}}", orig_text)
            .replace("{{time}}", timestamp)
        )
        try:
            send_gmail(to_email, subject, body_txt)
            append_log({
                "time": timestamp, "emoji": reaction, "sender": sender,
                "chat": chat_id, "original_text": orig_text,
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
