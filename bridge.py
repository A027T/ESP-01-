#!/usr/bin/env python3
"""
ESP agent bridge v2  -  Telegram <-> GitHub <-> WeMos D1 serial, with an AI agent.

Code lives in this (public, open-source) repo. ALL memory lives in the private data repo as plain files:
  agent/state.json   aliases, fast commands, channels (sensor parsers), rules, schedules, buttons
  agent/data.json    sensor history
  agent/brain.md     free-text notes/algorithms the agent writes for itself (human readable)
  agent/keys.json    AI API keys (added with /addkey, automatic failover between them)
  inbox.txt          records for the board:  M=serial text  S=system text  C=control command  A=silent ack

Design rule: the AI only UNDERSTANDS and PLANS (JSON actions). Everything that must be exact
(rules, schedules, math, serial output) is executed by plain deterministic code below.
Message contents are never printed to the (public) Actions log.
"""
import ast
import base64
import json
import math
import os
import re
import signal
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

# ----------------------------------------------------------------- config
TG_TOKEN = os.environ["TG_TOKEN"].strip()
TG_CHAT = os.environ.get("TG_CHAT_ID", "").strip()
DATA_PAT = os.environ["DATA_PAT"].strip()
DATA_REPO = os.environ["DATA_REPO"].strip()
OUTBOX_ISSUE = int(os.environ.get("OUTBOX_ISSUE", "1"))
INBOX_PATH = os.environ.get("INBOX_PATH", "inbox.txt")
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", str(5 * 3600 + 50 * 60)))
GH_POLL = float(os.environ.get("GH_POLL_SECONDS", "2"))
SELF_RESTART = os.environ.get("SELF_RESTART", "1") == "1"
TZ_NAME = os.environ.get("TZ_NAME", "Asia/Tehran")
AI_MAX_TOKENS = int(os.environ.get("AI_MAX_TOKENS", "1500"))
OWNER = DATA_REPO.split("/")[0].lower()
ALLOWED = {u.strip().lower() for u in os.environ.get("ALLOWED_GH_USERS", OWNER).split(",") if u.strip()}

MAX_RECORDS = 30
MAX_INBOX_BYTES = 6000
MAX_SERIAL_TEXT = 480
ESP_TAG = "@@ESP@@"
API = "https://api.github.com"
TG = f"https://api.telegram.org/bot{TG_TOKEN}"

S = requests.Session()
S.headers.update({
    "Authorization": f"Bearer {DATA_PAT}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "esp01-tg-bridge",
})

stop = threading.Event()
L = threading.RLock()
DEVLOG = deque(maxlen=60)                       # last lines printed by the serial device
reached_deadline = False


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def now_ts():
    return int(time.time())


# ----------------------------------------------------------------- GitHub file helpers
def gh_get(path):
    r = S.get(f"{API}/repos/{DATA_REPO}/contents/{path}", timeout=20)
    if r.status_code == 200:
        j = r.json()
        return base64.b64decode(j["content"]), j["sha"]
    if r.status_code == 404:
        return None, None
    raise RuntimeError(f"GitHub read {path}: HTTP {r.status_code}")


def gh_put(path, data: bytes, sha, msg="agent"):
    body = {"message": msg, "content": base64.b64encode(data).decode()}
    if sha:
        body["sha"] = sha
    r = S.put(f"{API}/repos/{DATA_REPO}/contents/{path}", json=body, timeout=25)
    if r.status_code in (200, 201):
        return r.json()["content"]["sha"]
    if r.status_code in (409, 422):
        return "CONFLICT"
    log(f"GitHub write {path}: HTTP {r.status_code}")
    return None


class RepoFile:
    """A file in the data repo kept in memory and saved in batches."""

    def __init__(self, path, default, is_json=True, min_interval=10):
        self.path, self.default, self.is_json = path, default, is_json
        self.min_interval = min_interval
        self.sha = None
        self.dirty = False
        self.ver = 0
        self.last_save = 0
        self.data = json.loads(json.dumps(default)) if is_json else default

    def load(self):
        raw, sha = gh_get(self.path)
        self.sha = sha
        if raw is None:
            return
        if self.is_json:
            d = json.loads(raw.decode("utf-8"))
            with L:
                self.data.clear()
                self.data.update(d)
                for k, v in self.default.items():
                    if k not in self.data:
                        self.data[k] = json.loads(json.dumps(v))
        else:
            self.data = raw.decode("utf-8")

    def mark(self):
        with L:
            self.dirty = True
            self.ver += 1

    def save(self, force=False):
        with L:
            if not self.dirty:
                return True
            if not force and time.time() - self.last_save < self.min_interval:
                return True
            ver = self.ver
            payload = (json.dumps(self.data, ensure_ascii=False, separators=(",", ":"))
                       if self.is_json else self.data).encode("utf-8")
        for attempt in range(2):
            try:
                res = gh_put(self.path, payload, self.sha, f"agent: {self.path}")
            except requests.RequestException as e:
                log(f"save {self.path} failed: {e.__class__.__name__}")
                return False
            if res == "CONFLICT" and attempt == 0:
                try:
                    _, self.sha = gh_get(self.path)
                except (requests.RequestException, RuntimeError):
                    return False
                continue
            if res and res != "CONFLICT":
                self.sha = res
                self.last_save = time.time()
                with L:
                    if self.ver == ver:
                        self.dirty = False
                return True
            return False
        return False


# ----------------------------------------------------------------- inbox (records for the board)
def parse_inbox(raw: bytes):
    recs, i = [], 0
    while i < len(raw):
        nl = raw.find(b"\n", i)
        if nl < 0:
            break
        parts = raw[i:nl].split(b" ")
        if len(parts) != 3:
            break
        try:
            kind, seq, n = parts[0].decode(), int(parts[1]), int(parts[2])
        except ValueError:
            break
        start, end = nl + 1, nl + 1 + n
        if end > len(raw):
            break
        recs.append((kind, seq, raw[start:end]))
        i = end + 1
    return recs


def build_inbox(recs) -> bytes:
    return b"".join(f"{k} {s} {len(p)}\n".encode() + p + b"\n" for k, s, p in recs)


class Inbox:
    def __init__(self):
        self.lock = threading.Lock()
        self.records = []
        self.sha = None
        self.dirty = False
        self.last_seq = 0
        self.url = f"{API}/repos/{DATA_REPO}/contents/{INBOX_PATH}"

    def load(self):
        r = S.get(self.url, timeout=20)
        if r.status_code == 200:
            j = r.json()
            self.sha = j["sha"]
            self.records = parse_inbox(base64.b64decode(j["content"]))
            if self.records:
                self.last_seq = max(s for _, s, _ in self.records)
            log(f"Inbox loaded: {len(self.records)} record(s)")
        elif r.status_code == 404:
            log("Inbox file does not exist yet - it will be created")
        else:
            raise RuntimeError(f"Cannot read inbox: HTTP {r.status_code} (check DATA_REPO / DATA_PAT)")

    def add(self, kind: str, payload: bytes):
        with self.lock:
            seq = max(self.last_seq + 1, int(time.time()))
            self.last_seq = seq
            self.records.append((kind, seq, payload))
            while len(self.records) > 1 and (
                len(self.records) > MAX_RECORDS or len(build_inbox(self.records)) > MAX_INBOX_BYTES
            ):
                self.records.pop(0)
            self.dirty = True

    def flush(self) -> bool:
        with self.lock:
            if not self.dirty:
                return True
            content = base64.b64encode(build_inbox(self.records)).decode()
            for attempt in range(2):
                body = {"message": "inbox", "content": content}
                if self.sha:
                    body["sha"] = self.sha
                try:
                    r = S.put(self.url, json=body, timeout=20)
                except requests.RequestException as e:
                    log(f"Inbox write failed: {e.__class__.__name__}")
                    return False
                if r.status_code in (200, 201):
                    self.sha = r.json()["content"]["sha"]
                    self.dirty = False
                    return True
                if r.status_code in (409, 422) and attempt == 0:
                    g = S.get(self.url, timeout=20)
                    self.sha = g.json().get("sha") if g.status_code == 200 else None
                    continue
                log(f"Inbox write failed: HTTP {r.status_code}")
                return False
            return False


INBOX = Inbox()

DEFAULT_STATE = {
    "mode": "agent", "tz": TZ_NAME, "confirm_critical": True,
    "aliases": {}, "commands": {}, "channels": {}, "rules": {}, "schedules": {},
    "buttons": {}, "history": [], "pending": {}, "esp": {}, "notified": {},
    "scenes": {}, "jobs": [], "audit": [], "lang": "fa", "board_alert": True, "offline_after_min": 30,
    "breaker_until": 0, "dev_seq": 0, "scn_seq": 0,
    "controls": {}, "custom": [], "ctl_seq": 0, "btn_seq": 0,
}
ST = RepoFile("agent/state.json", DEFAULT_STATE, min_interval=10)
DATA = RepoFile("agent/data.json", {"series": {}, "last": {}}, min_interval=90)
KEYS = RepoFile("agent/keys.json", {"keys": []}, min_interval=5)
NOTES = RepoFile("agent/brain.md",
                 "# Project brain\nNotes and algorithms the agent keeps for itself.\n", is_json=False,
                 min_interval=10)
state = ST.data


# ----------------------------------------------------------------- text utils
_FA = str.maketrans({"ي": "ی", "ك": "ک", "ۀ": "ه", "ة": "ه", "\u200c": " ", "\u200d": "",
                     "۰": "0", "۱": "1", "۲": "2", "۳": "3", "۴": "4", "۵": "5", "۶": "6", "۷": "7", "۸": "8", "۹": "9",
                     "٠": "0", "١": "1", "٢": "2", "٣": "3", "٤": "4", "٥": "5", "٦": "6", "٧": "7", "٨": "8", "٩": "9"})


def norm(s: str) -> str:
    s = str(s).translate(_FA).lower()
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    return " ".join(s.split())


def tz():
    try:
        return ZoneInfo(state.get("tz") or TZ_NAME)
    except Exception:
        return ZoneInfo("UTC")


def tznow():
    return datetime.now(tz())


# ---- safe expression evaluator for templates like {{ clamp((value-25)*10, 0, 100) }}
_BIN = {ast.Add: lambda a, b: a + b, ast.Sub: lambda a, b: a - b, ast.Mult: lambda a, b: a * b,
        ast.Div: lambda a, b: a / b, ast.Mod: lambda a, b: a % b, ast.FloorDiv: lambda a, b: a // b}
_CMP = {ast.Gt: lambda a, b: a > b, ast.Lt: lambda a, b: a < b, ast.GtE: lambda a, b: a >= b,
        ast.LtE: lambda a, b: a <= b, ast.Eq: lambda a, b: a == b, ast.NotEq: lambda a, b: a != b}
_FUNCS = {"min": min, "max": max, "round": round, "int": int, "float": float, "abs": abs,
          "floor": math.floor, "ceil": math.ceil,
          "clamp": lambda x, lo, hi: max(lo, min(hi, x))}


def safe_eval(expr: str, names: dict):
    if len(expr) > 200:
        raise ValueError("expression too long")
    tree = ast.parse(expr, mode="eval")

    def ev(n, depth=0):
        if depth > 30:
            raise ValueError("expression too deep")
        if isinstance(n, ast.Expression):
            return ev(n.body, depth + 1)
        if isinstance(n, ast.Constant) and isinstance(n.value, (int, float, str, bool)):
            return n.value
        if isinstance(n, ast.Name):
            if n.id in names:
                return names[n.id]
            raise ValueError(f"unknown name {n.id}")
        if isinstance(n, ast.BinOp) and type(n.op) in _BIN:
            return _BIN[type(n.op)](ev(n.left, depth + 1), ev(n.right, depth + 1))
        if isinstance(n, ast.UnaryOp):
            v = ev(n.operand, depth + 1)
            if isinstance(n.op, ast.USub):
                return -v
            if isinstance(n.op, ast.UAdd):
                return +v
            if isinstance(n.op, ast.Not):
                return not v
        if isinstance(n, ast.Compare):
            left = ev(n.left, depth + 1)
            for op, comp in zip(n.ops, n.comparators):
                right = ev(comp, depth + 1)
                if type(op) not in _CMP or not _CMP[type(op)](left, right):
                    return False
                left = right
            return True
        if isinstance(n, ast.BoolOp):
            vals = [ev(v, depth + 1) for v in n.values]
            return all(vals) if isinstance(n.op, ast.And) else any(vals)
        if isinstance(n, ast.IfExp):
            return ev(n.body, depth + 1) if ev(n.test, depth + 1) else ev(n.orelse, depth + 1)
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in _FUNCS
                and not n.keywords and len(n.args) <= 3):
            return _FUNCS[n.func.id](*[ev(a, depth + 1) for a in n.args])
        raise ValueError("unsupported expression")

    return ev(tree)


_OK_NODES = (ast.Expression, ast.BinOp, ast.UnaryOp, ast.Compare, ast.BoolOp, ast.IfExp, ast.Constant, ast.Name,
             ast.Call, ast.Load, ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.FloorDiv, ast.USub, ast.UAdd,
             ast.Not, ast.Gt, ast.Lt, ast.GtE, ast.LtE, ast.Eq, ast.NotEq, ast.And, ast.Or)


def validate_expr(expr: str):
    """Static check done when something is DEFINED (names may refer to channels that exist later)."""
    if len(expr) > 200:
        raise ValueError("expression too long")
    for n in ast.walk(ast.parse(expr, mode="eval")):
        if not isinstance(n, _OK_NODES):
            raise ValueError("unsupported expression")
        if isinstance(n, ast.Call) and (not isinstance(n.func, ast.Name) or n.func.id not in _FUNCS or n.keywords):
            raise ValueError("only min max round int float abs floor ceil clamp are allowed as functions")
        if isinstance(n, ast.Constant) and not isinstance(n.value, (int, float, str, bool)):
            raise ValueError("unsupported constant")


def check_templates(text: str):
    for m in re.finditer(r"\{\{(.+?)\}\}", text):
        validate_expr(m.group(1).strip())


def render(text: str, names: dict) -> str:
    def rep(m):
        v = safe_eval(m.group(1).strip(), names)
        if isinstance(v, float):
            v = int(v) if v == int(v) else round(v, 2)
        return str(v)
    return re.sub(r"\{\{(.+?)\}\}", rep, text)


# ----------------------------------------------------------------- i18n (UI language: fa / en)
STR = {
    "fa": {
        "title": "📱 پنل کنترل", "devices": "💡 دستگاه‌ها", "sensors": "📊 سنسورها", "scenes": "🎬 صحنه‌ها",
        "auto": "⏱ خودکارسازی", "board": "🔧 برد", "ai": "🤖 هوش مصنوعی", "settings": "⚙️ تنظیمات",
        "kb_panel": "📱 پنل", "back": "◀️ بازگشت", "home": "🏠 خانه", "refresh": "🔄 تازه‌سازی",
        "all_off": "⏻ خاموش کردن همه", "enable": "▶️ فعال کن", "disable": "⏸ غیرفعال کن", "delete": "🗑 حذف",
        "yes": "✅ بله", "no": "❌ خیر", "expired": "منقضی شد", "ok": "✓",
        "devices_title": "💡 دستگاه‌ها (برای روشن/خاموش روی نام بزن)", "sensors_title": "📊 سنسورها",
        "scenes_title": "🎬 صحنه‌ها", "auto_title": "⏱ قانون‌ها و زمان‌بندها", "board_title": "🔧 کنترل برد",
        "ai_title": "🤖 هوش مصنوعی", "settings_title": "⚙️ تنظیمات",
        "no_devices": "هنوز دستگاهی نیست. بگو: «برای لامپ اتاق پین D1 بذار» یا «لامپ اتاق رو L2 بذار»",
        "no_sensors": "هنوز سنسوری نیست. بگو: «داده دما با DX میاد»",
        "no_scenes": "صحنه‌ای نیست. بگو: «صحنه شب بساز: همه چیز خاموش شه»",
        "no_auto": "قانون یا زمان‌بندی نیست. بگو: «هر شب ساعت ۱ میانگین دما رو بفرست»",
        "board_online": "🟢 برد آنلاین (آخرین پیام {age} پیش)", "board_offline": "🔴 برد در دسترس نیست (آخرین پیام {age} پیش)",
        "board_unknown": "⚪ هنوز پیامی از برد نیامده", "board_back": "🟢 برد دوباره آنلاین شد",
        "board_lost": "🔴 از برد {age} است خبری نیست. برق یا وای‌فای را چک کن",
        "ai_line": "🤖 کلید آماده: {ready}/{total} · حالت: {mode}",
        "lang_btn": "🌐 زبان: فارسی", "confirm_btn": "🛡 تأیید دستور حساس: {v}", "alert_btn": "🔔 هشدار قطعی برد: {v}",
        "export": "📤 خروجی تنظیمات", "mode_btn": "🔀 حالت: {mode}", "forget": "🧹 پاک کردن حافظهٔ گفتگو",
        "reboot": "♻️ راه‌اندازی مجدد", "reboot_q": "برد راه‌اندازی مجدد شود؟", "delete_q": "حذف شود؟ {name}",
        "sent": "⏳ ارسال شد. نتیجه به‌صورت پیام می‌آید", "stale": "⚠️ سنسور {ch} بیش از {min} دقیقه داده نفرستاده",
        "stale_back": "🟢 سنسور {ch} دوباره داده می‌فرستد",
        "breaker": "🛑 بیش از {n} دستور خودکار در یک دقیقه. برای ایمنی {m} دقیقه متوقف شد. با /breaker reset ادامه بده",
        "graph_nodata": "برای {ch} داده‌ای در این بازه نیست", "log_empty": "هنوز خطی از دستگاه نیامده",
        "hist_empty": "هنوز عملی ثبت نشده", "on": "روشن", "off": "خاموش", "state_on": "روشن", "state_off": "خاموش",
        "jobs_line": "⏳ تایمر فعال: {n}", "lang_set": "زبان: فارسی ✓",
        "widgets": "🧩 دکمه‌های من", "widgets_title": "🧩 دکمه‌ها و متغیرها (برای حذف 🗑 را بزن)",
        "no_widgets": "هنوز دکمه‌ای نساخته‌ای. بگو: «یه دکمه بساز که لامپ x رو روشن/خاموش کنه» یا «برای سرعت فن دکمه + و - بساز، از ۰ تا ۲۵۵ با گام ۱۰»",
        "wifi": "وای‌فای", "internet": "اینترنت (GitHub)", "uptime": "مدت روشن بودن",
        "add_token": "➕ افزودن توکن هوش مصنوعی", "test_keys": "🔄 تست کلیدها", "cancel": "✖️ انصراف",
        "key_prompt": "🔑 کلید API را همین‌جا بفرست (فقط خود کلید).\nسرویس را خودم تشخیص می‌دهم، پیامت را پاک می‌کنم و کلید را تست می‌کنم.",
        "key_cancel": "لغو شد", "which_provider": "این کلید مال کدام سرویس است؟",
        "key_ok": "✅ کلید {id} ({provider} / {model}) کار می‌کند. پیام حاوی کلید پاک شد.",
        "key_fail": "⚠️ کلید {id} ذخیره شد ولی تست شکست خورد: {err}\nکلید، اینترنت یا نام مدل را چک کن. دستی: /addkey سرویس کلید مدل",
        "key_expired": "منقضی شد. دوباره کلید را بفرست", "no_keys": "هنوز کلیدی نیست. دکمهٔ ➕ را بزن و کلید را بفرست",
    },
    "en": {
        "title": "📱 Control panel", "devices": "💡 Devices", "sensors": "📊 Sensors", "scenes": "🎬 Scenes",
        "auto": "⏱ Automations", "board": "🔧 Board", "ai": "🤖 AI", "settings": "⚙️ Settings",
        "kb_panel": "📱 Panel", "back": "◀️ Back", "home": "🏠 Home", "refresh": "🔄 Refresh",
        "all_off": "⏻ All off", "enable": "▶️ Enable", "disable": "⏸ Disable", "delete": "🗑 Delete",
        "yes": "✅ Yes", "no": "❌ No", "expired": "expired", "ok": "✓",
        "devices_title": "💡 Devices (tap a name to toggle)", "sensors_title": "📊 Sensors",
        "scenes_title": "🎬 Scenes", "auto_title": "⏱ Rules and schedules", "board_title": "🔧 Board control",
        "ai_title": "🤖 AI", "settings_title": "⚙️ Settings",
        "no_devices": "No devices yet. Say: \"add the room lamp on pin D1\" or \"room lamp is L2\"",
        "no_sensors": "No sensors yet. Say: \"temperature arrives as DX\"",
        "no_scenes": "No scenes yet. Say: \"create a night scene that turns everything off\"",
        "no_auto": "No rules or schedules. Say: \"every night at 1 send the average temperature\"",
        "board_online": "🟢 Board online (last message {age} ago)", "board_offline": "🔴 Board unreachable (last message {age} ago)",
        "board_unknown": "⚪ No message from the board yet", "board_back": "🟢 Board is back online",
        "board_lost": "🔴 No news from the board for {age}. Check power or Wi-Fi",
        "ai_line": "🤖 Keys ready: {ready}/{total} · mode: {mode}",
        "lang_btn": "🌐 Language: English", "confirm_btn": "🛡 Confirm critical: {v}", "alert_btn": "🔔 Board offline alert: {v}",
        "export": "📤 Export settings", "mode_btn": "🔀 Mode: {mode}", "forget": "🧹 Clear chat memory",
        "reboot": "♻️ Reboot", "reboot_q": "Reboot the board?", "delete_q": "Delete {name}?",
        "sent": "⏳ Sent. The result arrives as a message", "stale": "⚠️ Sensor {ch} sent no data for {min} min",
        "stale_back": "🟢 Sensor {ch} is sending data again",
        "breaker": "🛑 More than {n} automatic commands in a minute. Paused for {m} min for safety. Use /breaker reset to resume",
        "graph_nodata": "No data for {ch} in that range", "log_empty": "No device lines yet",
        "hist_empty": "Nothing recorded yet", "on": "On", "off": "Off", "state_on": "on", "state_off": "off",
        "jobs_line": "⏳ Active timers: {n}", "lang_set": "Language: English ✓",
        "widgets": "🧩 My buttons", "widgets_title": "🧩 Buttons and variables (tap 🗑 to delete)",
        "no_widgets": "No custom buttons yet. Say: \"make a button that toggles lamp x\" or \"make + and - buttons for the fan speed, 0 to 255, step 10\"",
        "wifi": "Wi-Fi", "internet": "Internet (GitHub)", "uptime": "Uptime",
        "add_token": "➕ Add AI token", "test_keys": "🔄 Test keys", "cancel": "✖️ Cancel",
        "key_prompt": "🔑 Send the API key here (just the key).\nI detect the service, delete your message and test the key.",
        "key_cancel": "Cancelled", "which_provider": "Which service is this key for?",
        "key_ok": "✅ Key {id} ({provider} / {model}) works. The message containing the key was deleted.",
        "key_fail": "⚠️ Key {id} saved but the test failed: {err}\nCheck the key, the internet or the model name. Manual: /addkey provider KEY MODEL",
        "key_expired": "Expired. Send the key again", "no_keys": "No keys yet. Tap ➕ and send the key",
    },
}
SCREEN_KEYS = [("widgets", "wid"), ("kb_panel", "home"), ("devices", "dev"), ("sensors", "sen"), ("scenes", "scn"), ("auto", "aut"),
               ("board", "brd"), ("ai", "ai"), ("settings", "set")]
SCREEN_WORDS = {"panel": "home", "menu": "home", "منو": "home", "پنل": "home"}
for _lang in STR.values():
    for _k, _scr in SCREEN_KEYS:
        SCREEN_WORDS[norm(_lang[_k])] = _scr


def T(key, **kw):
    lang = state.get("lang", "fa")
    s = STR.get(lang, STR["en"]).get(key) or STR["en"].get(key) or key
    try:
        return s.format(**kw)
    except (KeyError, IndexError):
        return s


# ----------------------------------------------------------------- Telegram
def tg(method, payload, timeout=20):
    r = requests.post(f"{TG}/{method}", json=payload, timeout=timeout)
    try:
        data = r.json()
    except ValueError:
        data = {"ok": False, "description": f"HTTP {r.status_code}"}
    return r.status_code, data


def make_inline(rows):
    """rows: [[{"label","actions"|"serial"}]] -> inline_keyboard; actions stored under a short id."""
    kb = []
    with L:
        for row in rows[:6]:
            out = []
            for b in row[:4]:
                label = str(b.get("label", "?"))[:40]
                acts = b.get("actions")
                if acts is None and "serial" in b:
                    acts = [{"type": "serial", "text": str(b["serial"])}]
                if "raw_actions" in b:                       # internal use only (confirm buttons)
                    norm_acts = b["raw_actions"]
                else:
                    try:
                        norm_acts = [normalize_action(a, auto=True) for a in (acts or [])]
                    except ValueError:
                        norm_acts = []
                bid = uuid.uuid4().hex[:8]
                state["buttons"][bid] = {"actions": norm_acts, "ts": now_ts()}
                out.append({"text": label, "callback_data": f"b:{bid}"})
            if out:
                kb.append(out)
        if len(state["buttons"]) > 200:
            for k in sorted(state["buttons"], key=lambda x: state["buttons"][x]["ts"])[:50]:
                state["buttons"].pop(k, None)
        ST.mark()
    return {"inline_keyboard": kb}


def tg_send(text, buttons=None, keyboard=None, remove_keyboard=False, chat=None):
    chat = chat or TG_CHAT
    if not chat:
        return False
    text = str(text) or "."
    chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)] or ["."]
    ok = True
    for idx, ch in enumerate(chunks):
        payload = {"chat_id": chat, "text": ch, "disable_web_page_preview": True}
        if idx == len(chunks) - 1:
            if buttons:
                payload["reply_markup"] = make_inline(buttons)
            elif keyboard:
                payload["reply_markup"] = {"keyboard": [[{"text": str(c)[:30]} for c in row[:4]] for row in keyboard[:6]],
                                           "resize_keyboard": True}
            elif remove_keyboard:
                payload["reply_markup"] = {"remove_keyboard": True}
        try:
            code, data = tg("sendMessage", payload)
            ok = ok and code == 200 and data.get("ok", False)
            if not ok:
                log(f"sendMessage failed {code}")
        except requests.RequestException as e:
            log(f"sendMessage error {e.__class__.__name__}")
            ok = False
    return ok


def notify_once(key, text, every=1800):
    with L:
        last = state["notified"].get(key, 0)
        if time.time() - last < every:
            return
        state["notified"][key] = time.time()
        ST.mark()
    tg_send(text)


# ----------------------------------------------------------------- sensor data
_regex_cache = {}


def get_regex(pattern):
    r = _regex_cache.get(pattern)
    if r is None:
        r = re.compile(pattern)
        _regex_cache[pattern] = r
    return r


def match_channel(line):
    with L:
        chans = list(state["channels"].items())
    for cid, ch in chans:
        try:
            m = get_regex(ch["regex"]).search(line)
        except re.error:
            continue
        if m:
            val = m.group(1) if m.groups() else m.group(0)
            return cid, ch, val
    return None


def match_channels(line):
    """Every channel whose regex matches the line (one line may carry temperature AND humidity)."""
    with L:
        chans = list(state["channels"].items())
    out = []
    for cid, ch in chans:
        try:
            m = get_regex(ch["regex"]).search(line)
        except re.error:
            continue
        if m:
            out.append((cid, ch, m.group(1) if m.groups() else m.group(0)))
    return out


def to_value(txt):
    try:
        f = float(txt)
        return int(f) if f == int(f) and "." not in str(txt) else round(f, 3)
    except (ValueError, OverflowError):
        return str(txt)[:60]


def record(cid, valtxt):
    v = to_value(valtxt)
    ts = now_ts()
    with L:
        ser = DATA.data["series"].setdefault(cid, [])
        ser.append([ts, v])
        cutoff = ts - 14 * 86400
        while ser and ser[0][0] < cutoff:
            ser.pop(0)
        if len(ser) > 2000:                      # keep recent detail, thin out the old half
            ser[:] = ser[:1000][::2] + ser[1000:]
        DATA.data["last"][cid] = [ts, v]
        DATA.mark()
        ch = state["channels"].get(cid)
        back = bool(ch and ch.get("stale_alerted"))
        if back:
            ch["stale_alerted"] = False
            ST.mark()
    if back:
        tg_send(T("stale_back", ch=cid))
    eval_rules(cid, v)


def series_stats(cid, hours):
    with L:
        ser = list(DATA.data["series"].get(cid, []))
    cutoff = now_ts() - hours * 3600
    vals = [v for t, v in ser if t >= cutoff and isinstance(v, (int, float))]
    if not vals:
        return {"n": 0}
    return {"n": len(vals), "avg": round(sum(vals) / len(vals), 2), "min": min(vals), "max": max(vals),
            "last": vals[-1]}


def make_report(channels, hours, title=None):
    with L:
        cids = list(channels) if channels else list(state["channels"].keys())
    lines = [title or f"Report (last {hours} h)"]
    for cid in cids:
        st = series_stats(cid, hours)
        unit = (state["channels"].get(cid) or {}).get("unit", "")
        if st["n"] == 0:
            lines.append(f"{cid}: no data")
        else:
            lines.append(f"{cid}: avg {st['avg']}{unit}  min {st['min']}{unit}  max {st['max']}{unit}  "
                         f"last {st['last']}{unit}  (n={st['n']})")
    return "\n".join(lines)


def build_names(extra=None):
    names = {}
    with L:
        for cid in state["channels"]:
            last = DATA.data["last"].get(cid)
            if last and isinstance(last[1], (int, float)):
                names[cid] = last[1]
    for cid in list(names):
        st = series_stats(cid, 24)
        if st.get("n"):
            names["avg24_" + cid] = st["avg"]
    n = tznow()
    names.update({"hour": n.hour, "minute": n.minute, "weekday": n.weekday()})
    if extra:
        names.update(extra)
    return names


# ----------------------------------------------------------------- actions: validation
AUTO_TYPES = {"serial", "esp", "send", "report", "device", "scene", "graph", "set_control"}
OPS = {">", "<", ">=", "<=", "==", "!=", "any"}
ESP_CMDS = {"STATUS", "PING", "SCAN", "WIFI", "REBOOT", "GPIO", "ADC", "LED"}
D_MAP = {"D0": 16, "D1": 5, "D2": 4, "D3": 0, "D4": 2, "D5": 14, "D6": 12, "D7": 13, "D8": 15}
OUTPUT_GPIOS = {16, 5, 4, 14, 12, 13}                      # D0 D1 D2 D5 D6 D7 (D3/D4/D8 are boot-strapping pins)
SCREEN_NAMES = {"home", "devices", "sensors", "scenes", "auto", "board", "ai", "settings", "buttons"}


def pin_to_gpio(p):
    p = str(p).upper()
    if p in D_MAP:
        return D_MAP[p]
    return int(p) if p.isdigit() else -1
_ID = re.compile(r"^[A-Za-z0-9_]{1,24}$")
_DAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _s(a, key, lo=1, hi=200, required=True, default=""):
    v = a.get(key, default)
    if v is None:
        v = default
    v = str(v)
    if required and not (lo <= len(v.strip()) <= hi):
        raise ValueError(f"{key} must be {lo}-{hi} chars")
    return v[:hi]


def _aliases(a):
    return [str(x).strip()[:40] for x in (a.get("aliases") or []) if str(x).strip()][:5]


def _when(a):
    w = str(a.get("when") or "").strip()[:150]
    if w:
        validate_expr(w)
    return w


def normalize_action(a, auto=False):
    if not isinstance(a, dict) or "type" not in a:
        raise ValueError("action must be an object with a type")
    t = str(a["type"])
    if auto and t not in AUTO_TYPES:
        raise ValueError(f"'{t}' not allowed inside rules/schedules/buttons")
    o = {"type": t}
    if a.get("critical"):
        o["critical"] = True
    if t == "serial":
        o["text"] = _s(a, "text", 1, MAX_SERIAL_TEXT)
        check_templates(o["text"])
    elif t == "esp":
        cmd = _s(a, "cmd", 2, 12).upper()
        if cmd not in ESP_CMDS:
            raise ValueError("esp cmd must be one of " + ",".join(sorted(ESP_CMDS)))
        args = _s(a, "args", 0, 95, required=False)
        if re.search(r"[\x00-\x1f]", args):
            raise ValueError("bad characters in args")
        if cmd == "WIFI" and (args.split(" ")[0].upper() if args else "") not in {"TRY", "LIST", "FORGET", "RECONNECT"}:
            raise ValueError("WIFI needs TRY|LIST|FORGET|RECONNECT")
        if cmd == "GPIO" and "{{" in args:                     # PWM duty from a formula, e.g. fan speed from temperature
            m = re.match(r"^(D[0-8]|\d{1,2})\s+PWM\s+(\{\{.+\}\})$", args.strip(), re.I)
            if not m or pin_to_gpio(m.group(1).upper()) not in OUTPUT_GPIOS or len(args) > 95:
                raise ValueError("templated GPIO must look like: D2 PWM {{ clamp(round((value-25)*100),0,1023) }}")
            validate_expr(m.group(2)[2:-2].strip())
            args = f"{m.group(1).upper()} PWM {m.group(2)}"
        elif cmd == "GPIO":
            m = re.match(r"^(D[0-8]|\d{1,2})\s+(ON|OFF|TOGGLE|READ|PWM\s+\d{1,4}|PULSE\s+\d{1,4})$", args.strip(), re.I)
            if not m:
                raise ValueError("GPIO args: <pin> ON|OFF|TOGGLE|READ|PWM n|PULSE ms")
            pin, act = m.group(1).upper(), " ".join(m.group(2).upper().split())
            if act != "READ" and pin_to_gpio(pin) not in OUTPUT_GPIOS:
                raise ValueError("output pins allowed: D0 D1 D2 D5 D6 D7")
            args = f"{pin} {act}"
        if cmd == "LED" and args.upper() not in ("ON", "OFF", "AUTO"):
            raise ValueError("LED needs ON|OFF|AUTO")
        if cmd == "LED":
            args = args.upper()
        if cmd == "ADC":
            args = ""
        o.update(cmd=cmd, args=args)
    elif t == "send":
        o["text"] = _s(a, "text", 1, 1000)
        check_templates(o["text"])
    elif t == "report":
        o["channels"] = [str(c) for c in (a.get("channels") or [])][:10]
        o["hours"] = max(1, min(720, int(a.get("hours", 24))))
        o["title"] = _s(a, "title", 0, 100, required=False)
    elif t == "define_alias":
        code = _s(a, "code", 1, 30)
        if not re.match(r"^[A-Za-z0-9_]+$", code):
            raise ValueError("code: letters/digits/_ only")
        pin = str(a.get("pin") or "").upper()
        if pin and pin_to_gpio(pin) not in OUTPUT_GPIOS:
            raise ValueError("pin must be D0, D1, D2, D5 or D6/D7 (D3, D4, D8 are boot pins)")
        o.update(name=_s(a, "name", 1, 40), code=code, desc=_s(a, "desc", 0, 120, required=False),
                 on=_s(a, "on", 0, 40, required=False) or code + "ON",
                 off=_s(a, "off", 0, 40, required=False) or code + "OFF",
                 pin=pin, invert=bool(a.get("invert")), aliases=_aliases(a))
    elif t == "remove_alias":
        o["name"] = _s(a, "name", 1, 40)
    elif t == "define_command":
        phr = a.get("phrases") or ([a["phrase"]] if a.get("phrase") else [])
        phr = [str(p) for p in phr if norm(p)][:10]
        if not phr:
            raise ValueError("phrases required")
        ser = a.get("serial")
        lines = [str(x) for x in (ser if isinstance(ser, list) else [ser]) if x][:10]
        if not lines or any(len(x) > MAX_SERIAL_TEXT for x in lines):
            raise ValueError("serial lines required (<=480 chars each)")
        o.update(phrases=phr, serial=lines, desc=_s(a, "desc", 0, 120, required=False))
    elif t == "remove_command":
        o["phrase"] = _s(a, "phrase", 1, 80)
    elif t == "define_channel":
        cid = _s(a, "id", 1, 24)
        if not _ID.match(cid):
            raise ValueError("channel id: letters/digits/_")
        rx = _s(a, "regex", 1, 100)
        get_regex(rx)
        if re.search(r"\([^)]*[+*][^)]*\)[+*{]", rx):
            raise ValueError("regex too risky (nested quantifier)")
        o.update(id=cid, regex=rx, desc=_s(a, "desc", 0, 120, required=False),
                 unit=_s(a, "unit", 0, 8, required=False), forward=bool(a.get("forward", False)),
                 stale_min=max(0, min(10080, int(a.get("stale_min", 0)))))
    elif t == "remove_channel":
        o["id"] = _s(a, "id", 1, 24)
    elif t == "add_rule":
        rid = _s(a, "id", 1, 24)
        if not _ID.match(rid):
            raise ValueError("rule id: letters/digits/_")
        op = str(a.get("op", "any"))
        if op not in OPS:
            raise ValueError("bad op")
        acts = [normalize_action(x, auto=True) for x in (a.get("actions") or [])][:5]
        if not acts:
            raise ValueError("rule needs actions")
        o.update(id=rid, channel=_s(a, "channel", 1, 24), op=op,
                 value=float(a.get("value", 0)) if op != "any" else 0.0, actions=acts,
                 cooldown_s=max(0, min(86400, int(a.get("cooldown_s", 30)))), repeat=bool(a.get("repeat", False)),
                 when=_when(a))
    elif t == "remove_rule":
        o["id"] = _s(a, "id", 1, 24)
    elif t == "add_schedule":
        sid = _s(a, "id", 1, 24)
        if not _ID.match(sid):
            raise ValueError("schedule id: letters/digits/_")
        kind = str(a.get("kind", ""))
        acts = [normalize_action(x, auto=True) for x in (a.get("actions") or [])][:5]
        if not acts:
            raise ValueError("schedule needs actions")
        o.update(id=sid, kind=kind, actions=acts, when=_when(a))
        if kind == "daily":
            m = re.match(r"^(\d{1,2}):(\d{2})$", str(a.get("time", "")))
            if not m or int(m.group(1)) > 23 or int(m.group(2)) > 59:
                raise ValueError("time must be HH:MM")
            o["time"] = f"{int(m.group(1)):02d}:{m.group(2)}"
            days = a.get("days")
            if days:
                dd = []
                for d in days:
                    d = _DAYS.get(str(d).lower()[:3]) if not isinstance(d, int) else d
                    if d is None or not 0 <= d <= 6:
                        raise ValueError("days: mon..sun or 0..6")
                    dd.append(d)
                o["days"] = sorted(set(dd))
        elif kind == "every":
            o["minutes"] = max(1, min(10080, int(a.get("minutes", 60))))
        else:
            raise ValueError("kind must be daily or every")
    elif t == "remove_schedule":
        o["id"] = _s(a, "id", 1, 24)
    elif t == "graph":
        o.update(channel=_s(a, "channel", 1, 24), hours=max(1, min(720, int(a.get("hours", 24)))))
    elif t == "device":
        st = str(a.get("state", "toggle")).lower()
        if st not in ("on", "off", "toggle"):
            raise ValueError("device state: on|off|toggle")
        fm = float(a.get("for_minutes") or 0)
        if fm < 0 or fm > 1440 or (fm and st == "toggle"):
            raise ValueError("for_minutes: 0-1440 and needs state on/off")
        o.update(name=_s(a, "name", 1, 40), state=st, for_minutes=fm)
    elif t == "scene":
        o["name"] = _s(a, "name", 1, 40)
    elif t == "define_scene":
        acts = [normalize_action(x, auto=True) for x in (a.get("actions") or [])][:10]
        if not acts or any(x["type"] == "scene" for x in acts):
            raise ValueError("scene needs actions (scenes cannot contain scenes)")
        o.update(name=_s(a, "name", 1, 40), actions=acts, desc=_s(a, "desc", 0, 120, required=False))
    elif t == "remove_scene":
        o["name"] = _s(a, "name", 1, 40)
    elif t == "panel":
        sc = str(a.get("screen", "home"))
        if sc not in SCREEN_NAMES:
            raise ValueError("screen: " + ",".join(sorted(SCREEN_NAMES)))
        o["screen"] = sc
    elif t == "define_control":
        mn, mx, step = float(a.get("min", 0)), float(a.get("max", 100)), float(a.get("step", 1))
        if not mn < mx or step <= 0 or step > mx - mn or mx - mn > 1e6:
            raise ValueError("control needs min < max and 0 < step <= (max-min)")
        acts = [normalize_action(x, auto=True) for x in (a.get("actions") or [])][:3]
        if not acts or any(x["type"] == "set_control" for x in acts):
            raise ValueError("control needs output actions, e.g. serial 'FAN{{ value }}'")
        init = a.get("init")
        o.update(name=_s(a, "name", 1, 30), min=mn, max=mx, step=step, unit=_s(a, "unit", 0, 8, required=False),
                 actions=acts, init=float(init) if init is not None else None, aliases=_aliases(a))
    elif t == "set_control":
        has_v, has_d = a.get("value") is not None, a.get("delta") is not None
        to, expr = str(a.get("to") or "").lower(), str(a.get("expr") or "").strip()
        if has_v + has_d + bool(to) + bool(expr) != 1:
            raise ValueError("set_control needs exactly one of value, delta, to(min|max), expr")
        if to and to not in ("min", "max"):
            raise ValueError("to must be min or max")
        if expr:
            validate_expr(expr)
        o.update(name=_s(a, "name", 1, 30), value=float(a["value"]) if has_v else None,
                 delta=float(a["delta"]) if has_d else None, to=to, expr=expr)
    elif t == "remove_control":
        o["name"] = _s(a, "name", 1, 30)
    elif t == "add_button":
        acts = [normalize_action(x, auto=True) for x in (a.get("actions") or [])][:6]
        if not acts:
            raise ValueError("a button needs actions")
        o.update(label=_s(a, "label", 1, 30), actions=acts, same_row=bool(a.get("same_row")))
    elif t == "add_stepper":
        o.update(control=_s(a, "control", 1, 30), label=_s(a, "label", 0, 30, required=False),
                 step=float(a["step"]) if a.get("step") is not None else None,
                 big_step=float(a["big_step"]) if a.get("big_step") is not None else None)
    elif t == "remove_button":
        o["label"] = _s(a, "label", 1, 40)
    elif t == "note":
        o["text"] = _s(a, "text", 1, 500)
    elif t == "set_mode":
        m = str(a.get("mode", ""))
        if m not in ("agent", "raw"):
            raise ValueError("mode agent|raw")
        o["mode"] = m
    elif t == "set_tz":
        name = _s(a, "tz", 2, 40)
        ZoneInfo(name)
        o["tz"] = name
    elif t == "query":
        o.update(channel=_s(a, "channel", 1, 24), hours=max(1, min(720, int(a.get("hours", 24)))),
                 agg=str(a.get("agg", "avg")) if str(a.get("agg", "avg")) in ("avg", "min", "max", "last", "count") else "avg")
    else:
        raise ValueError(f"unknown action type '{t}'")
    return o


def critical_serials():
    out = set()
    with L:
        for al in state["aliases"].values():
            if al.get("critical"):
                out.update({al["on"], al["off"], al["code"]})
        for cm in state["commands"].values():
            if cm.get("critical"):
                out.update(cm["serial"])
    return out


def is_critical(a):
    t = a["type"]
    if t in ("serial", "esp") and a.get("critical"):
        return True
    # define_alias / define_command carry "critical" as a property of the DEVICE; defining it is not itself critical
    if t == "esp":
        return a["cmd"] == "REBOOT" or (a["cmd"] == "WIFI" and a["args"].split(" ")[0].upper() in ("TRY", "FORGET"))
    if t == "serial":
        return a["text"] in critical_serials()
    if t == "device":
        al = alias_by(a["name"])
        return bool(al and al.get("critical"))
    if t == "set_control":
        c = control_by(a["name"])
        return bool(c and c.get("critical"))
    if t == "scene":
        sc = scene_by(a["name"])
        return bool(sc and any(is_critical(x) for x in sc["actions"]))
    if t in ("add_rule", "add_schedule", "define_scene", "add_button", "define_control"):
        return any(is_critical(x) for x in a["actions"])
    return False


# ----------------------------------------------------------------- actions: execution
def find_alias(name):
    n = norm(name)
    with L:
        for k, al in state["aliases"].items():
            if norm(k) == n or norm(al["name"]) == n:
                return k
    return None


def alias_by(ref):
    r = str(ref)
    n = norm(r)
    with L:
        for al in state["aliases"].values():
            if al.get("id") == r or n in {norm(al["name"]), norm(al["code"])} | {norm(x) for x in al.get("aliases", [])}:
                return al
    return None


def scene_by(ref):
    r = str(ref)
    n = norm(r)
    with L:
        for sc in state["scenes"].values():
            if sc.get("id") == r or norm(sc["name"]) == n:
                return sc
    return None


def fmt_age(sec):
    sec = int(max(0, sec))
    if sec < 90:
        return f"{sec}s"
    if sec < 5400:
        return f"{sec // 60}m"
    return f"{sec // 3600}h" if sec < 172800 else f"{sec // 86400}d"


def audit(src, text):
    with L:
        state["audit"].append({"ts": now_ts(), "src": str(src)[:24], "text": str(text)[:140]})
        del state["audit"][:-40]
        ST.mark()


# ---- one-shot timers ("turn on the pump for 10 minutes")
def add_job(due, actions, key=None):
    with L:
        jobs = state["jobs"]
        if key:
            jobs[:] = [j for j in jobs if j.get("key") != key]
        jobs.append({"key": key, "due": due, "actions": actions})
        del jobs[:-100]
        ST.mark()


def remove_job(key):
    with L:
        n = len(state["jobs"])
        state["jobs"][:] = [j for j in state["jobs"] if j.get("key") != key]
        if len(state["jobs"]) != n:
            ST.mark()


def run_due_jobs():
    now = time.time()
    with L:
        due = [j for j in state["jobs"] if j["due"] <= now]
        if due:
            state["jobs"][:] = [j for j in state["jobs"] if j["due"] > now]
            ST.mark()
    for j in due:
        run_actions(j["actions"], {"user": False, "names": build_names(), "source": "timer"})
        INBOX.flush()


# ---- safety breaker: stops runaway automations (e.g. a rule that triggers itself through the serial device)
_burst = []
BREAKER_LIMIT, BREAKER_WINDOW, BREAKER_PAUSE = 40, 60, 300


def breaker_ok():
    return state.get("breaker_until", 0) <= time.time()


def breaker_count():
    now = time.time()
    _burst[:] = [t for t in _burst if now - t < BREAKER_WINDOW]
    _burst.append(now)
    if len(_burst) > BREAKER_LIMIT and breaker_ok():
        with L:
            state["breaker_until"] = now + BREAKER_PAUSE
            ST.mark()
        tg_send(T("breaker", n=BREAKER_LIMIT, m=BREAKER_PAUSE // 60))


def breaker_reset():
    _burst.clear()
    with L:
        state["breaker_until"] = 0
        ST.mark()


# ---- board liveness (any message from the board counts) and silent-ack throttling
BOARD = {"seen": 0.0, "alerted": False}
_ack = {"due": False, "last": 0.0}


def touch_board():
    BOARD["seen"] = time.time()
    if BOARD["alerted"]:
        BOARD["alerted"] = False
        tg_send(T("board_back"))


def watchdog():
    if not state.get("board_alert", True) or not state["esp"].get("hb") or not BOARD["seen"]:
        return
    age = time.time() - BOARD["seen"]
    if age > state.get("offline_after_min", 30) * 60 and not BOARD["alerted"]:
        BOARD["alerted"] = True
        tg_send(T("board_lost", age=fmt_age(age)))


def check_stale():
    now = time.time()
    with L:
        chans = list(state["channels"].items())
    for cid, ch in chans:
        sm = ch.get("stale_min", 0)
        last = DATA.data["last"].get(cid)
        if sm and last and now - last[0] > sm * 60 and not ch.get("stale_alerted"):
            ch["stale_alerted"] = True
            ST.mark()
            tg_send(T("stale", ch=cid, min=sm))


def request_ack():
    _ack["due"] = True


def maybe_ack():
    if _ack["due"] and time.time() - _ack["last"] >= 20:
        _ack["due"] = False
        _ack["last"] = time.time()
        INBOX.add("A", b"")


def _unchanged(ctx, text):
    """Inside a rule, skip output identical to the previous one (e.g. same fan speed every second)."""
    rule = ctx.get("rule")
    if rule is None:
        return False
    lo = rule.setdefault("last_out", {})
    key = str(ctx.get("idx", 0))
    if lo.get(key) == text:
        return True
    lo[key] = text
    ST.mark()
    return False


def ex_serial(a, ctx):
    text = render(a["text"], ctx["names"])
    if len(text) > MAX_SERIAL_TEXT:
        raise ValueError("serial text too long after rendering")
    if _unchanged(ctx, text):
        return "serial: unchanged, skipped"
    INBOX.add("M", text.encode("utf-8"))
    return f"serial <- {text[:60]}"


def ex_esp(a, ctx):
    args = a["args"]
    if "{{" in args:
        args = render(args, ctx["names"])
        m = re.match(r"^(D[0-8]|\d{1,2}) PWM (-?\d+)$", args)
        if not m or a["cmd"] != "GPIO":
            raise ValueError("template did not produce a valid GPIO PWM value")
        args = f"{m.group(1)} PWM {max(0, min(1023, int(m.group(2))))}"
    payload = (a["cmd"] + " " + args).strip()
    if _unchanged(ctx, payload):
        return "board command unchanged, skipped"
    INBOX.add("C", payload.encode("utf-8"))
    with L:
        state["esp"]["waiting"] = time.time()
        ST.mark()
    return f"board command queued: {payload.split(' ')[0]}"


def ex_send(a, ctx):
    tg_send(render(a["text"], ctx["names"]))
    return "message sent"


def ex_report(a, ctx):
    tg_send(make_report(a["channels"], a["hours"], a.get("title") or None))
    return "report sent"


def ex_define_alias(a, ctx):
    with L:
        old = state["aliases"].get(a["name"], {})
        aid = old.get("id")
        if not aid:
            state["dev_seq"] = state.get("dev_seq", 0) + 1
            aid = f"d{state['dev_seq']}"
        state["aliases"][a["name"]] = {
            "id": aid, "name": a["name"], "code": a["code"], "desc": a.get("desc", ""), "on": a["on"], "off": a["off"],
            "critical": bool(a.get("critical")), "pin": a.get("pin", ""), "invert": bool(a.get("invert")),
            "aliases": a.get("aliases", []),
            "state": old.get("state"), "state_ts": old.get("state_ts", 0)}
        ST.mark()
    how = f"pin {a['pin']}{' inverted' if a.get('invert') else ''}" if a.get("pin") else f"on:{a['on']} off:{a['off']}"
    return f"device '{a['name']}' = {a['code']} ({how})"


def ex_remove_alias(a, ctx):
    k = find_alias(a["name"])
    with L:
        if k:
            state["aliases"].pop(k, None)
            ST.mark()
    return f"alias '{a['name']}' removed" if k else f"alias '{a['name']}' not found"


def ex_define_command(a, ctx):
    with L:
        for p in a["phrases"]:
            state["commands"][norm(p)] = {"phrase": p, "serial": a["serial"], "desc": a.get("desc", ""),
                                          "critical": bool(a.get("critical"))}
        if len(state["commands"]) > 300:
            raise ValueError("too many commands (max 300)")
        ST.mark()
    return f"command defined for {len(a['phrases'])} phrase(s)"


def ex_remove_command(a, ctx):
    with L:
        ok = state["commands"].pop(norm(a["phrase"]), None) is not None
        ST.mark()
    return "command removed" if ok else "command not found"


def ex_define_channel(a, ctx):
    with L:
        state["channels"][a["id"]] = {"regex": a["regex"], "desc": a["desc"], "unit": a["unit"], "forward": a["forward"],
                                      "stale_min": a.get("stale_min", 0), "stale_alerted": False}
        ST.mark()
    return f"channel '{a['id']}' defined (forward={a['forward']}, stale alert={a.get('stale_min', 0)} min)"


def ex_remove_channel(a, ctx):
    with L:
        ok = state["channels"].pop(a["id"], None) is not None
        ST.mark()
    return "channel removed" if ok else "channel not found"


def ex_add_rule(a, ctx):
    with L:
        state["rules"][a["id"]] = {"channel": a["channel"], "op": a["op"], "value": a["value"], "actions": a["actions"],
                                   "cooldown_s": a["cooldown_s"], "repeat": a["repeat"], "when": a.get("when", ""),
                                   "enabled": True, "active": False, "last_fire": 0, "last_out": {}}
        ST.mark()
    return f"rule '{a['id']}' saved" + (f" (only when {a['when']})" if a.get("when") else "")


def ex_remove_rule(a, ctx):
    with L:
        ok = state["rules"].pop(a["id"], None) is not None
        ST.mark()
    return "rule removed" if ok else "rule not found"


def ex_add_schedule(a, ctx):
    sc = {"kind": a["kind"], "actions": a["actions"], "last_run": now_ts(), "last_date": "",
          "when": a.get("when", ""), "enabled": True}
    if a["kind"] == "daily":
        sc["time"] = a["time"]
        if a.get("days") is not None:
            sc["days"] = a["days"]
        hh, mm = map(int, a["time"].split(":"))
        n = tznow()
        if n >= n.replace(hour=hh, minute=mm, second=0, microsecond=0):
            sc["last_date"] = n.strftime("%Y-%m-%d")      # already past today: first run is tomorrow
    else:
        sc["minutes"] = a["minutes"]
    with L:
        state["schedules"][a["id"]] = sc
        ST.mark()
    return f"schedule '{a['id']}' saved ({a['kind']} {a.get('time') or str(a.get('minutes')) + ' min'})"


def ex_remove_schedule(a, ctx):
    with L:
        ok = state["schedules"].pop(a["id"], None) is not None
        ST.mark()
    return "schedule removed" if ok else "schedule not found"


def ex_note(a, ctx):
    with L:
        stamp = tznow().strftime("%Y-%m-%d %H:%M")
        text = NOTES.data + f"\n- [{stamp}] {a['text'].strip()}\n"
        while len(text) > 20000:
            text = text[text.index("\n", 200) + 1:] if "\n" in text[200:] else text[-15000:]
        NOTES.data = text
        NOTES.mark()
    return "note saved"


def ex_set_mode(a, ctx):
    with L:
        state["mode"] = a["mode"]
        ST.mark()
    return f"mode = {a['mode']}"


def ex_set_tz(a, ctx):
    with L:
        state["tz"] = a["tz"]
        ST.mark()
    return f"timezone = {a['tz']}"


def ex_device(a, ctx):
    al = alias_by(a["name"])
    if not al:
        raise ValueError(f"unknown device '{a['name']}'")
    cur = al.get("state")
    on = (cur != "on") if a["state"] == "toggle" else (a["state"] == "on")
    if al.get("pin"):
        high = on != bool(al.get("invert"))                  # inverted (active-low) relay modules
        INBOX.add("C", f"GPIO {al['pin']} {'ON' if high else 'OFF'}".encode())
    else:
        INBOX.add("M", (al["on"] if on else al["off"]).encode())
    with L:
        al["state"] = "on" if on else "off"
        al["state_ts"] = now_ts()
        ST.mark()
    if a.get("for_minutes"):
        add_job(time.time() + a["for_minutes"] * 60,
                [{"type": "device", "name": al["name"], "state": "off" if on else "on", "for_minutes": 0}],
                key="dev:" + al["id"])
        return f"{al['name']}: {'on' if on else 'off'} for {a['for_minutes']:g} min"
    remove_job("dev:" + al["id"])                            # a manual change cancels a pending timer
    return f"{al['name']}: {'on' if on else 'off'}"


def ex_scene(a, ctx):
    sc = scene_by(a["name"])
    if not sc:
        raise ValueError(f"unknown scene '{a['name']}'")
    c2 = dict(ctx)
    c2.update(user=False, rule=None, source="scene:" + sc["name"])
    res = run_actions(sc["actions"], c2)
    return f"scene '{sc['name']}': " + "; ".join(str(r) for r in res)[:300]


def ex_define_scene(a, ctx):
    with L:
        old = state["scenes"].get(a["name"], {})
        sid = old.get("id")
        if not sid:
            state["scn_seq"] = state.get("scn_seq", 0) + 1
            sid = f"s{state['scn_seq']}"
        state["scenes"][a["name"]] = {"id": sid, "name": a["name"], "actions": a["actions"], "desc": a.get("desc", "")}
        ST.mark()
    return f"scene '{a['name']}' saved ({len(a['actions'])} action(s))"


def ex_remove_scene(a, ctx):
    sc = scene_by(a["name"])
    with L:
        if sc:
            state["scenes"].pop(sc["name"], None)
            ST.mark()
    return "scene removed" if sc else "scene not found"


def ex_graph(a, ctx):
    tg_send(make_graph(a["channel"], a["hours"]))
    return "graph sent"


_SCREEN_ID = {"home": "home", "devices": "dev", "sensors": "sen", "scenes": "scn", "auto": "aut",
              "board": "brd", "ai": "ai", "settings": "set", "buttons": "wid"}


def ex_panel(a, ctx):
    show(_SCREEN_ID[a["screen"]], [])
    return "panel opened"


# ---- control variables (fan speed 0..255, brightness, ...) and custom panel buttons
def control_by(ref):
    n = norm(str(ref))
    with L:
        for c in state["controls"].values():
            if c.get("id") == ref or n in {norm(c["name"])} | {norm(x) for x in c.get("aliases", [])}:
                return c
    return None


def fmt_num(v):
    v = float(v)
    return str(int(v)) if v.is_integer() else f"{v:g}"


def ex_define_control(a, ctx):
    with L:
        old = state["controls"].get(a["name"], {})
        cid = old.get("id")
        if not cid:
            state["ctl_seq"] = state.get("ctl_seq", 0) + 1
            cid = f"c{state['ctl_seq']}"
        v = old.get("value")
        if v is None:
            v = a["init"] if a.get("init") is not None else a["min"]
        v = max(a["min"], min(a["max"], v))
        state["controls"][a["name"]] = {
            "id": cid, "name": a["name"], "min": a["min"], "max": a["max"], "step": a["step"], "unit": a["unit"],
            "actions": a["actions"], "critical": bool(a.get("critical")), "value": int(v) if float(v).is_integer() else v,
            "sent": False, "aliases": a.get("aliases", [])}
        ST.mark()
    return f"control '{a['name']}' {fmt_num(a['min'])}..{fmt_num(a['max'])} step {fmt_num(a['step'])}"


def ex_set_control(a, ctx):
    c = control_by(a["name"])
    if not c:
        raise ValueError(f"unknown control '{a['name']}'")
    if a.get("expr"):
        v = safe_eval(a["expr"], ctx.get("names") or {})
    elif a.get("to") == "min":
        v = c["min"]
    elif a.get("to") == "max":
        v = c["max"]
    elif a.get("delta") is not None:
        v = c["value"] + a["delta"]
    else:
        v = a["value"]
    v = max(c["min"], min(c["max"], float(v)))
    if float(c["step"]).is_integer():
        v = round(v)
    v = int(v) if float(v).is_integer() else round(v, 3)
    if ctx.get("rule") is not None and c.get("sent") and v == c["value"]:
        return f"{c['name']}: unchanged"                    # an automation computed the same value again
    with L:
        c["value"] = v
        c["sent"] = True
        ST.mark()
    names = dict(ctx.get("names") or {})
    names["value"] = v
    run_actions(c["actions"], {"user": False, "names": names, "source": "control:" + c["name"]})
    return f"{c['name']}: {fmt_num(v)}{c.get('unit', '')}"


def ex_remove_control(a, ctx):
    c = control_by(a["name"])
    with L:
        if c:
            state["controls"].pop(c["name"], None)
            state["custom"][:] = [w for w in state["custom"] if not (w["kind"] == "stepper" and norm(w["control"]) == norm(c["name"]))]
            ST.mark()
    return f"control '{a['name']}' removed (with its +/- buttons)" if c else "control not found"


def _add_widget(w, label):
    with L:
        state["custom"][:] = [x for x in state["custom"] if norm(x.get("label", "")) != norm(label)]
        if len(state["custom"]) >= 40:
            raise ValueError("too many custom buttons (max 40)")
        state["btn_seq"] = state.get("btn_seq", 0) + 1
        w["id"] = f"w{state['btn_seq']}"
        state["custom"].append(w)
        ST.mark()


def ex_add_button(a, ctx):
    _add_widget({"kind": "button", "label": a["label"], "actions": a["actions"], "same_row": a["same_row"]}, a["label"])
    return f"button '{a['label']}' added to the panel"


def ex_add_stepper(a, ctx):
    c = control_by(a["control"])
    if not c:
        raise ValueError(f"unknown control '{a['control']}' (define_control first)")
    label = a.get("label") or c["name"]
    _add_widget({"kind": "stepper", "control": c["name"], "label": label, "step": a.get("step"),
                 "big_step": a.get("big_step")}, label)
    return f"+/- buttons for '{c['name']}' added to the panel"


def ex_remove_button(a, ctx):
    n = norm(a["label"])
    with L:
        before = len(state["custom"])
        state["custom"][:] = [w for w in state["custom"] if w["id"] != a["label"] and norm(w["label"]) != n]
        removed = before - len(state["custom"])
        if removed:
            ST.mark()
    return f"button '{a['label']}' removed from the panel" if removed else f"no button named '{a['label']}'"


EXEC = {"serial": ex_serial, "esp": ex_esp, "send": ex_send, "report": ex_report,
        "define_alias": ex_define_alias, "remove_alias": ex_remove_alias,
        "define_command": ex_define_command, "remove_command": ex_remove_command,
        "define_channel": ex_define_channel, "remove_channel": ex_remove_channel,
        "add_rule": ex_add_rule, "remove_rule": ex_remove_rule,
        "add_schedule": ex_add_schedule, "remove_schedule": ex_remove_schedule,
        "note": ex_note, "set_mode": ex_set_mode, "set_tz": ex_set_tz,
        "device": ex_device, "scene": ex_scene, "define_scene": ex_define_scene, "remove_scene": ex_remove_scene,
        "graph": ex_graph, "panel": ex_panel,
        "define_control": ex_define_control, "set_control": ex_set_control, "remove_control": ex_remove_control,
        "add_button": ex_add_button, "add_stepper": ex_add_stepper, "remove_button": ex_remove_button}


def run_query(q):
    st = series_stats(q["channel"], q["hours"])
    if st.get("n", 0) == 0:
        return {"channel": q["channel"], "hours": q["hours"], "result": None, "n": 0}
    agg = q["agg"]
    return {"channel": q["channel"], "hours": q["hours"], "agg": agg,
            "result": st["n"] if agg == "count" else st[agg], "n": st["n"]}


def run_actions(actions, ctx):
    """Execute normalized actions. Returns a list of result strings. Critical ones may need confirmation."""
    results = []
    held = []
    for i, a in enumerate(actions):
        try:
            if (ctx.get("user") and not ctx.get("confirmed") and state.get("confirm_critical", True)
                    and is_critical(a)):
                held.append(a)
                continue
            ctx["idx"] = i
            if a["type"] == "query":
                results.append(run_query(a))
                continue
            if not ctx.get("user") and a["type"] in ("serial", "esp", "device"):
                if not breaker_ok():
                    results.append("skipped: safety breaker is active")
                    continue
                breaker_count()
            r = EXEC[a["type"]](a, ctx)
            results.append(r)
            if isinstance(r, str):
                audit(ctx.get("source", "?"), r)
        except Exception as e:                    # one bad action must not stop the others
            results.append(f"error in {a.get('type')}: {e}")
    if held:
        pid = uuid.uuid4().hex[:8]
        with L:
            state["pending"][pid] = {"actions": held, "ts": now_ts()}
            for k in [k for k, v in state["pending"].items() if now_ts() - v["ts"] > 900]:
                state["pending"].pop(k, None)
            ST.mark()
        summary = "; ".join(describe(a) for a in held)[:700]
        tg_send(f"⚠️ Confirmation needed:\n{summary}",
                buttons=[[{"label": "✅ Confirm", "raw_actions": [{"type": "_confirm", "id": pid, "yes": True}]},
                          {"label": "❌ Cancel", "raw_actions": [{"type": "_confirm", "id": pid, "yes": False}]}]])
        results.append(f"waiting for user confirmation ({len(held)} critical action(s))")
    return results


def describe(a):
    t = a["type"]
    if t == "serial":
        return f"send '{a['text']}' to serial"
    if t == "esp":
        return f"board: {a['cmd']} {a['args']}".strip()
    if t in ("add_rule", "add_schedule"):
        return f"{t} '{a['id']}' containing a critical action"
    return t


# ----------------------------------------------------------------- rules & schedules
def cond_true(rule, v):
    op = rule["op"]
    if op == "any":
        return True
    if not isinstance(v, (int, float)):
        return False
    return _CMP[{">": ast.Gt, "<": ast.Lt, ">=": ast.GtE, "<=": ast.LtE, "==": ast.Eq, "!=": ast.NotEq}[op]](v, rule["value"])


def guard_ok(expr, names):
    if not expr:
        return True
    try:
        return bool(safe_eval(expr, names))
    except Exception:
        return False


def eval_rules(cid, v):
    with L:
        todo = [(rid, r) for rid, r in state["rules"].items() if r["channel"] == cid and r.get("enabled", True)]
    for rid, r in todo:
        try:
            now = time.time()
            true = cond_true(r, v)
            fire = False
            if true and (r["op"] == "any" or r["repeat"] or not r.get("active")):
                fire = now - r.get("last_fire", 0) >= r.get("cooldown_s", 0)
            r["active"] = bool(true) if r["op"] != "any" else False
            if fire:
                names = build_names({"value": v})
                if not guard_ok(r.get("when"), names):
                    continue
                r["last_fire"] = now
                ST.mark()
                run_actions(r["actions"], {"user": False, "names": names, "source": "rule:" + rid, "rule": r})
                INBOX.flush()
        except Exception as e:
            log(f"rule {rid} error: {e.__class__.__name__}")


def schedule_due(sc, now):
    if sc["kind"] == "every":
        return time.time() - sc.get("last_run", 0) >= sc.get("minutes", 60) * 60
    if sc["kind"] == "daily":
        hh, mm = map(int, sc["time"].split(":"))
        if sc.get("days") is not None and now.weekday() not in sc["days"]:
            return False
        at = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if now < at or now - at > timedelta(minutes=15):
            return False
        return sc.get("last_date") != now.strftime("%Y-%m-%d")
    return False


def scheduler_loop():
    while not stop.is_set():
        try:
            now = tznow()
            with L:
                items = list(state["schedules"].items())
            for sid, sc in items:
                if sc.get("enabled", True) and schedule_due(sc, now):
                    sc["last_run"] = time.time()
                    sc["last_date"] = now.strftime("%Y-%m-%d")
                    ST.mark()
                    names = build_names()
                    if not guard_ok(sc.get("when"), names):
                        continue
                    log(f"schedule {sid} fired")
                    run_actions(sc["actions"], {"user": False, "names": names, "source": "schedule:" + sid})
                    INBOX.flush()
            run_due_jobs()
            check_stale()
            watchdog()
        except Exception as e:
            log(f"scheduler error: {e.__class__.__name__}")
        stop.wait(20)


# ----------------------------------------------------------------- AI keys & providers
PRESETS = {
    "anthropic": ("anthropic", "https://api.anthropic.com", "claude-haiku-5-5"),
    "openai": ("openai", "https://api.openai.com/v1", "gpt-4o-mini"),
    "gemini": ("openai", "https://generativelanguage.googleapis.com/v1beta/openai", "gemini-2.5-flash"),
    "groq": ("openai", "https://api.groq.com/openai/v1", "llama-3.3-70b-versatile"),
    "openrouter": ("openai", "https://openrouter.ai/api/v1", "openrouter/auto"),
    "deepseek": ("openai", "https://api.deepseek.com", "deepseek-chat"),
    "mistral": ("openai", "https://api.mistral.ai/v1", "mistral-small-latest"),
    "custom": ("openai", "", ""),
}


def add_key(provider, key, model=None, base_url=None):
    provider = provider.lower()
    if provider not in PRESETS:
        raise ValueError("provider must be one of: " + ", ".join(PRESETS))
    typ, base, dmodel = PRESETS[provider]
    base = base_url or base
    model = model or dmodel
    if not base or not model:
        raise ValueError("custom provider needs: /addkey custom KEY MODEL BASE_URL")
    with L:
        for k in KEYS.data["keys"]:
            if k["key"] == key:
                k.update(provider=provider, type=typ, base_url=base, model=model, until=0, disabled=False)
                KEYS.mark()
                return k
        n = max([int(k["id"][1:]) for k in KEYS.data["keys"]] + [0]) + 1
        k = {"id": f"k{n}", "provider": provider, "type": typ, "base_url": base, "model": model, "key": key,
             "until": 0, "last_ok": 0, "last_error": ""}
        KEYS.data["keys"].append(k)
        KEYS.mark()
        return k


def ordered_keys():
    now = time.time()
    with L:
        ks = [k for k in KEYS.data["keys"] if k.get("until", 0) <= now]
    best = max(ks, key=lambda k: k.get("last_ok", 0), default=None)
    if best and now - best.get("last_ok", 0) < 3600:
        ks.remove(best)
        ks.insert(0, best)
    return ks


def mark_ok(k):
    with L:
        k["last_ok"] = time.time()
        k["last_error"] = ""
        k["until"] = 0
        KEYS.mark()


def mark_fail(k, status, snippet):
    now = time.time()
    low = (snippet or "").lower()
    with L:
        k["last_error"] = f"{status} {snippet}"[:120]
        if status in (401, 403):
            k["until"] = now + 12 * 3600
        elif status == 402:
            k["until"] = now + 6 * 3600
        elif status == 429:
            quota = any(w in low for w in ("quota", "insufficient", "billing", "exceeded your", "credit"))
            k["until"] = now + (6 * 3600 if quota else 90)
        elif status in (400, 404):
            k["until"] = now + 3600
        else:
            k["until"] = now + 60
        KEYS.mark()


def llm_call(k, system, msgs):
    """Returns (http_status, text_or_error)."""
    base = k["base_url"].rstrip("/")
    if k["type"] == "anthropic":
        r = requests.post(base + "/v1/messages", timeout=60,
                          headers={"x-api-key": k["key"], "anthropic-version": "2023-06-01",
                                   "content-type": "application/json"},
                          json={"model": k["model"], "max_tokens": AI_MAX_TOKENS, "system": system, "messages": msgs})
        if r.status_code != 200:
            return r.status_code, r.text[:150]
        j = r.json()
        return 200, "".join(b.get("text", "") for b in j.get("content", []) if b.get("type") == "text")
    r = requests.post(base + "/chat/completions", timeout=60,
                      headers={"Authorization": f"Bearer {k['key']}", "content-type": "application/json"},
                      json={"model": k["model"], "max_tokens": AI_MAX_TOKENS, "temperature": 0.2,
                            "messages": [{"role": "system", "content": system}] + msgs})
    if r.status_code != 200:
        return r.status_code, r.text[:150]
    try:
        return 200, r.json()["choices"][0]["message"]["content"] or ""
    except (KeyError, IndexError, ValueError):
        return 502, "unexpected response shape"


KEY_PREFIXES = [("sk-ant-", "anthropic"), ("sk-or-", "openrouter"), ("gsk_", "groq"), ("AIza", "gemini"),
                ("sk-proj-", "openai"), ("sk-svcacct-", "openai")]
PROVIDER_ALIASES = {"claude": "anthropic", "gpt": "openai", "chatgpt": "openai", "google": "gemini", "llama": "groq"}
PREFER = {
    "anthropic": ["haiku", "sonnet"],
    "openai": ["gpt-4o-mini", "gpt-4.1-mini", "mini", "gpt-4o", "gpt-"],
    "gemini": ["flash", "pro"],
    "groq": ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "versatile", "instant", "llama"],
    "openrouter": ["openrouter/auto"],
    "deepseek": ["deepseek-chat"],
    "mistral": ["mistral-small-latest", "small", "mistral-large-latest", "mistral"],
}
_BAD = ("embed", "whisper", "tts", "audio", "guard", "moderation", "image", "dall", "transcribe", "vision", "rerank",
        "speech", "realtime", "search", "davinci", "babbage", "computer-use", "live", "imagen", "veo", "gemma")
_DATE = re.compile(r"\d{8}|\d{4}-\d{2}-\d{2}")
PENDING_KEY = {"key": None, "ts": 0.0}


def looks_like_key(text):
    t = text.strip()
    return 20 <= len(t) <= 250 and not any(c.isspace() for c in t) and any(t.startswith(p) for p, _ in KEY_PREFIXES)


def detect_provider(key):
    for prefix, prov in KEY_PREFIXES:
        if key.startswith(prefix):
            return prov
    if re.fullmatch(r"sk-[0-9a-f]{32}", key):
        return "deepseek"
    for prov in ("openai", "mistral", "deepseek"):          # unknown format: whichever service accepts it
        try:
            r = requests.get(PRESETS[prov][1].rstrip("/") + "/models", headers={"Authorization": f"Bearer {key}"}, timeout=15)
            if r.status_code == 200:
                return prov
        except requests.RequestException:
            pass
    return None


def list_models(k):
    """Returns (ids or None, http_status)."""
    base = k["base_url"].rstrip("/")
    try:
        if k["type"] == "anthropic":
            r = requests.get(base + "/v1/models", params={"limit": 100}, timeout=20,
                             headers={"x-api-key": k["key"], "anthropic-version": "2023-06-01"})
        else:
            r = requests.get(base + "/models", headers={"Authorization": f"Bearer {k['key']}"}, timeout=20)
        if r.status_code != 200:
            return None, r.status_code
        j = r.json()
        items = j.get("data") or j.get("models") or []
        return [str(m.get("id") or m.get("name") or "") for m in items if isinstance(m, dict)], 200
    except (requests.RequestException, ValueError):
        return None, 0


def rank_models(provider, ids):
    ids = [i.split("/", 1)[1] if i.startswith("models/") else i for i in ids if i]
    good = [i for i in ids if not any(b in i.lower() for b in _BAD)] or list(ids)
    ranked = []
    for pref in PREFER.get(provider, []):
        c = [i for i in good if pref in i.lower() and i not in ranked]
        if provider == "gemini":
            c = [i for i in c if not any(x in i for x in ("preview", "exp", "lite", "latest"))] or c
        c.sort(reverse=True)                                 # newest-looking version first
        c.sort(key=lambda i: bool(_DATE.search(i)))          # stable: undated aliases before dated snapshots
        ranked += c
    ranked += [i for i in good if i not in ranked]
    return ranked[:5]


def _test_key(k):
    try:
        return llm_call(k, "Reply with the single word OK.", [{"role": "user", "content": "ping"}])
    except requests.RequestException as e:
        return 0, e.__class__.__name__


def fix_model(k):
    """The saved model name no longer exists: ask the service which models it has and switch to a good one."""
    ids, _ = list_models(k)
    if not ids:
        return False
    for m in rank_models(k["provider"], ids):
        if m != k["model"]:
            with L:
                k["model"] = m
                KEYS.mark()
            return True
    return False


def setup_key(provider, key, model=None, base_url=None):
    """Add, test and (if the default model is gone) auto-pick a working model. Returns (key, ok, message)."""
    k = add_key(provider, key, model, base_url)
    st, out = _test_key(k)
    if st != 200 and not model and st in (400, 404, 422):
        ids, _ = list_models(k)
        for m in (rank_models(k["provider"], ids) if ids else []):
            if m == k["model"]:
                continue
            k["model"] = m
            st, out = _test_key(k)
            if st == 200:
                break
    if st == 200:
        mark_ok(k)
        KEYS.save(force=True)
        return k, True, T("key_ok", id=k["id"], provider=k["provider"], model=k["model"])
    mark_fail(k, st, out)
    KEYS.save(force=True)
    return k, False, T("key_fail", id=k["id"], err=f"{st} {str(out)[:100]}")


def call_llm(system, msgs):
    """Try every available key in turn. Returns text or None."""
    for k in ordered_keys():
        st, out = 0, ""
        for attempt in range(2):
            try:
                st, out = llm_call(k, system, msgs)
            except requests.RequestException as e:
                st, out = 0, e.__class__.__name__
            if st == 200 and out.strip():
                mark_ok(k)
                return out
            if attempt == 0 and st in (400, 404) and "model" in str(out).lower() and fix_model(k):
                log(f"AI key {k['id']}: model replaced automatically")
                continue
            break
        mark_fail(k, st, out)
        log(f"AI key {k['id']} ({k['provider']}) failed: {st}")
    return None


def alternate(msgs):
    """Strictly alternating user/assistant messages starting with user (required by some APIs)."""
    out = []
    for m in msgs:
        if out and out[-1]["role"] == m["role"]:
            out[-1]["content"] += "\n" + m["content"]
        else:
            out.append(dict(m))
    while out and out[0]["role"] != "user":
        out.pop(0)
    return out


# ----------------------------------------------------------------- AI prompt
SYSTEM_PROMPT = """You are the autonomous controller ("agent") of a hobby IoT project.
- A WeMos D1 (ESP8266) board is the gateway between the user's Telegram bot and a serial port (UART).
- Text you send with a "serial" action is printed RAW on that serial port; a device on it (e.g. an Arduino Uno) reads it.
- Lines the serial device prints come back as "device data". Some lines are parsed into numeric CHANNELS (sensors).
- You can also control the gateway board itself with an "esp" action.
You work ONLY by emitting ACTIONS. Reply with exactly ONE JSON object and nothing else (no markdown fences).

OUTPUT
{"reply": "<short text for the user, SAME LANGUAGE as the user>",
 "buttons": [[{"label": "..", "actions": [<serial|esp|send|report actions>]}]],   // optional inline buttons
 "menu": [["..",".."]],                                                           // optional persistent keyboard
 "actions": [ {...}, ... ]}
Every field is optional. Keep "reply" under 400 characters. If you only answer a question, give no actions.

ACTIONS  (each is an object with "type")
serial            {text, critical?}                  send raw text to the serial device. Templates: {{ expression }}
esp               {cmd, args}                        cmd: STATUS | PING host | SCAN | ADC | LED (ON|OFF|AUTO) | GPIO ("D1 ON|OFF|TOGGLE|READ|PWM 0-1023|PULSE ms"; PWM may be a formula: "D2 PWM {{ clamp(round((value-25)*100),0,1023) }}") | WIFI (args: "TRY ssid|password", "LIST", "FORGET ssid", "RECONNECT") | REBOOT
send              {text}                             Telegram message (templates allowed)
report            {channels?, hours, title?}         avg/min/max/last of channels, sent to Telegram
device            {name, state:"on|off|toggle", for_minutes?}   switch a defined device. for_minutes = automatic timer back to the opposite state
scene             {name}                             run a stored scene
define_scene      {name, actions:[...], desc?}       named group of actions (device/serial/esp/send/report/graph). No nested scenes
remove_scene      {name}
graph             {channel, hours}                   sparkline chart of a channel, sent to Telegram
panel             {screen}                           open the control panel: home|devices|sensors|scenes|auto|board|ai|settings|buttons
define_control    {name, min, max, step, unit?, init?, aliases?, actions:[...], critical?}   a numeric control variable such as fan speed 0..255. aliases = other names the user may say. "actions" run with {{ value }} = the new value, e.g. [{"type":"serial","text":"FAN{{ value }}"}] or [{"type":"esp","cmd":"GPIO","args":"D2 PWM {{ value }}"}]
set_control       {name, value | delta | to:"min"|"max" | expr}   change a control. expr = formula, e.g. "clamp((temp-25)*25,0,255)"
remove_control    {name}                             also removes its +/- buttons
add_button        {label, actions:[...], same_row?}  a custom button on the Telegram panel (home screen). actions may be serial, esp, send, report, device, scene, graph, set_control
add_stepper       {control, label?, step?, big_step?}   a  [-] value [+]  row on the panel for a control (big_step adds fast buttons)
remove_button     {label}                            remove a custom button or stepper from the panel
define_alias      {name, code, on?, off?, pin?, invert?, aliases?, desc?, critical?}   a device. aliases = other names/languages the user may say (e.g. ["لامپ x","lamp x"]). With pin (D0 D1 D2 D5 D6 D7) the BOARD itself drives that GPIO (relay!); invert:true for active-low relay modules. Without pin it prints on/off text on serial (default code+"ON"/"OFF")
remove_alias      {name}
define_command    {phrases:[...], serial:"text" or ["line1","line2"], desc?, critical?}   fast exact-phrase shortcut that works WITHOUT AI
remove_command    {phrase}
define_channel    {id, regex, unit?, desc?, forward?, stale_min?}   regex with ONE capture group for the value, e.g. "^DX\\s*(-?[0-9.]+)". forward=false hides these lines from Telegram. stale_min = alert when no data for N minutes
remove_channel    {id}
add_rule          {id, channel, op, value, actions:[...], cooldown_s?, repeat?, when?}   op: > < >= <= == != any ("any" = every new sample). when = extra condition, e.g. "hour>=18 and hour<23"
remove_rule       {id}
add_schedule      {id, kind:"daily", time:"HH:MM", days?:["mon",..]}  or  {id, kind:"every", minutes:N}   plus "actions":[...] and optional "when" condition
remove_schedule   {id}
note              {text}                             remember a fact/algorithm in your notes file (brain.md)
query             {channel, hours, agg}              agg: avg|min|max|last|count. Result comes back to you in TOOL_RESULTS
set_mode          {mode:"agent"|"raw"}
set_tz            {tz:"Area/City"}
Inside rules/schedules/buttons only serial, esp, send, report, device, scene, graph are allowed.

TEMPLATES  {{ expr }} may use: value (the triggering sample), every channel id, avg24_<channel>, hour, minute, weekday,
min max round int abs floor ceil clamp(x,lo,hi), + - * / %, comparisons, "a if cond else b".
Example fan speed from temperature channel "temp": {"type":"serial","text":"FAN {{ clamp(round((value-25)*10),0,100) }}"}

CONDUCT
- Use ONLY serial codes the user defined or confirmed. If asked to control something unknown, ask which serial text to use. Never invent codes.
- For defined devices use the "device" action (not raw serial). A device with a pin is driven directly by the board; if the user wants a new relay and gave no pin, ask which pin (D0 D1 D2 D5 D6 D7; D3/D4/D8 are not allowed). Relay modules are often active-low: set invert:true if the user says so.
- A lamp/relay toggle button = add_button whose actions are [{"type":"device","name":"<device>","state":"toggle"}] (its label then shows 🟢/⚪ automatically). Make sure the device exists first (define_alias; ask for its serial code or pin if unknown).
- A "+/-" fan or dimmer: define_control (min, max, step, output format) THEN add_stepper. If the user did not say what text the device expects, propose CODE+value (e.g. FAN120) and ask once. "to remove a button" => remove_button; to remove the variable => remove_control.
- To learn the device's serial format, read RECENT SERIAL LINES below. One line can carry several values (T25.3 H60): define ONE channel per value, each regex with ONE capture group (e.g. "T([0-9.]+)" and "H([0-9.]+)"), then say which lines they parse.
- "turn on X for N minutes"
- When you add a menu or buttons for shortcuts, ALSO add define_command entries for the same labels so they work without AI.
- Mark critical:true for anything dangerous or hard to undo (heaters, pumps, locks, mains power, reboot, Wi-Fi changes). The system asks the user to confirm.
- Prefer rules/schedules over repeated chatting. Times are in the user's timezone shown below.
- Do not repeat actions already listed as executed in TOOL_RESULTS. Never reveal API keys or passwords.
- If something is impossible or unclear, say so briefly and ask ONE question.
- Existing definitions are listed below: modify them with define_/remove_ rather than duplicating.
"""


def build_context():
    n = tznow()
    parts = [f"NOW: {n.strftime('%Y-%m-%d %H:%M')} ({state.get('tz')}), weekday={n.strftime('%a')}", f"MODE: {state.get('mode')}"]
    with L:
        al = [f"  {a['name']} [{a.get('id')}] = {a['code']} "
              + (f"pin {a['pin']}{' inverted' if a.get('invert') else ''}" if a.get("pin") else f"(on:{a['on']} off:{a['off']})")
              + f" state={a.get('state') or 'unknown'}{' CRITICAL' if a.get('critical') else ''} {a.get('desc') or ''}"
              + (f" aka {a['aliases']}" if a.get("aliases") else "")
              for a in state["aliases"].values()]
        scn = [f"  {x['name']}: {json.dumps(x['actions'], ensure_ascii=False)}" for x in state["scenes"].values()]
        njobs = len(state["jobs"])
        cm = [f"  \"{c['phrase']}\" -> {c['serial']}{' CRITICAL' if c.get('critical') else ''}" for c in list(state["commands"].values())[:60]]
        chs = []
        for cid, c in state["channels"].items():
            last = DATA.data["last"].get(cid)
            age = f"{int(time.time() - last[0])}s ago" if last else "never"
            st = series_stats(cid, 24)
            chs.append(f"  {cid}: regex={c['regex']} unit={c.get('unit','')} forward={c.get('forward')} last={last[1] if last else None} ({age}) avg24={st.get('avg')} n24={st.get('n')}")
        ru = [f"  {rid}: {r['channel']} {r['op']} {r['value']} -> {json.dumps(r['actions'], ensure_ascii=False)} cooldown={r['cooldown_s']}"
              for rid, r in state["rules"].items()]
        sc = [f"  {sid}: {s['kind']} {s.get('time') or str(s.get('minutes')) + 'min'} days={s.get('days')} -> {json.dumps(s['actions'], ensure_ascii=False)}"
              for sid, s in state["schedules"].items()]
        esp = state["esp"].get("last")
        notes = NOTES.data[-3500:]
    parts.append("ALIASES:\n" + ("\n".join(al) or "  (none)"))
    parts.append("FAST COMMANDS:\n" + ("\n".join(cm) or "  (none)"))
    parts.append("CHANNELS:\n" + ("\n".join(chs) or "  (none)"))
    parts.append("RULES:\n" + ("\n".join(ru) or "  (none)"))
    parts.append("SCHEDULES:\n" + ("\n".join(sc) or "  (none)"))
    with L:
        ctl = [f"  {c['name']} [{c['id']}]: {fmt_num(c['value'])}{c.get('unit', '')} range {fmt_num(c['min'])}..{fmt_num(c['max'])} step {fmt_num(c['step'])} -> {json.dumps(c['actions'], ensure_ascii=False)}"
               + (f" aka {c['aliases']}" if c.get("aliases") else "") for c in state["controls"].values()]
        wid = [f"  {w['label']} ({w['kind']}{' of ' + w['control'] if w['kind'] == 'stepper' else ''})" for w in state["custom"]]
        recent = [f"  {fmt_age(time.time() - r[0])} ago: {r[1][:100]!r} -> " + (",".join(r[2]) if len(r) > 2 and r[2] else "(not parsed, forwarded to Telegram)")
                  for r in list(DEVLOG)[-12:]]
    parts.append("CONTROLS:\n" + ("\n".join(ctl) or "  (none)"))
    parts.append("CUSTOM PANEL BUTTONS:\n" + ("\n".join(wid) or "  (none)"))
    parts.append("RECENT SERIAL LINES FROM THE DEVICE:\n" + ("\n".join(recent) or "  (none yet)"))
    parts.append("SCENES:\n" + ("\n".join(scn) or "  (none)"))
    parts.append(f"ACTIVE TIMERS: {njobs}")
    parts.append("BOARD (last report): " + (esp["text"] if esp else "none yet"))
    parts.append("NOTES (brain.md, most recent):\n" + notes)
    return "\n\n".join(parts)


def parse_json_reply(text):
    t = text.strip()
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t, flags=re.S)
    a, b = t.find("{"), t.rfind("}")
    if a < 0 or b <= a:
        return None
    try:
        obj = json.loads(t[a:b + 1])
        return obj if isinstance(obj, dict) else None
    except ValueError:
        return None


def ai_turn(text):
    """Returns True if the AI handled the message, False if no AI could be used."""
    if not KEYS.data["keys"]:
        return False
    tg_typing()
    system = SYSTEM_PROMPT + "\n--- CURRENT STATE ---\n" + build_context()
    with L:
        hist = list(state["history"])
    msgs = alternate(hist + [{"role": "user", "content": text}])
    replies, executed = [], []
    buttons = menu = None
    for rnd in range(3):
        out = call_llm(system, msgs)
        if out is None:
            return False if rnd == 0 else _finish(text, replies, executed, buttons, menu, partial=True)
        obj = parse_json_reply(out)
        if obj is None:
            replies = [out.strip()[:1500]]
            break
        if obj.get("reply"):
            replies.append(str(obj["reply"]))
        buttons = obj.get("buttons") or buttons
        menu = obj.get("menu") or menu
        acts, errs = [], []
        for a in (obj.get("actions") or [])[:12]:
            try:
                acts.append(normalize_action(a))
            except Exception as e:
                errs.append(f"invalid action ({e})")
        queries = [a for a in acts if a["type"] == "query"]
        others = [a for a in acts if a["type"] != "query"]
        res = run_actions(others, {"user": True, "names": build_names(), "source": "ai"}) + errs
        executed += [r for r in res if isinstance(r, str)]
        if queries:
            qres = [run_query(q) for q in queries]
            replies.clear()
            msgs = alternate(msgs + [{"role": "assistant", "content": out},
                                     {"role": "user", "content": "TOOL_RESULTS: " + json.dumps(
                                         {"executed": res, "queries": qres}, ensure_ascii=False)}])
            continue
        break
    return _finish(text, replies, executed, buttons, menu)


def _finish(user_text, replies, executed, buttons, menu, partial=False):
    reply = "\n".join(r for r in replies if r).strip()
    if not reply and executed:
        reply = "✓ " + "\n✓ ".join(executed[:8])
    elif executed and any(x.startswith("error") for x in executed):
        reply += "\n" + "\n".join(x for x in executed if x.startswith("error"))
    if not reply:
        reply = "OK"
    kb = [[str(c) for c in row] for row in menu] if isinstance(menu, list) and menu and isinstance(menu[0], list) else None
    inline = None
    if isinstance(buttons, list) and buttons and isinstance(buttons[0], list):
        inline = [[{k: v for k, v in b.items() if k in ("label", "actions", "serial")}
                   for b in row if isinstance(b, dict)] for row in buttons if isinstance(row, list)]
    tg_send(reply, buttons=inline, keyboard=kb)
    with L:
        state["history"] += [{"role": "user", "content": user_text[:600]}, {"role": "assistant", "content": reply[:600]}]
        state["history"] = state["history"][-8:]
        ST.mark()
    INBOX.flush()
    return True


# ----------------------------------------------------------------- graphs
SPARK = "▁▂▃▄▅▆▇█"


def sparkline(points, hours, width=24):
    now = now_ts()
    start = now - hours * 3600
    size = hours * 3600 / width
    buckets = [[] for _ in range(width)]
    for t, v in points:
        if t >= start and isinstance(v, (int, float)):
            buckets[min(width - 1, int((t - start) / size))].append(v)
    avgs = [sum(b) / len(b) if b else None for b in buckets]
    vals = [x for x in avgs if x is not None]
    if not vals:
        return None
    lo, hi = min(vals), max(vals)
    return "".join("·" if x is None else SPARK[0 if hi == lo else int((x - lo) / (hi - lo) * 7 + 0.5)] for x in avgs)


def make_graph(cid, hours):
    with L:
        ser = list(DATA.data["series"].get(cid, []))
    sp = sparkline(ser, hours)
    if not sp:
        return T("graph_nodata", ch=cid)
    st = series_stats(cid, hours)
    unit = (state["channels"].get(cid) or {}).get("unit", "")
    span = f"{hours}h" if hours < 48 else f"{hours // 24}d"
    return (f"📈 {cid} · {span}\n{sp}\n⏪ {span}  ⏩ now\n"
            f"min {st['min']}{unit} · avg {st['avg']}{unit} · max {st['max']}{unit} · now {st['last']}{unit}")


# ----------------------------------------------------------------- Telegram panel (inline UI that edits itself in place)
def inline_rows(rows):
    return {"inline_keyboard": [[{"text": str(l)[:40], "callback_data": d[:64]} for l, d in row] for row in rows if row]}


def tg_send_rows(text, rows):
    try:
        tg("sendMessage", {"chat_id": TG_CHAT, "text": text[:4000], "disable_web_page_preview": True,
                           "reply_markup": inline_rows(rows)})
    except requests.RequestException as e:
        log(f"panel send error {e.__class__.__name__}")


def tg_edit(mid, text, rows):
    try:
        code, data = tg("editMessageText", {"chat_id": TG_CHAT, "message_id": mid, "text": text[:4000],
                                            "disable_web_page_preview": True, "reply_markup": inline_rows(rows)})
        if not data.get("ok") and "not modified" not in str(data.get("description", "")):
            log(f"edit failed: {str(data.get('description'))[:60]}")
    except requests.RequestException as e:
        log(f"panel edit error {e.__class__.__name__}")


def tg_typing():
    try:
        tg("sendChatAction", {"chat_id": TG_CHAT, "action": "typing"}, timeout=5)
    except requests.RequestException:
        pass


def tg_document(name, content: bytes, caption=""):
    try:
        r = requests.post(f"{TG}/sendDocument", data={"chat_id": TG_CHAT, "caption": caption[:900]},
                          files={"document": (name, content)}, timeout=40)
        return r.status_code == 200
    except requests.RequestException:
        return False


def home_keyboard():
    return [[T("kb_panel"), T("devices")], [T("sensors"), T("board")]]


CMD_DESC = {
    "fa": [("panel", "پنل کنترل"), ("devices", "دستگاه‌ها"), ("sensors", "سنسورها"), ("scenes", "صحنه‌ها"),
           ("auto", "قانون‌ها و زمان‌بندها"), ("buttons", "دکمه‌های من"), ("board", "کنترل برد"), ("graph", "نمودار سنسور"), ("log", "آخرین خطوط دستگاه"),
           ("history", "تاریخچهٔ کارها"), ("export", "خروجی تنظیمات"), ("keys", "کلیدهای هوش مصنوعی"),
           ("status", "وضعیت"), ("lang", "زبان"), ("help", "راهنما")],
    "en": [("panel", "Control panel"), ("devices", "Devices"), ("sensors", "Sensors"), ("scenes", "Scenes"),
           ("auto", "Rules and schedules"), ("buttons", "My buttons"), ("board", "Board control"), ("graph", "Sensor graph"), ("log", "Last device lines"),
           ("history", "Action history"), ("export", "Export settings"), ("keys", "AI keys"),
           ("status", "Status"), ("lang", "Language"), ("help", "Help")],
}


def set_commands():
    try:
        tg("setMyCommands", {"commands": [{"command": c, "description": d} for c, d in CMD_DESC.get(state.get("lang", "fa"), CMD_DESC["en"])]})
    except requests.RequestException:
        pass


def board_line():
    if not BOARD["seen"]:
        return T("board_unknown")
    return T("board_offline" if BOARD["alerted"] else "board_online", age=fmt_age(time.time() - BOARD["seen"]))


def dev_icon(d):
    return "🟢" if d.get("state") == "on" else ("⚪" if d.get("state") == "off" else "❔")


def parse_status(line):
    d = {}
    m = re.search(r"ssid=(.*?)(?= \w+=|$)", line)
    if m:
        d["ssid"] = m.group(1)
    for k, v in re.findall(r"(\w+)=(\S+)", line):
        d.setdefault(k, v)
    return d


def pretty_status(line):
    d = parse_status(line)
    online = d.get("online") == "1"
    try:
        up = fmt_age(int(d.get("uptime_s", 0)))
    except ValueError:
        up = "?"
    try:
        heap = f"{int(d.get('heap', 0)) // 1024} KB"
    except ValueError:
        heap = "?"
    return (f"📶 {T('wifi')}: {d.get('ssid') or '—'} ({d.get('rssi', '?')} dBm) · IP {d.get('ip', '?')}\n"
            f"🌐 {T('internet')}: {'✅' if online else '❌'}\n"
            f"⏱ {T('uptime')}: {up} · 🧠 {heap}\n"
            f"↗ {d.get('sent', '0')}  ↙ {d.get('recv', '0')}  ⚠️ {d.get('err', '0')}  🔁 {d.get('reconnects', '0')} · fw {d.get('fw', '?')}")


def pretty_ping(line):
    kv = dict(re.findall(r"(\w+)=(\S+)", line))
    if " ERR " in line or "host" not in kv:
        return "🏓 " + line
    ms = lambda k: "✗" if kv.get(k, "-1") == "-1" else f"{kv[k]} ms"
    return f"🏓 {kv['host']} ({kv.get('ip', '?')})\nDNS {ms('dns_ms')} · ICMP {ms('icmp_ms')} · TCP {ms('tcp443_ms')}"


def request_board_status(mid=None):
    """Ask the board for STATUS; when the answer arrives the panel message is edited in place."""
    INBOX.add("C", b"STATUS")
    INBOX.flush()
    with L:
        state["esp"]["waiting"] = time.time()
        state["esp"]["panel_mid"] = mid
        ST.mark()


def widget_by(wid):
    with L:
        for w in state["custom"]:
            if w["id"] == wid:
                return w
    return None


def widget_rows():
    """Buttons the user created by chat. A device toggle button shows 🟢/⚪; a stepper is  ➖ value ➕."""
    rows = []
    with L:
        ws = list(state["custom"])
    for w in ws:
        if w["kind"] == "button":
            label, acts = w["label"], w["actions"]
            if len(acts) == 1 and acts[0]["type"] == "device" and acts[0].get("state") == "toggle":
                al = alias_by(acts[0]["name"])
                if al:
                    label = f"{dev_icon(al)} {label}"
            btn = (label, f"m:cb:{w['id']}")
            if w.get("same_row") and rows and all(b[1].startswith("m:cb:") for b in rows[-1]) and len(rows[-1]) < 3:
                rows[-1].append(btn)
            else:
                rows.append([btn])
        else:
            c = control_by(w["control"])
            if not c:
                continue
            val = f"{w['label']}: {fmt_num(c['value'])}{c.get('unit', '')}"
            row = [("➖", f"m:cs:{w['id']}:-"), (val, f"m:cs:{w['id']}:r"), ("➕", f"m:cs:{w['id']}:+")]
            if w.get("big_step"):
                row = [("⏪", f"m:cs:{w['id']}:--")] + row + [("⏩", f"m:cs:{w['id']}:++")]
            rows.append(row)
    return rows


def live_line():
    parts = []
    with L:
        for cid, ch in list(state["channels"].items())[:6]:
            last = DATA.data["last"].get(cid)
            if last:
                parts.append(f"{cid} {last[1]}{ch.get('unit', '')}")
    return "📊 " + " · ".join(parts) if parts else ""


def scr_home(args):
    with L:
        devs = list(state["aliases"].values())
        n_on = sum(1 for d in devs if d.get("state") == "on")
        n_auto = len(state["rules"]) + len(state["schedules"])
        text = (f"{T('title')}\n\n{board_line()}\n"
                f"{T('ai_line', ready=len(ordered_keys()), total=len(KEYS.data['keys']), mode=state.get('mode'))}\n"
                f"💡 {n_on}/{len(devs)}  ·  📊 {len(state['channels'])}  ·  🎬 {len(state['scenes'])}  ·  ⏱ {n_auto}")
    live = live_line()
    if live:
        text += "\n" + live
    onboarding = [] if KEYS.data["keys"] else [[(T("add_token"), "m:ak")]]
    rows = onboarding + widget_rows() + [[(T("devices"), "m:dev"), (T("sensors"), "m:sen")], [(T("scenes"), "m:scn"), (T("auto"), "m:aut")],
                            [(T("board"), "m:brd"), (T("ai"), "m:ai")], [(T("settings"), "m:set")]]
    return text, rows


def scr_dev(args):
    page = int(args[0]) if args and args[0].isdigit() else 0
    with L:
        devs = sorted(state["aliases"].values(), key=lambda a: a["name"])
    per = 8
    pages = max(1, (len(devs) + per - 1) // per)
    page = min(page, pages - 1)
    lines, rows = [T("devices_title")], []
    for d in devs[page * per:(page + 1) * per]:
        tag = f" · {d['pin']}" if d.get("pin") else ""
        lines.append(f"{dev_icon(d)} {d['name']}{tag}")
        rows.append([(f"{dev_icon(d)} {d['name']}", f"m:dt:{d['id']}:{page}")])
    if not devs:
        lines.append(T("no_devices"))
    nav = ([("◀️", f"m:dev:{page - 1}")] if page > 0 else []) + ([("▶️", f"m:dev:{page + 1}")] if page < pages - 1 else [])
    if nav:
        rows.append(nav)
    if devs:
        rows.append([(T("all_off"), "m:alloff"), (T("refresh"), f"m:dev:{page}")])
    rows.append([(T("home"), "m:home")])
    with L:
        nj = len(state["jobs"])
    if nj:
        lines.append(T("jobs_line", n=nj))
    return "\n".join(lines), rows


def scr_sen(args):
    lines, rows = [T("sensors_title")], []
    with L:
        chans = list(state["channels"].items())
    for cid, ch in chans:
        last = DATA.data["last"].get(cid)
        val = f"{last[1]}{ch.get('unit', '')}" if last else "—"
        age = fmt_age(time.time() - last[0]) if last else "—"
        st = series_stats(cid, 24)
        lines.append(f"• {cid}: {val} ({age})  avg24: {st.get('avg', '—')}")
        rows.append([(f"📈 {cid}: {val}", f"m:sg:{cid}:24")])
    if not chans:
        lines.append(T("no_sensors"))
    rows.append([(T("refresh"), "m:sen"), (T("home"), "m:home")])
    return "\n".join(lines), rows


def scr_sg(args):
    cid = args[0] if args else ""
    hours = int(args[1]) if len(args) > 1 and args[1].isdigit() else 24
    rows = [[("1h", f"m:sg:{cid}:1"), ("6h", f"m:sg:{cid}:6"), ("24h", f"m:sg:{cid}:24"), ("7d", f"m:sg:{cid}:168")],
            [(T("back"), "m:sen"), (T("home"), "m:home")]]
    return make_graph(cid, hours), rows


def scr_scn(args):
    with L:
        scs = sorted(state["scenes"].values(), key=lambda s: s["name"])
    lines, rows = [T("scenes_title")], []
    for sc in scs[:12]:
        lines.append(f"🎬 {sc['name']} ({len(sc['actions'])})" + (f" - {sc['desc']}" if sc.get("desc") else ""))
        rows.append([(f"▶️ {sc['name']}", f"m:sr:{sc['id']}")])
    if not scs:
        lines.append(T("no_scenes"))
    rows.append([(T("home"), "m:home")])
    return "\n".join(lines), rows


def _auto_items():
    with L:
        return ([("r", rid, r) for rid, r in state["rules"].items()] +
                [("s", sid, s) for sid, s in state["schedules"].items()])


def scr_aut(args):
    lines, rows = [T("auto_title")], []
    for kind, iid, it in _auto_items()[:14]:
        on = it.get("enabled", True)
        what = f"{it['channel']} {it['op']} {it['value']}" if kind == "r" else (it.get("time") or f"/{it.get('minutes')}m")
        lines.append(f"{'✅' if on else '⏸'} {'⚙️' if kind == 'r' else '⏰'} {iid}: {what}")
        rows.append([(f"{'✅' if on else '⏸'} {iid}", f"m:ar:{kind}:{iid}")])
    if len(lines) == 1:
        lines.append(T("no_auto"))
    rows.append([(T("home"), "m:home")])
    return "\n".join(lines), rows


def scr_ar(args):
    kind, iid = (args + ["", ""])[:2]
    with L:
        it = (state["rules"] if kind == "r" else state["schedules"]).get(iid)
    if not it:
        return T("expired"), [[(T("back"), "m:aut")]]
    on = it.get("enabled", True)
    if kind == "r":
        desc = f"⚙️ {iid}\n{it['channel']} {it['op']} {it['value']}" + (f"\nwhen: {it['when']}" if it.get("when") else "")
    else:
        desc = f"⏰ {iid}\n{it['kind']} " + (it.get("time") or f"every {it.get('minutes')} min") + (f"\nwhen: {it['when']}" if it.get("when") else "")
    desc += "\n→ " + "; ".join(describe(a) for a in it["actions"])[:400]
    rows = [[(T("disable") if on else T("enable"), f"m:ae:{kind}:{iid}"), (T("delete"), f"m:ad:{kind}:{iid}")],
            [(T("back"), "m:aut"), (T("home"), "m:home")]]
    return desc, rows


def scr_ad(args):
    kind, iid = (args + ["", ""])[:2]
    return T("delete_q", name=iid), [[(T("yes"), f"m:ad:{kind}:{iid}:y"), (T("no"), f"m:ar:{kind}:{iid}")]]


def scr_brd(args):
    with L:
        st, hb, last = state["esp"].get("status"), state["esp"].get("hb"), state["esp"].get("last")
    lines = [T("board_title"), board_line()]
    if st:
        lines += [pretty_status(st["text"]), f"({fmt_age(now_ts() - st['ts'])})"]
    elif last:
        lines.append(f"📋 {last['text'][:300]}  ({fmt_age(now_ts() - last['ts'])})")
    if hb:
        lines.append(f"💓 {fmt_age(now_ts() - hb['ts'])}")
    rows = [[("🔄 Status", "m:bx:status"), ("🏓 Ping", "m:bx:ping")],
            [("📡 Scan", "m:bx:scan"), ("📋 Wi-Fi", "m:bx:wifi")],
            [("💡 LED on", "m:bx:ledon"), ("LED off", "m:bx:ledoff"), ("LED auto", "m:bx:ledauto")],
            [("🔁 Wi-Fi reconnect", "m:bx:recon"), (T("reboot"), "m:bx:reboot")],
            [(T("home"), "m:home")]]
    return "\n".join(lines), rows


def scr_wid(args):
    with L:
        ws, cs = list(state["custom"]), list(state["controls"].values())
    lines, rows = [T("widgets_title")], []
    for w in ws:
        lines.append(f"{'🎚' if w['kind'] == 'stepper' else '🔘'} {w['label']}")
        rows.append([(f"🗑 {w['label']}", f"m:wd:{w['id']}")])
    for c in cs:
        lines.append(f"🎚 {c['name']}: {fmt_num(c['value'])}{c.get('unit', '')}  [{fmt_num(c['min'])}..{fmt_num(c['max'])}, step {fmt_num(c['step'])}]")
        rows.append([(f"🗑 {c['name']} (variable)", f"m:cd:{c['id']}")])
    if not ws and not cs:
        lines.append(T("no_widgets"))
    rows.append([(T("home"), "m:home")])
    return "\n".join(lines), rows


def scr_ai(args):
    with L:
        ks = list(KEYS.data["keys"])
    rows = [[(T("add_token"), "m:ak"), (T("test_keys"), "m:kt")]]
    for k in ks[:8]:
        rows.append([(f"🗑 {k['id']} {k['provider']}", f"m:kd:{k['id']}")])
    rows.append([(T("mode_btn", mode=state.get("mode")), "m:st:mode"), (T("forget"), "m:st:forget")])
    rows.append([(T("home"), "m:home")])
    return T("ai_title") + "\n" + cmd_keys(), rows


def scr_set(args):
    yn = lambda v: "✅" if v else "❌"
    rows = [[(T("lang_btn"), "m:st:lang")],
            [(T("confirm_btn", v=yn(state.get("confirm_critical", True))), "m:st:confirm")],
            [(T("alert_btn", v=yn(state.get("board_alert", True))), "m:st:alert")],
            [(T("widgets"), "m:wid")], [(T("export"), "m:st:export")], [(T("home"), "m:home")]]
    return f"{T('settings_title')}\ntz: {state.get('tz')}", rows


SCREENS = {"home": scr_home, "dev": scr_dev, "sen": scr_sen, "sg": scr_sg, "scn": scr_scn, "aut": scr_aut,
           "ar": scr_ar, "ad": scr_ad, "brd": scr_brd, "ai": scr_ai, "set": scr_set, "wid": scr_wid}


def show(screen, args=(), mid=None):
    text, rows = SCREENS[screen](list(args))
    if mid:
        tg_edit(mid, text, rows)
    else:
        tg_send_rows(text, rows)


def _user_ctx(src):
    return {"user": True, "names": {}, "source": src}


def export_settings():
    with L:
        snap = {k: v for k, v in state.items() if k in ("aliases", "commands", "channels", "rules", "schedules", "scenes", "lang", "tz")}
        notes = NOTES.data
    body = json.dumps(snap, ensure_ascii=False, indent=2).encode("utf-8")
    tg_document("esp_agent_settings.json", body, "Settings export (no API keys, no history)")
    tg_document("brain.md", notes.encode("utf-8"), "Agent notes")


def handle_menu(data, cb):
    parts = data.split(":")[1:]
    mid = (cb.get("message") or {}).get("message_id")
    act, args = (parts[0] if parts else "home"), parts[1:]
    note = T("ok")
    if act == "brd":
        show("brd", args, mid)
        st = state["esp"].get("status")
        if not st or now_ts() - st["ts"] > 30:
            request_board_status(mid)                              # fresh data replaces this message when it arrives
    elif act in ("home", "dev", "sen", "sg", "scn", "aut", "ar", "ai", "set", "wid"):
        show(act, args, mid)
    elif act == "cb":                                              # a button the user created by chat
        w = widget_by(args[0]) if args else None
        if not w:
            note = T("expired")
        else:
            res = run_actions(w["actions"], {"user": False, "names": build_names(), "source": "button"})
            INBOX.flush()
            note = str(res[0])[:150] if res else T("ok")
        show("home", [], mid)
    elif act == "cs":                                              # ➖ / ➕ on a control (fan speed ...)
        w = widget_by(args[0]) if args else None
        c = control_by(w["control"]) if w else None
        if not c:
            note = T("expired")
        else:
            op = args[1] if len(args) > 1 else "r"
            if op in ("+", "-", "++", "--"):
                step = w.get("step") or c["step"]
                big = w.get("big_step") or step * 5
                delta = {"+": step, "-": -step, "++": big, "--": -big}[op]
                res = run_actions([normalize_action({"type": "set_control", "name": c["name"], "delta": delta})],
                                  {"user": True, "names": build_names(), "source": "panel"})
                INBOX.flush()
                note = str(res[0])[:150] if res else T("ok")
            else:
                note = f"{c['name']}: {fmt_num(c['value'])}{c.get('unit', '')}"
        show("home", [], mid)
    elif act == "ak":                                              # ➕ Add AI token: next message = the key
        start_key_prompt(mid)
    elif act == "ac":
        with L:
            state["awaiting_key"] = 0
            ST.mark()
        show("ai", [], mid)
    elif act == "ap":                                              # provider chosen for a key we could not identify
        if PENDING_KEY["key"] and time.time() - PENDING_KEY["ts"] < 600 and args:
            key, PENDING_KEY["key"] = PENDING_KEY["key"], None
            complete_key(args[0], key)
        else:
            note = T("key_expired")
    elif act == "kd":
        with L:
            KEYS.data["keys"][:] = [k for k in KEYS.data["keys"] if k["id"] != (args[0] if args else "")]
            KEYS.mark()
        KEYS.save(force=True)
        show("ai", [], mid)
    elif act == "kt":
        tg_send(test_all_keys())
        show("ai", [], mid)
    elif act == "wd":
        with L:
            state["custom"][:] = [w for w in state["custom"] if w["id"] != (args[0] if args else "")]
            ST.mark()
        show("wid", [], mid)
    elif act == "cd":
        c = control_by(args[0]) if args else None
        if c:
            run_actions([{"type": "remove_control", "name": c["name"]}], _user_ctx("panel"))
        show("wid", [], mid)
    elif act == "dt":                                           # toggle one device
        res = run_actions([{"type": "device", "name": args[0], "state": "toggle", "for_minutes": 0}], _user_ctx("panel"))
        INBOX.flush()
        if res and str(res[0]).startswith("error"):
            note = str(res[0])[:150]
        show("dev", [args[1] if len(args) > 1 else "0"], mid)
    elif act == "alloff":
        with L:
            names = [d["name"] for d in state["aliases"].values()]
        run_actions([{"type": "device", "name": n, "state": "off", "for_minutes": 0} for n in names], _user_ctx("panel"))
        INBOX.flush()
        show("dev", ["0"], mid)
    elif act == "sr":
        sc = scene_by(args[0]) if args else None
        if sc:
            res = run_actions([{"type": "scene", "name": sc["name"]}], {"user": False, "names": build_names(), "source": "panel"})
            INBOX.flush()
            note = str(res[0])[:150] if res else T("ok")
        else:
            note = T("expired")
    elif act == "ae":
        with L:
            it = (state["rules"] if args[0] == "r" else state["schedules"]).get(args[1])
            if it:
                it["enabled"] = not it.get("enabled", True)
                ST.mark()
        show("ar", args, mid)
    elif act == "ad":
        if len(args) >= 3 and args[2] == "y":
            with L:
                (state["rules"] if args[0] == "r" else state["schedules"]).pop(args[1], None)
                ST.mark()
            show("aut", [], mid)
        else:
            show("ad", args, mid)
    elif act == "bx":
        what = args[0] if args else ""
        if what == "reboot" and (len(args) < 2 or args[1] != "y"):
            tg_edit(mid, T("reboot_q"), [[(T("yes"), "m:bx:reboot:y"), (T("no"), "m:brd")]])
            note = "?"
        else:
            cmds = {"status": ("STATUS", ""), "ping": ("PING", "api.github.com"), "scan": ("SCAN", ""),
                    "wifi": ("WIFI", "LIST"), "recon": ("WIFI", "RECONNECT"), "ledon": ("LED", "ON"),
                    "ledoff": ("LED", "OFF"), "ledauto": ("LED", "AUTO"), "reboot": ("REBOOT", "")}
            if what == "status":
                request_board_status(mid)
                note = T("sent")
            elif what in cmds:
                c, a2 = cmds[what]
                run_actions([{"type": "esp", "cmd": c, "args": a2}], {"user": True, "confirmed": True, "names": {}, "source": "panel"})
                INBOX.flush()
                note = T("sent")
                if what == "reboot":
                    show("brd", [], mid)
    elif act == "st":
        what = args[0] if args else ""
        with L:
            if what == "lang":
                state["lang"] = "en" if state.get("lang", "fa") == "fa" else "fa"
            elif what == "confirm":
                state["confirm_critical"] = not state.get("confirm_critical", True)
            elif what == "alert":
                state["board_alert"] = not state.get("board_alert", True)
            elif what == "mode":
                state["mode"] = "raw" if state.get("mode") == "agent" else "agent"
            elif what == "forget":
                state["history"] = []
            ST.mark()
        if what == "export":
            export_settings()
        elif what == "lang":
            set_commands()
            show("set", [], mid)
        elif what in ("confirm", "alert"):
            show("set", [], mid)
        else:
            show("ai", [], mid)
    return note


# ----------------------------------------------------------------- user text pipeline
def send_raw(text):
    INBOX.add("M", text.encode("utf-8"))
    INBOX.flush()


ON_WORDS = ["روشن کن", "روشن", "وصل کن", "وصل", "turn on", "switch on", "on"]
OFF_WORDS = ["خاموش کن", "خاموش", "قطع کن", "قطع", "turn off", "switch off", "off"]
FILLER = {"را", "رو", "the", "please", "لطفا"}


def parse_device_phrase(text):
    """'room lamp on' / 'لامپ اتاق رو خاموش کن' -> (device, 'on'|'off'), no AI needed."""
    n = " ".join(w for w in norm(text).split() if w not in FILLER)
    for words, st in ((ON_WORDS, "on"), (OFF_WORDS, "off")):
        for w in sorted(words, key=len, reverse=True):
            name = None
            if n.startswith(w + " "):
                name = n[len(w) + 1:]
            elif n.endswith(" " + w):
                name = n[:-(len(w) + 1)]
            if name:
                al = alias_by(name)
                if al:
                    return al, st
    return None


def device_phrase(text):
    p = parse_device_phrase(text)
    if not p:
        return False
    al, st = p
    res = run_actions([{"type": "device", "name": al["name"], "state": st, "for_minutes": 0}], _user_ctx("phrase"))
    INBOX.flush()
    if res and isinstance(res[0], str) and not res[0].startswith("waiting"):
        tg_send(("✓ " if not res[0].startswith("error") else "") + res[0])
    return True


NUM_RE = re.compile(r"-?\d+(?:\.\d+)?")
UP_WORDS = {"up", "increase", "more", "higher", "بیشتر", "زیاد", "افزایش", "بالا"}
DOWN_WORDS = {"down", "decrease", "less", "lower", "کمتر", "کم", "کاهش", "پایین"}
MAX_WORDS = {"max", "maximum", "حداکثر", "ماکسیمم"}
MIN_WORDS = {"min", "minimum", "حداقل", "صفر"}
CTL_FILLER = {"set", "to", "at", "the", "please", "روی", "به", "کن", "را", "رو", "بذار", "بزار", "تنظیم", "لطفا", "سرعت", "میزان", "مقدار"}


def parse_control_phrase(text):
    """'fan 120' / 'سرعت فن رو 100 کن' / 'fan up' / 'فن کمتر' / 'fan max' -> a set_control action, no AI needed."""
    raw = text.translate(_FA)
    n = norm(text)
    with L:
        ctls = sorted(state["controls"].values(), key=lambda c: -len(c["name"]))
    cand = sorted(((norm(nm), c) for c in ctls for nm in [c["name"]] + c.get("aliases", [])), key=lambda x: -len(x[0]))
    for nm, c in cand:
        if not nm or f" {nm} " not in f" {n} ":
            continue
        words = {w for w in f" {n} ".replace(f" {nm} ", " ", 1).split() if not w.isdigit()} - CTL_FILLER
        nums = NUM_RE.findall(raw)
        step = float(nums[0]) if nums else c["step"]
        if not words and nums:
            return {"type": "set_control", "name": c["name"], "value": float(nums[0])}
        if words and words <= UP_WORDS:
            return {"type": "set_control", "name": c["name"], "delta": abs(step)}
        if words and words <= DOWN_WORDS:
            return {"type": "set_control", "name": c["name"], "delta": -abs(step)}
        if words and words <= MAX_WORDS and not nums:
            return {"type": "set_control", "name": c["name"], "to": "max"}
        if words and words <= MIN_WORDS and not nums:
            return {"type": "set_control", "name": c["name"], "to": "min"}
    return None


def control_phrase(text):
    p = parse_control_phrase(text)
    if not p:
        return False
    res = run_actions([normalize_action(p)], _user_ctx("phrase"))
    INBOX.flush()
    if res and isinstance(res[0], str) and not res[0].startswith("waiting"):
        tg_send(("✓ " if not res[0].startswith("error") else "") + res[0])
    return True


STATUS_PHRASES = ("وضعیت وای فای", "وضعیت برد", "وضعیت اینترنت", "وضعیت شبکه", "وای فای وصله", "wifi status", "board status",
                  "internet status", "network status", "is online", "are you online")
REBOOT_PHRASES = {"ریستارت", "ریستارت کن", "ریست کن", "ریبوت", "ریبوت کن", "برد رو ریستارت کن", "برد رو ریست کن",
                  "ریستارت برد", "reboot", "restart", "reboot board", "restart board", "راه اندازی مجدد"}


def board_phrase(text):
    n = norm(text)
    if n in REBOOT_PHRASES:
        tg_send_rows(T("reboot_q"), [[(T("yes"), "m:bx:reboot:y"), (T("no"), "m:brd")]])
        return True
    if any(p in n for p in STATUS_PHRASES):
        request_board_status(None)
        INBOX.add("C", b"PING api.github.com")
        INBOX.flush()
        tg_send(T("sent"))
        return True
    return False


def fast_command(text):
    key = norm(text)
    with L:
        cm = state["commands"].get(key)
    if not cm:
        return False
    if cm.get("critical") and state.get("confirm_critical", True):
        return False                              # goes through the AI/confirm path
    INBOX.add("M", "\n".join(cm["serial"]).encode("utf-8"))
    INBOX.flush()
    tg_send("✓ " + (cm.get("desc") or cm["phrase"]))
    return True


def start_key_prompt(mid=None):
    with L:
        state["awaiting_key"] = time.time()
        ST.mark()
    rows = [[(T("cancel"), "m:ac")]]
    if mid:
        tg_edit(mid, T("key_prompt"), rows)
    else:
        tg_send_rows(T("key_prompt"), rows)


def handle_key_message(text, msg_id=None):
    """The user sent an API key (after the ➕ button, or on its own). Delete it, detect the service, test it."""
    if msg_id:
        try:
            tg("deleteMessage", {"chat_id": TG_CHAT, "message_id": msg_id})
        except requests.RequestException:
            pass
    parts = text.split()
    first = PROVIDER_ALIASES.get(parts[0].lower(), parts[0].lower())
    provider = model = base = None
    if len(parts) >= 2 and first in PRESETS:
        provider, key = first, parts[1]
        model = parts[2] if len(parts) > 2 else None
        base = parts[3] if len(parts) > 3 else None
    else:
        key = parts[0]
    tg_typing()
    if not provider:
        provider = detect_provider(key)
    if not provider:                                           # ask once, keep the key only in memory
        PENDING_KEY.update(key=key, ts=time.time())
        names = list(PRESETS)[:-1]
        tg_send_rows(T("which_provider"), [[(n, f"m:ap:{n}") for n in names[i:i + 3]] for i in range(0, len(names), 3)])
        return
    complete_key(provider, key, model, base)


def complete_key(provider, key, model=None, base=None):
    try:
        _, _, msg = setup_key(provider, key, model, base)
    except ValueError as e:
        msg = f"❌ {e}"
    tg_send(msg)


def test_all_keys():
    lines = []
    with L:
        ks = list(KEYS.data["keys"])
    for k in ks:
        st, out = _test_key(k)
        if st == 200:
            mark_ok(k)
            lines.append(f"✅ {k['id']} {k['provider']}/{k['model']}")
        else:
            mark_fail(k, st, out)
            lines.append(f"❌ {k['id']} {k['provider']}: {st} {str(out)[:60]}")
    KEYS.save(force=True)
    return "\n".join(lines) or T("no_keys")


HELP = """📱 /panel - control panel (devices, sensors, scenes, board, AI, settings)
Quick: "turn on <device>" / "<device> خاموش" work without AI. Start a message with > to send it raw to serial.
/devices /sensors /scenes /auto /board /graph temp 24 /log /history /export /lang fa|en
/keys  /addkey (then just send the key)  /delkey ID  /mode agent|raw  /state  /status
/forget  /confirm on|off  /breaker reset  /keyboard off  /raw TEXT
Providers: anthropic openai gemini groq openrouter deepseek mistral custom
Anything else is understood by the AI agent (if a key works) or sent raw to the serial port.
Wi-Fi passwords you type are stored in the private data repo."""


def mask(key):
    return key[:4] + "…" + key[-3:] if len(key) > 10 else "…"


def cmd_keys():
    now = time.time()
    lines = []
    with L:
        for k in KEYS.data["keys"]:
            if k.get("until", 0) > now:
                st = f"⏳ cooldown {int(k['until'] - now)}s ({k.get('last_error', '')[:60]})"
            else:
                st = "✅ ready" + (f" (last ok {int(now - k['last_ok'])}s ago)" if k.get("last_ok") else "")
            lines.append(f"{k['id']} {k['provider']} {k['model']} {mask(k['key'])}  {st}")
    return "\n".join(lines) or T("no_keys")


def cmd_state():
    with L:
        out = [f"mode={state['mode']} tz={state['tz']} confirm_critical={state['confirm_critical']}"]
        out.append("Aliases: " + (", ".join(f"{a['name']}={a['code']}" for a in state["aliases"].values()) or "-"))
        out.append("Fast commands: " + (", ".join(c["phrase"] for c in state["commands"].values()) or "-"))
        out.append("Channels: " + (", ".join(f"{c}" for c in state["channels"]) or "-"))
        out.append("Rules: " + (", ".join(state["rules"]) or "-"))
        out.append("Schedules: " + (", ".join(f"{k}({s.get('time') or str(s.get('minutes')) + 'm'})" for k, s in state["schedules"].items()) or "-"))
    return "\n".join(out)


def handle_command(text, msg):
    parts = text.split()
    cmd = parts[0].split("@")[0].lower()
    args = parts[1:]
    if cmd == "/start":
        tg_send(HELP, keyboard=home_keyboard())
        show("home")
        return True
    if cmd == "/help":
        return tg_send(HELP)
    scr = {"/panel": "home", "/menu": "home", "/devices": "dev", "/sensors": "sen", "/scenes": "scn",
           "/auto": "aut", "/buttons": "wid", "/board": "brd", "/ai": "ai", "/settings": "set"}.get(cmd)
    if scr:
        show(scr)
        return True
    if cmd == "/graph":
        if not args:
            show("sen")
            return True
        hours = int(args[1]) if len(args) > 1 and args[1].isdigit() else 24
        return tg_send(make_graph(args[0], max(1, min(720, hours))))
    if cmd == "/log":
        n = int(args[0]) if args and args[0].isdigit() else 15
        rows = list(DEVLOG)[-max(1, min(40, n)):]
        return tg_send("\n".join(
            f"{datetime.fromtimestamp(r[0], tz()).strftime('%H:%M:%S')}  {r[1]}" + (f"   → {','.join(r[2])}" if len(r) > 2 and r[2] else "")
            for r in rows) or T("log_empty"))
    if cmd == "/history":
        with L:
            rows = state["audit"][-15:]
        return tg_send("\n".join(f"{datetime.fromtimestamp(r['ts'], tz()).strftime('%H:%M:%S')} [{r['src']}] {r['text']}" for r in rows) or T("hist_empty"))
    if cmd == "/export":
        export_settings()
        return True
    if cmd == "/lang" and args and args[0] in ("fa", "en"):
        with L:
            state["lang"] = args[0]
            ST.mark()
        set_commands()
        return tg_send(T("lang_set"), keyboard=home_keyboard())
    if cmd == "/breaker":
        breaker_reset()
        return tg_send("✓ breaker reset")
    if cmd == "/keyboard" and args and args[0] == "off":
        return tg_send("✓", remove_keyboard=True)
    if cmd == "/keys":
        return tg_send(cmd_keys())
    if cmd == "/addkey":
        try:                                       # delete the message that contains the secret
            tg("deleteMessage", {"chat_id": TG_CHAT, "message_id": msg["message_id"]})
        except requests.RequestException:
            pass
        if not args:
            start_key_prompt()
        else:
            handle_key_message(" ".join(args))
        return True
    if cmd == "/cancel":
        return tg_send(T("key_cancel"))
    if cmd == "/delkey" and args:
        with L:
            n0 = len(KEYS.data["keys"])
            KEYS.data["keys"] = [k for k in KEYS.data["keys"] if k["id"] != args[0]]
            KEYS.mark()
        KEYS.save(force=True)
        return tg_send("Key removed." if len(KEYS.data["keys"]) < n0 else "No such key id.")
    if cmd == "/mode" and args and args[0] in ("agent", "raw"):
        with L:
            state["mode"] = args[0]
            ST.mark()
        return tg_send(f"Mode: {args[0]}")
    if cmd == "/state":
        return tg_send(cmd_state())
    if cmd == "/forget":
        with L:
            state["history"] = []
            ST.mark()
        return tg_send("Conversation memory cleared (definitions and notes are kept).")
    if cmd == "/confirm" and args and args[0] in ("on", "off"):
        with L:
            state["confirm_critical"] = args[0] == "on"
            ST.mark()
        return tg_send(f"Confirmation for critical actions: {args[0]}")
    if cmd == "/raw":
        send_raw(text[len("/raw"):].strip() if len(text) > 5 else "")
        return tg_send("✓ sent raw")
    if cmd == "/status":
        INBOX.add("C", b"STATUS")
        INBOX.flush()
        with L:
            state["esp"]["waiting"] = time.time()
        return tg_send(f"Bridge running. Keys ready: {len(ordered_keys())}/{len(KEYS.data['keys'])}. Asking the board for its status…")
    return None                                    # unknown /command -> treat as normal text


def handle_text(text, msg):
    text = text.strip()
    if not text:
        return
    aw = state.get("awaiting_key", 0)
    if aw:
        with L:
            state["awaiting_key"] = 0
            ST.mark()
        if time.time() - aw < 600 and not text.startswith("/"):
            handle_key_message(text, msg.get("message_id"))
            return
        if text.lower().startswith("/cancel"):
            tg_send(T("key_cancel"))
            return
    if looks_like_key(text):                       # a key sent on its own is never shown to the AI or stored in history
        handle_key_message(text, msg.get("message_id"))
        return
    if text.startswith("/"):
        if handle_command(text, msg) is not None:
            return
    if text.startswith(">"):
        send_raw(text[1:].lstrip())
        return
    if state.get("mode") == "raw":
        send_raw(text)
        return
    scr = SCREEN_WORDS.get(norm(text))
    if scr:
        show(scr)
        return
    if board_phrase(text):
        return
    if device_phrase(text):
        return
    if control_phrase(text):
        return
    if fast_command(text):
        return
    if ai_turn(text):
        return
    if KEYS.data["keys"]:
        notify_once("ai_down", "⚠️ No AI key is working right now - your text was sent raw to the serial port. Check /keys")
    send_raw(text)


def handle_callback(cb):
    data = cb.get("data", "")
    try:
        if data.startswith("m:"):
            note = handle_menu(data, cb)
            tg("answerCallbackQuery", {"callback_query_id": cb["id"], "text": str(note)[:180]})
            return
        if data.startswith("b:"):
            with L:
                b = state["buttons"].get(data[2:])
            if not b:
                tg("answerCallbackQuery", {"callback_query_id": cb["id"], "text": "expired"})
                return
            acts = b["actions"]
            ctx = {"user": False, "names": build_names(), "source": "button"}
            confirm = [a for a in acts if a.get("type") == "_confirm"]
            if confirm:
                c = confirm[0]
                with L:
                    p = state["pending"].pop(c["id"], None)
                    ST.mark()
                if not p:
                    tg("answerCallbackQuery", {"callback_query_id": cb["id"], "text": "expired"})
                    return
                if c["yes"]:
                    res = run_actions(p["actions"], {"user": True, "confirmed": True, "names": ctx["names"], "source": "confirm"})
                    tg_send("✓ " + "; ".join(str(r) for r in res)[:600])
                else:
                    tg_send("Cancelled.")
            else:
                res = run_actions(acts, ctx) if acts else ["(no action)"]
                if any(isinstance(r, str) and r.startswith("error") for r in res):
                    tg_send("; ".join(str(r) for r in res)[:600])
            INBOX.flush()
        tg("answerCallbackQuery", {"callback_query_id": cb["id"], "text": "✓"})
    except Exception as e:
        log(f"callback error: {e.__class__.__name__}")


def handle_update(u):
    cb = u.get("callback_query")
    if cb:
        chat = str((cb.get("message") or {}).get("chat", {}).get("id", ""))
        if TG_CHAT and chat == TG_CHAT:
            handle_callback(cb)
        return
    msg = u.get("message")
    if not msg:
        return
    chat_id = msg["chat"]["id"]
    if not TG_CHAT:
        try:
            tg("sendMessage", {"chat_id": chat_id,
                               "text": f"Your chat id is {chat_id}. Put it in the TG_CHAT_ID secret to authorize this chat."})
        except requests.RequestException:
            pass
        return
    if str(chat_id) != TG_CHAT:
        return
    text = msg.get("text")
    if text is None:
        kinds = ("photo", "document", "sticker", "voice", "video", "audio", "location", "contact")
        k = next((x for x in kinds if x in msg), "unsupported")
        INBOX.add("S", f"[WARN] Telegram sent a non-text message ({k}) - ignored".encode())
        INBOX.flush()
        return
    handle_text(text, msg)


def telegram_loop():
    offset = None
    backoff = 1
    notified = set()
    try:
        tg("deleteWebhook", {"drop_pending_updates": False})
    except requests.RequestException:
        pass
    while not stop.is_set():
        try:
            params = {"timeout": 25, "allowed_updates": ["message", "callback_query"]}
            if offset is not None:
                params["offset"] = offset
            r = requests.post(f"{TG}/getUpdates", json=params, timeout=40)
            data = r.json()
            if not data.get("ok"):
                code = data.get("error_code")
                log(f"Telegram getUpdates error {code}")
                if code in (401, 409) and code not in notified:
                    notified.add(code)
                    INBOX.add("S", ("[ERR ] Telegram bot token invalid (401) - check TG_TOKEN" if code == 401 else
                                    "[ERR ] Telegram conflict (409) - another consumer is polling this bot").encode())
                    INBOX.flush()
                stop.wait(30 if code in (401, 409) else min(backoff, 30))
                backoff = min(backoff * 2, 30)
                continue
            backoff = 1
            for u in data["result"]:
                offset = u["update_id"] + 1
                try:
                    handle_update(u)
                except Exception as e:                 # never let one message kill the loop
                    log(f"update error: {e.__class__.__name__}: {str(e)[:80]}")
        except (requests.RequestException, ValueError) as e:
            log(f"Telegram poll error: {e.__class__.__name__}")
            stop.wait(backoff)
            backoff = min(backoff * 2, 30)
    if offset is not None:
        try:
            requests.post(f"{TG}/getUpdates", json={"offset": offset, "timeout": 0, "limit": 1}, timeout=10)
        except requests.RequestException:
            pass


# ----------------------------------------------------------------- device data (ESP -> bridge)
def deliver(text: str):
    """Plain Telegram delivery for non-intercepted device text. Returns ('ok'|'retry'|'fail', info)."""
    try:
        for i in range(0, len(text), 4096):
            code, data = tg("sendMessage", {"chat_id": TG_CHAT, "text": text[i:i + 4096],
                                            "disable_web_page_preview": True})
            if code == 200 and data.get("ok"):
                continue
            if code == 429:
                wait = (data.get("parameters") or {}).get("retry_after", 5)
                stop.wait(min(int(wait), 30))
                return "retry", "rate limited"
            if code >= 500:
                return "retry", f"telegram server error {code}"
            return "fail", f"{code} {data.get('description', '')}".strip()
        return "ok", ""
    except requests.RequestException as e:
        return "retry", e.__class__.__name__


def reapply_pins():
    """After a board reboot, GPIOs are back to default: restore the last known device states."""
    with L:
        devs = [d for d in state["aliases"].values() if d.get("pin") and d.get("state")][:8]
    for al in devs:
        high = (al["state"] == "on") != bool(al.get("invert"))
        INBOX.add("C", f"GPIO {al['pin']} {'ON' if high else 'OFF'}".encode())
    return len(devs)


def handle_esp_report(line):
    now = now_ts()
    if line.startswith("HB "):                       # heartbeat: keeps the offline watchdog quiet, never shown
        with L:
            state["esp"]["hb"] = {"ts": now, "text": line[:200]}
            ST.mark()
        return
    is_status = line.startswith("STATUS ")
    with L:
        state["esp"]["last"] = {"ts": now, "text": line[:400]}
        if is_status:
            state["esp"]["status"] = {"ts": now, "text": line[:400]}
        mid = state["esp"].pop("panel_mid", None) if is_status else None
        waiting = state["esp"].pop("waiting", None)
        ST.mark()
    if is_status:
        if mid and waiting and time.time() - waiting < 120:
            show("brd", [], mid)                      # update the open panel instead of sending a new message
        else:
            tg_send("🔧 " + pretty_status(line))
        return
    if line.startswith("PING "):
        tg_send(pretty_ping(line))
        return
    note = ""
    if line.startswith("BOOT "):
        n = reapply_pins()
        if n:
            note = f"  (restored {n} pin state{'s' if n != 1 else ''})"
    tg_send("🔧 " + line + note)


def classify_device_text(text):
    """Split a device message into lines to forward to Telegram; sensor values and board reports are consumed.
    One line may carry several values (T25.3 H60): every matching channel records its own value."""
    forward = []
    for ln in text.split("\n"):
        ln = ln.rstrip("\r")
        if not ln.strip():
            continue
        if ln.startswith(ESP_TAG):
            handle_esp_report(ln[len(ESP_TAG):].strip())
            continue
        ms = match_channels(ln)
        DEVLOG.append((time.time(), ln, [m[0] for m in ms]))
        if not ms:
            forward.append(ln)
            continue
        show_it = False
        for cid, ch, val in ms:
            try:
                record(cid, val)
            except Exception as e:
                log(f"record error: {e.__class__.__name__}")
            if ch.get("forward"):
                show_it = True
        if show_it:
            forward.append(ln)
    return "\n".join(forward)


done_ids = set()
attempts = {}
pre = {}                                           # comment id -> text still to forward (after classification)


def delete_comment(cid):
    try:
        r = S.delete(f"{API}/repos/{DATA_REPO}/issues/comments/{cid}", timeout=20)
        if r.status_code in (204, 404):
            done_ids.discard(cid)
            return True
    except requests.RequestException:
        pass
    done_ids.add(cid)
    return False


def process_comment(c) -> str:
    cid = c["id"]
    if cid in done_ids:
        delete_comment(cid)
        return "done"
    login = ((c.get("user") or {}).get("login") or "").lower()
    if login not in ALLOWED:
        log(f"Ignoring comment {cid} from unauthorized user")
        delete_comment(cid)
        return "done"
    touch_board()
    text = c.get("body") or ""
    if not text.strip():
        INBOX.add("S", f"[WARN] Empty message ignored (ref {cid})".encode())
        delete_comment(cid)
        return "done"
    if cid not in pre:
        pre[cid] = classify_device_text(text)       # consumed lines are processed exactly once
    to_forward = pre[cid]
    if not to_forward.strip():
        request_ack()                               # silent ack (throttled): nothing to show on the serial port
        pre.pop(cid, None)
        done_ids.add(cid)
        delete_comment(cid)
        return "done"
    res, info = deliver(to_forward)
    if res == "retry":
        attempts[cid] = attempts.get(cid, 0) + 1
        if attempts[cid] < 8:
            return "retry"
        res, info = "fail", f"gave up after 8 attempts ({info})"
    if res == "ok":
        INBOX.add("S", f"[ OK ] Delivered to Telegram (ref {cid})".encode())
        log(f"Delivered ref {cid} ({len(to_forward)} chars)")
    else:
        INBOX.add("S", f"[ERR ] Telegram rejected message (ref {cid}): {info}".encode())
        log(f"Failed ref {cid}: {info}")
    pre.pop(cid, None)
    done_ids.add(cid)
    delete_comment(cid)
    attempts.pop(cid, None)
    return "done"


def outbox_loop():
    url = f"{API}/repos/{DATA_REPO}/issues/{OUTBOX_ISSUE}/comments"
    etag = None
    warned = set()
    while not stop.is_set():
        wait = GH_POLL
        try:
            h = {"If-None-Match": etag} if etag else {}
            r = S.get(url, params={"per_page": 100}, headers=h, timeout=20)
            if r.status_code == 200:
                comments = r.json()
                changed = retry = False
                for c in comments:
                    try:
                        res = process_comment(c)
                    except Exception as e:
                        log(f"comment error: {e.__class__.__name__}: {str(e)[:80]}")
                        res = "done"
                        delete_comment(c["id"])
                    if res == "retry":
                        retry = True
                        wait = 3
                        break
                    changed = True
                if changed:
                    INBOX.flush()
                etag = r.headers.get("ETag") if (not comments and not retry) else None
            elif r.status_code == 304:
                pass
            else:
                if r.status_code not in warned:
                    warned.add(r.status_code)
                    log(f"Outbox poll HTTP {r.status_code} (check OUTBOX_ISSUE / DATA_PAT)")
                wait = 30 if r.status_code in (401, 403, 404) else 10
        except (requests.RequestException, ValueError) as e:
            log(f"Outbox poll error: {e.__class__.__name__}")
            wait = 5
        stop.wait(wait)


# ----------------------------------------------------------------- housekeeping
def flusher_loop():
    while not stop.is_set():
        for f in (ST, DATA, KEYS, NOTES):
            try:
                f.save()
            except Exception as e:
                log(f"flush {f.path}: {e.__class__.__name__}")
        try:
            maybe_ack()
            if INBOX.dirty:
                INBOX.flush()
        except Exception:
            pass
        stop.wait(5)


def migrate_state():
    with L:
        for al in state["aliases"].values():
            if not al.get("id"):
                state["dev_seq"] = state.get("dev_seq", 0) + 1
                al["id"] = f"d{state['dev_seq']}"
            al.setdefault("pin", "")
            al.setdefault("invert", False)
            al.setdefault("state", None)
            al.setdefault("state_ts", 0)
        for sc in state["scenes"].values():
            if not sc.get("id"):
                state["scn_seq"] = state.get("scn_seq", 0) + 1
                sc["id"] = f"s{state['scn_seq']}"
        ST.mark()


def seed_env_keys():
    """AI_KEYS secret: one key per line  provider|key  or  provider|model|key[|base_url]"""
    for line in os.environ.get("AI_KEYS", "").splitlines():
        p = [x.strip() for x in line.split("|")]
        try:
            if len(p) == 2:
                add_key(p[0], p[1])
            elif len(p) >= 3:
                add_key(p[0], p[2], p[1] or None, p[3] if len(p) > 3 else None)
        except ValueError as e:
            log(f"AI_KEYS line ignored: {e}")


def self_dispatch():
    repo = os.environ.get("GITHUB_REPOSITORY")
    tok = os.environ.get("GITHUB_TOKEN")
    ref = os.environ.get("GITHUB_REF_NAME", "main")
    wf = os.environ.get("WORKFLOW_FILE", "bridge.yml")
    if not (repo and tok):
        return
    try:
        r = requests.post(
            f"{API}/repos/{repo}/actions/workflows/{wf}/dispatches",
            headers={"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28"},
            json={"ref": ref}, timeout=20)
        log(f"Queued next bridge run: HTTP {r.status_code}")
    except requests.RequestException as e:
        log(f"Could not queue next run: {e.__class__.__name__} (cron will recover)")


def main():
    global reached_deadline

    def _sig(*_):
        stop.set()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    INBOX.load()
    for f in (ST, DATA, KEYS, NOTES):
        f.load()
    seed_env_keys()
    migrate_state()
    BOARD["seen"] = time.time()
    set_commands()
    log(f"Agent bridge started | repo={DATA_REPO} | chat {'set' if TG_CHAT else 'NOT set (discovery mode)'} | "
        f"AI keys {len(KEYS.data['keys'])} | runtime {RUN_SECONDS // 60} min")
    threads = [threading.Thread(target=f, daemon=True)
               for f in (telegram_loop, outbox_loop, scheduler_loop, flusher_loop)]
    for t in threads:
        t.start()

    deadline = time.time() + RUN_SECONDS
    while not stop.is_set():
        if time.time() >= deadline:
            reached_deadline = True
            break
        stop.wait(5)

    stop.set()
    for t in threads:
        t.join(timeout=45)
    for f in (ST, DATA, KEYS, NOTES):
        f.save(force=True)
    INBOX.flush()
    if reached_deadline and SELF_RESTART:
        self_dispatch()
    log("Bridge stopped")


if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        print(f"Missing environment variable: {e}", file=sys.stderr)
        sys.exit(2)
