#!/usr/bin/env python3
"""
Transparent Telegram <-> GitHub bridge for the ESP-01 firmware.

ESP -> Telegram : ESP posts a comment on OUTBOX_ISSUE of the private data repo.
                  This bridge reads it, sends the text unchanged to Telegram,
                  deletes the comment and writes a delivery acknowledgement.
Telegram -> ESP : Long-polls Telegram, appends each message to inbox.txt
                  (record format: "<kind> <seq> <len>\\n<payload>\\n", kind M=message, S=system).

Message contents are NEVER logged (this repo is public, Actions logs are public).
"""
import base64
import os
import signal
import sys
import threading
import time

import requests

# ----------------------------------------------------------------- config
TG_TOKEN = os.environ["TG_TOKEN"].strip()
TG_CHAT = os.environ.get("TG_CHAT_ID", "").strip()
DATA_PAT = os.environ["DATA_PAT"].strip()
DATA_REPO = os.environ["DATA_REPO"].strip()                    # e.g. user/esp-data
OUTBOX_ISSUE = int(os.environ.get("OUTBOX_ISSUE", "1"))
INBOX_PATH = os.environ.get("INBOX_PATH", "inbox.txt")
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", str(5 * 3600 + 50 * 60)))
GH_POLL = float(os.environ.get("GH_POLL_SECONDS", "2"))
SELF_RESTART = os.environ.get("SELF_RESTART", "1") == "1"
OWNER = DATA_REPO.split("/")[0].lower()
ALLOWED = {u.strip().lower() for u in os.environ.get("ALLOWED_GH_USERS", OWNER).split(",") if u.strip()}

MAX_RECORDS = 30
MAX_INBOX_BYTES = 6000
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
reached_deadline = False


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


# ----------------------------------------------------------------- inbox file
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
        self._load()

    def _load(self):
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
            raise RuntimeError(f"Cannot read data repo inbox: HTTP {r.status_code} "
                               f"(check DATA_REPO / DATA_PAT permissions)")

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


# ----------------------------------------------------------------- telegram
def tg(method, payload, timeout=20):
    r = requests.post(f"{TG}/{method}", json=payload, timeout=timeout)
    try:
        data = r.json()
    except ValueError:
        data = {"ok": False, "description": f"HTTP {r.status_code}"}
    return r.status_code, data


def deliver(text: str):
    """Returns ('ok'|'retry'|'fail', info)."""
    try:
        for i in range(0, len(text), 4096):
            code, data = tg("sendMessage", {
                "chat_id": TG_CHAT,
                "text": text[i:i + 4096],
                "disable_web_page_preview": True,
            })
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


def handle_update(u, inbox: Inbox):
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
        inbox.add("S", f"[WARN] Telegram sent a non-text message ({k}) - ignored".encode())
        return
    inbox.add("M", text.encode("utf-8"))


def telegram_loop(inbox: Inbox):
    offset = None
    backoff = 1
    notified = set()
    try:
        tg("deleteWebhook", {"drop_pending_updates": False})
    except requests.RequestException:
        pass
    while not stop.is_set():
        try:
            params = {"timeout": 25, "allowed_updates": ["message"]}
            if offset is not None:
                params["offset"] = offset
            r = requests.post(f"{TG}/getUpdates", json=params, timeout=40)
            data = r.json()
            if not data.get("ok"):
                code = data.get("error_code")
                log(f"Telegram getUpdates error {code}")
                if code in (401, 409) and code not in notified:
                    notified.add(code)
                    inbox.add("S", ("[ERR ] Telegram bot token invalid (401) - check TG_TOKEN" if code == 401 else
                                    "[ERR ] Telegram conflict (409) - another consumer is polling this bot").encode())
                    inbox.flush()
                stop.wait(30 if code in (401, 409) else min(backoff, 30))
                backoff = min(backoff * 2, 30)
                continue
            backoff = 1
            got = False
            for u in data["result"]:
                offset = u["update_id"] + 1
                handle_update(u, inbox)
                got = True
            if got:
                inbox.flush()
        except (requests.RequestException, ValueError) as e:
            log(f"Telegram poll error: {e.__class__.__name__}")
            stop.wait(backoff)
            backoff = min(backoff * 2, 30)
    # confirm the last offset so the next run does not re-deliver
    if offset is not None:
        try:
            requests.post(f"{TG}/getUpdates", json={"offset": offset, "timeout": 0, "limit": 1}, timeout=10)
        except requests.RequestException:
            pass


# ----------------------------------------------------------------- outbox
done_ids = set()
attempts = {}


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


def process_comment(c, inbox: Inbox) -> str:
    cid = c["id"]
    if cid in done_ids:
        delete_comment(cid)
        return "done"
    login = ((c.get("user") or {}).get("login") or "").lower()
    if login not in ALLOWED:
        log(f"Ignoring comment {cid} from unauthorized user")
        delete_comment(cid)
        return "done"
    text = c.get("body") or ""
    if not text.strip():
        inbox.add("S", f"[WARN] Empty message ignored (ref {cid})".encode())
        delete_comment(cid)
        return "done"
    res, info = deliver(text)
    if res == "retry":
        attempts[cid] = attempts.get(cid, 0) + 1
        if attempts[cid] < 8:
            return "retry"
        res, info = "fail", f"gave up after 8 attempts ({info})"
    if res == "ok":
        inbox.add("S", f"[ OK ] Delivered to Telegram (ref {cid})".encode())
        log(f"Delivered ref {cid} ({len(text)} chars)")
    else:
        inbox.add("S", f"[ERR ] Telegram rejected message (ref {cid}): {info}".encode())
        log(f"Failed ref {cid}: {info}")
    done_ids.add(cid)
    delete_comment(cid)
    attempts.pop(cid, None)
    return "done"


def outbox_loop(inbox: Inbox):
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
                changed = False
                retry = False
                for c in comments:
                    if process_comment(c, inbox) == "retry":
                        retry = True
                        wait = 3
                        break
                    changed = True
                if changed:
                    inbox.flush()
                etag = r.headers.get("ETag") if not comments else None
                if retry:
                    etag = None
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


# ----------------------------------------------------------------- main
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

    inbox = Inbox()
    log(f"Bridge started | repo={DATA_REPO} | chat {'set' if TG_CHAT else 'NOT set (discovery mode)'} | "
        f"runtime {RUN_SECONDS // 60} min")
    threads = [
        threading.Thread(target=telegram_loop, args=(inbox,), daemon=True),
        threading.Thread(target=outbox_loop, args=(inbox,), daemon=True),
    ]
    for t in threads:
        t.start()

    deadline = time.time() + RUN_SECONDS
    while not stop.is_set():
        if time.time() >= deadline:
            reached_deadline = True
            break
        stop.wait(5)
        if inbox.dirty:
            inbox.flush()

    stop.set()
    for t in threads:
        t.join(timeout=45)
    inbox.flush()
    if reached_deadline and SELF_RESTART:
        self_dispatch()
    log("Bridge stopped")


if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        print(f"Missing environment variable: {e}", file=sys.stderr)
        sys.exit(2)
