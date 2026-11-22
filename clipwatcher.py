#!/usr/bin/env python3
"""
clip-watcher: record a live Twitch or YouTube stream, watch chat for hype spikes,
and cut vertical (9:16) clips with subtitles automatically.

    ./watch.sh <twitch_channel | youtube_live_url | @handle> [options]

Pipeline
  1. streamlink records the stream to rec/*.ts (MPEG-TS stays readable while growing)
  2. chat is read anonymously (Twitch: IRC justinfan, YouTube: chat-downloader; no token/API key)
  3. messages are counted in 10 s buckets; spike = >= factor x median of the last
     3 minutes AND >= --min messages AND >= --min-users distinct chatters
  4. ffmpeg cuts [moment - pre, moment + post] into clips/, 9:16
  5. Whisper writes an .srt next to every clip (optionally burned in)
"""
from __future__ import annotations

import argparse
import array
import base64
import collections
import dataclasses
import importlib.machinery
import importlib.util
import json
import math
import os
import queue
import random
import re
import shlex
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import types
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

VERSION = "1.8"
HERE = Path(__file__).resolve().parent

# Ensure local .venv/Scripts, bin, and python scripts directories are always in PATH
for _cand in (HERE / ".venv" / "Scripts", HERE / ".venv" / "bin",
             Path(sys.executable).parent, Path(sys.prefix) / "Scripts"):
    if _cand.exists() and str(_cand) not in os.environ.get("PATH", ""):
        os.environ["PATH"] = str(_cand) + os.pathsep + os.environ.get("PATH", "")


def find_tool(name: str) -> str | None:
    """Locate an executable checking system PATH, .venv/Scripts, and local folders."""
    exe = shutil.which(name) or shutil.which(f"{name}.exe")
    if exe:
        return exe
    for _p in (HERE / ".venv" / "Scripts" / f"{name}.exe",
              HERE / ".venv" / "bin" / name,
              Path(sys.executable).parent / f"{name}.exe",
              Path(sys.prefix) / "Scripts" / f"{name}.exe",
              HERE / f"{name}.exe"):
        if _p.exists():
            return str(_p)
    return None

# --------------------------------------------------------------------------- #
# logging
# --------------------------------------------------------------------------- #
_log_lock = threading.Lock()


def log(msg: str) -> None:
    with _log_lock:
        if sys.stdout.isatty():
            sys.stdout.write("\r\033[K")
        print(f"{time.strftime('%H:%M:%S')}  {msg}", flush=True)


def status_line(msg: str) -> None:
    if os.environ.get("CLIPWATCHER_UI"):                  # the web page shows this line in its header
        with _log_lock:
            print("@@STATUS " + msg, flush=True)
        return
    if not sys.stdout.isatty():
        return
    cols = shutil.get_terminal_size((100, 20)).columns - 1
    with _log_lock:
        sys.stdout.write("\r\033[K" + msg[:cols])
        sys.stdout.flush()


# --------------------------------------------------------------------------- #
# spike detection (pure logic, no I/O, fully testable)
# --------------------------------------------------------------------------- #
@dataclasses.dataclass
class Bucket:
    count: int = 0   # anti-spam count: each chatter contributes at most `user_cap`
    raw: int = 0     # every message
    users: collections.Counter = dataclasses.field(default_factory=collections.Counter)
    times: list = dataclasses.field(default_factory=list)
    terms: collections.Counter = dataclasses.field(default_factory=collections.Counter)
    msgs: list = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Spike:
    peak_ts: float
    bucket_start: float
    count: int
    raw: int
    median: float
    ratio: float
    users: int
    terms: list
    samples: list
    pos: float | None = None   # media events: position in the recording (seconds from its start)
    source: str = "chat"       # chat | audio | motion
    detail: str = ""
    hype: float = 0.0          # share of chat messages that are hype (W, KEKW, LUL, fire/skull emoji...)
    clipit: int = 0            # messages literally asking "clip it / clip that"


CHAT_HYPE = {"w", "lol", "lmao", "lmfao", "kekw", "omegalul", "lul", "pog", "poggers", "pogchamp", "pogu",
             "clip", "clipit", "dead", "wtf", "omg", "insane", "crazy", "nooo", "noooo", "wow", "cooked",
             "goat", "sheesh", "bruh", "rip", "ez", "lets", "gg"}
CHAT_HYPE_MARKS = ("💀", "😂", "🤣", "🔥", "😭", "😱", "🗣", ":joy:", ":skull:", ":fire:", ":sob:", ":scream:",
                   ":rofl:", ":face-")
CLIP_ASK = re.compile(r"\bclip\s*(it|that|this)\b|\bclipit\b", re.I)


def chat_hype(msgs) -> tuple[float, int]:
    """(share of messages that look like hype, number of 'clip it' requests)."""
    if not msgs:
        return 0.0, 0
    hype = ask = 0
    for m in msgs:
        low = m.lower()
        toks = set(re.findall(r"[a-z0-9']+", low))
        if toks & CHAT_HYPE or any(x in low for x in CHAT_HYPE_MARKS):
            hype += 1
        if CLIP_ASK.search(m):
            ask += 1
    return hype / len(msgs), ask


class SpikeDetector:
    """
    Counts chat messages in fixed buckets. When a bucket closes it is compared
    with the median of the previous `window` seconds (empty buckets count as 0).

    Extras over the plain spec:
      * min_users   - one person spamming can't trigger a clip
      * user_cap    - each chatter counts at most N messages per bucket
      * sigma       - must also beat median + sigma*sqrt(median), so random wobble in a
                      medium-busy chat doesn't fire clips
      * warmup      - no clips until enough history exists
      * peak finding - a rising spike waits for its top bucket before firing
      * cooldown    - one big moment fires one clip, not five
    """

    def __init__(self, bucket=10.0, window=180.0, factor=2.5, min_msgs=8,
                 min_users=3, warmup=60.0, cooldown=30.0, user_cap=5, sigma=5.0):
        self.user_cap, self.sigma = user_cap, sigma
        self.bucket, self.window, self.factor = float(bucket), float(window), float(factor)
        self.min_msgs, self.min_users = min_msgs, min_users
        self.warmup, self.cooldown = float(warmup), float(cooldown)
        self.buckets: dict[int, Bucket] = {}
        self.start_idx: int | None = None
        self.next_idx: int | None = None
        self.pending: Spike | None = None
        self.cooldown_until = 0.0
        self.last = None  # (count, median, threshold, history_len)

    def _idx(self, ts: float) -> int:
        return int(ts // self.bucket)

    def _init(self, ts: float) -> None:
        if self.start_idx is None:
            # the bucket we start in is partial -> history starts at the next one
            self.start_idx = self.next_idx = self._idx(ts) + 1

    def add(self, ts: float, user: str, text: str) -> None:
        self._init(ts)
        b = self.buckets.setdefault(self._idx(ts), Bucket())
        b.raw += 1
        b.users[user] += 1
        if b.users[user] <= self.user_cap:
            b.count += 1
        b.times.append(ts)
        for tok in set(text.split()):
            if len(tok) > 1 and not tok.startswith("@"):
                b.terms[tok] += 1
        if len(b.msgs) < 60:
            b.msgs.append(text)

    def tick(self, now: float) -> list[Spike]:
        """Close every finished bucket; return spikes ready to be clipped."""
        self._init(now)
        out: list[Spike] = []
        cur = self._idx(now)
        while self.next_idx < cur:
            out.extend(self._close(self.next_idx))
            self.next_idx += 1
        return out

    def _make(self, b: Bucket, start: float, median: float) -> Spike:
        peak = statistics.median(b.times) if b.times else start + self.bucket / 2
        terms = [(t, c) for t, c in b.terms.most_common(5) if c >= 2]
        step = max(1, len(b.msgs) // 5)
        samples = [m[:120] for m in b.msgs[::step][:5]]
        hype, ask = chat_hype(b.msgs)
        return Spike(peak_ts=peak, bucket_start=start, count=b.count, raw=b.raw, median=median,
                     ratio=b.count / max(median, 1.0), users=len(b.users),
                     terms=terms, samples=samples, hype=hype, clipit=ask)

    def _finalize(self) -> Spike:
        sp, self.pending = self.pending, None
        self.cooldown_until = sp.peak_ts + self.cooldown
        return sp

    def _close(self, i: int) -> list[Spike]:
        b = self.buckets.get(i) or Bucket()
        n_hist = int(self.window // self.bucket)
        hist = [self.buckets[j].count if j in self.buckets else 0
                for j in range(max(self.start_idx, i - n_hist), i)]
        median = float(statistics.median(hist)) if hist else 0.0
        # factor x median, floor of min_msgs, and clearly above ordinary Poisson noise
        thr = max(float(self.min_msgs), self.factor * median,
                  median + self.sigma * max(median, 1.0) ** 0.5)
        self.last = (b.count, median, thr, len(hist))
        ready = len(hist) * self.bucket >= self.warmup
        is_spike = ready and b.count >= thr and len(b.users) >= self.min_users
        start = i * self.bucket

        for k in [k for k in self.buckets if k < i - n_hist - 2]:
            del self.buckets[k]

        out: list[Spike] = []
        if self.pending is not None:
            if is_spike and b.count > self.pending.count:
                self.pending = self._make(b, start, median)  # still climbing
            else:
                out.append(self._finalize())  # peak passed (this bucket is the tail)
            return out
        if is_spike and start >= self.cooldown_until:
            self.pending = self._make(b, start, median)
        return out


# --------------------------------------------------------------------------- #
# Twitch chat (anonymous IRC)
# --------------------------------------------------------------------------- #
class ChatReader(threading.Thread):
    HOST = os.environ.get("CLIPWATCHER_IRC_HOST", "irc.chat.twitch.tv")  # env overrides are for testing
    PORT = int(os.environ.get("CLIPWATCHER_IRC_PORT", "6667"))

    def __init__(self, channel: str, on_message, stop: threading.Event):
        super().__init__(daemon=True)
        self.channel, self.on_message, self.stop_evt = channel, on_message, stop
        self.state = "connecting"
        self.total = 0

    def run(self) -> None:
        backoff = 2
        while not self.stop_evt.is_set():
            try:
                self._session()
                backoff = 2
            except Exception as e:  # noqa: BLE001
                log(f"chat: connection problem ({e}); retrying in {backoff}s")
            self.state = "connecting"
            if self.stop_evt.wait(backoff):
                break
            backoff = min(backoff * 2, 60)

    def _session(self) -> None:
        nick = f"justinfan{random.randint(10000, 99999)}"
        sock = socket.create_connection((self.HOST, self.PORT), timeout=15)
        sock.settimeout(45)
        try:
            sock.sendall(f"PASS SCHMOOPIIE\r\nNICK {nick}\r\nJOIN #{self.channel}\r\n".encode())
            buf = b""
            awaiting_pong = False
            while not self.stop_evt.is_set():
                try:
                    data = sock.recv(8192)
                except socket.timeout:
                    if awaiting_pong:
                        raise ConnectionError("no response to PING")
                    sock.sendall(b"PING :tmi.twitch.tv\r\n")
                    awaiting_pong = True
                    continue
                if not data:
                    raise ConnectionError("server closed the connection")
                awaiting_pong = False
                buf += data
                *lines, buf = buf.split(b"\r\n")
                for raw in lines:
                    if self._handle(sock, raw.decode("utf-8", "replace")):
                        return  # server asked us to reconnect
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def _handle(self, sock, line: str) -> bool:
        if line.startswith("PING"):
            sock.sendall(("PONG" + line[4:] + "\r\n").encode())  # keeps Twitch from dropping us
            return False
        parts = line.split(" ", 3)
        if len(parts) >= 2:
            cmd = parts[1]
            if cmd == "PRIVMSG" and len(parts) == 4:
                user = parts[0][1:].split("!", 1)[0]
                text = parts[3][1:] if parts[3].startswith(":") else parts[3]
                self.total += 1
                self.on_message(time.time(), user, text)
            elif cmd == "366":
                self.state = "ok"
                log(f"chat: joined #{self.channel}")
            elif cmd == "RECONNECT":
                return True
        return False


# --------------------------------------------------------------------------- #
# YouTube live chat (chat-downloader, no API key)
# --------------------------------------------------------------------------- #
class InnertubeChat:
    """
    Direct YouTube live-chat client (fallback for when `chat-downloader` fails with
    "Unable to parse initial video data"). It skips the /live_chat HTML page that the
    library scrapes and talks to the same JSON endpoint the player uses.
    """
    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/124.0 Safari/537.36")

    def __init__(self, url: str, fetch=None):
        self.url, self.key, self.version, self.cont = url, None, "2.20240101.00.00", None
        self.context = None
        self.title = None
        self._fetch = fetch or self._http

    def _http(self, url, body=None):
        headers = {"User-Agent": self.UA, "Accept-Language": "en-US,en;q=0.9",
                   "Cookie": "SOCS=CAI; CONSENT=YES+1"}
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers.update({"Content-Type": "application/json",
                            "Origin": "https://www.youtube.com",
                            "Referer": self.url,
                            "X-YouTube-Client-Name": "1",
                            "X-YouTube-Client-Version": self.version})
        req = urllib.request.Request(url, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            try:
                detail = e.read().decode("utf-8", "replace")
                m = re.search(r'"message":\s*"([^"]+)"', detail)
                detail = m.group(1) if m else detail[:150]
            except Exception:  # noqa: BLE001
                detail = ""
            raise RuntimeError(f"HTTP {e.code} from YouTube chat API: {detail}".strip()) from None

    @staticmethod
    def _json_after(html: str, marker: str):
        i = html.find(marker)
        if i < 0:
            return None
        j = html.find("{", i)
        try:
            return json.JSONDecoder().raw_decode(html[j:])[0]
        except ValueError:
            return None

    def open(self) -> None:
        """Load the watch page and find the live-chat continuation token."""
        html = self._fetch(self.url)
        m = re.search(r'"INNERTUBE_API_KEY":"([^"]+)"', html)
        self.key = m.group(1) if m else None
        m = re.search(r'"INNERTUBE_CONTEXT_CLIENT_VERSION":"([^"]+)"', html)
        self.version = m.group(1) if m else self.version
        ctx = self._json_after(html, '"INNERTUBE_CONTEXT"')
        if isinstance(ctx, dict) and "client" in ctx:
            self.context = ctx
            self.version = ctx["client"].get("clientVersion") or self.version
        data = self._json_after(html, "ytInitialData")
        if not data:
            raise RuntimeError("YouTube watch page had no ytInitialData (consent page or blocked?)")
        try:
            title = data["contents"]["twoColumnWatchNextResults"]["results"]["results"]["contents"][0][
                "videoPrimaryInfoRenderer"]["title"]["runs"]
            self.title = "".join(r.get("text", "") for r in title)
        except (KeyError, IndexError, TypeError):
            pass
        try:
            chat = data["contents"]["twoColumnWatchNextResults"]["conversationBar"]["liveChatRenderer"]
        except (KeyError, TypeError):
            raise RuntimeError("this video has no live chat (not live yet, ended, or chat disabled)")
        if chat.get("isReplay"):
            raise RuntimeError("this is a replay, not a live stream")
        items = (chat.get("header", {}).get("liveChatHeaderRenderer", {}).get("viewSelector", {})
                 .get("sortFilterSubMenuRenderer", {}).get("subMenuItems", []))
        conts = [i["continuation"]["reloadContinuationData"]["continuation"] for i in items
                 if "continuation" in i]
        try:
            main = chat["continuations"][0]["reloadContinuationData"]["continuation"]
        except (KeyError, IndexError, TypeError):
            main = None
        # the page's main chat token is the complete one; the Top/Live selector tokens are only a fallback
        self.conts = [t for t in dict.fromkeys(([main] if main else []) + conts[::-1]) if t]
        if not self.conts:
            raise RuntimeError("could not find a live chat token on the watch page")
        self.cont = self.conts[0]
        self.ok_once = False
        self.combo = 0
        self.pending = []
        self._exchange_token()

    # (send the page's full client context?, send the API key?) - tried in order until one works
    COMBOS = [(True, True), (False, True), (True, False), (False, False)]

    def _exchange_token(self) -> None:
        """Browsers first open /live_chat?continuation=TOKEN and continue from the token that page
        hands back. Do the same, with a tolerant parser. If it fails, keep the watch-page token."""
        for tok in self.conts:
            html = ""
            try:
                url = ("https://www.youtube.com/live_chat?is_popout=1&continuation="
                       + urllib.parse.quote(tok, safe="%"))
                html = self._fetch(url)
                data = self._json_after(html, "ytInitialData")
                lc = data["continuationContents"]["liveChatContinuation"]
                out, nxt, _ = self._parse(lc)
                if not nxt:
                    continue
                m = re.search(r'"INNERTUBE_API_KEY":"([^"]+)"', html)
                if m:
                    self.key = m.group(1)
                ctx = self._json_after(html, '"INNERTUBE_CONTEXT"')
                if isinstance(ctx, dict) and "client" in ctx:
                    self.context = ctx
                    self.version = ctx["client"].get("clientVersion") or self.version
                self.cont, self.pending = nxt, out
                return
            except Exception:  # noqa: BLE001
                if html:
                    try:
                        Path("chat_debug.html").write_text(html[:300000], encoding="utf-8")
                    except Exception:  # noqa: BLE001
                        pass

    def rotate(self) -> None:
        """A request was rejected before anything worked: try the next request variant / token."""
        if self.ok_once:
            return
        self.combo += 1
        if self.combo >= len(self.COMBOS):
            self.combo = 0
            if len(self.conts) > 1:
                self.conts = self.conts[1:] + self.conts[:1]
                self.cont = self.conts[0]

    @staticmethod
    def _parse(lc):
        out = []
        for act in lc.get("actions", []):
            item = act.get("addChatItemAction", {}).get("item", {})
            r = item.get("liveChatTextMessageRenderer") or item.get("liveChatPaidMessageRenderer")
            if not r:
                continue
            text = "".join(x.get("text") or (x.get("emoji", {}).get("shortcuts") or [""])[0]
                           for x in r.get("message", {}).get("runs", []))
            user = r.get("authorExternalChannelId") or r.get("authorName", {}).get("simpleText") or "?"
            out.append((r.get("id"), user, text))
        nxt, wait = None, 5.0
        for c in lc.get("continuations", []):
            for k in ("timedContinuationData", "invalidationContinuationData"):
                if k in c:
                    nxt = c[k].get("continuation")
                    wait = max(1.0, min(c[k].get("timeoutMs", 5000) / 1000.0, 10.0))
        return out, nxt, wait

    def poll(self):
        """One API call. Returns (messages, wait_seconds, ended). messages = [(id, user, text)]."""
        full, use_key = self.COMBOS[self.combo]
        url = "https://www.youtube.com/youtubei/v1/live_chat/get_live_chat"
        if self.key and use_key:
            url += "?key=" + self.key
        context = (self.context if (full and self.context)
                   else {"client": {"clientName": "WEB", "clientVersion": self.version, "hl": "en"}})
        body = {"context": context, "continuation": self.cont}
        resp = json.loads(self._fetch(url, body))
        lc = resp.get("continuationContents", {}).get("liveChatContinuation")
        if not lc:
            return [], 5.0, True
        out, nxt, wait = self._parse(lc)
        out, self.pending = self.pending + out, []
        if not nxt:
            return out, wait, True
        self.cont = nxt
        self.ok_once = True
        return out, wait, False

class YouTubeChatReader(threading.Thread):
    """
    Reads YouTube live chat through the `chat-downloader` package (scrapes the same
    endpoint the web player uses; no login, no API key). Works for a video URL or for a
    channel (@handle, /channel/ID): in channel mode it waits until the channel goes live.
    """
    TYPES = ["text_message", "paid_message"]  # Super Chats count as messages too

    def __init__(self, a, on_message, stop: threading.Event):
        super().__init__(daemon=True)
        self.a, self.on_message, self.stop_evt = a, on_message, stop
        self.state = "connecting"
        self.total = 0
        self._ids: collections.deque = collections.deque(maxlen=5000)
        self._idset: set = set()
        self.use_fallback = False

    def _duplicate(self, mid) -> bool:
        """After a reconnect YouTube replays recent messages; never count them twice."""
        if not mid:
            return False
        if mid in self._idset:
            return True
        if len(self._ids) == self._ids.maxlen:
            self._idset.discard(self._ids[0])
        self._ids.append(mid)
        self._idset.add(mid)
        return False

    def _run_fallback(self) -> None:
        """Poll YouTube's chat endpoint directly. Returns when the chat ends or errors."""
        yt = InnertubeChat(self.a.stream_url)
        yt.open()
        if yt.title:
            self.a.stream_title = yt.title
        self.state = "ok"
        log(f"chat: connected to YouTube live chat (direct mode){' - ' + yt.title if yt.title else ''}")
        fails = 0
        while not self.stop_evt.is_set():
            try:
                msgs, wait, ended = yt.poll()
                fails = 0
            except Exception as e:  # noqa: BLE001
                fails += 1
                if fails == 1:
                    log(f"chat: poll failed: {e}")
                yt.rotate()
                if fails >= 10:
                    raise
                self.stop_evt.wait(1)
                continue
            for mid, user, text in msgs:
                if self._duplicate(mid):
                    continue
                self.total += 1
                self.on_message(time.time(), str(user), text)
            if ended:
                self.state = "stream ended"
                log("chat: stream ended or chat closed; waiting to reconnect")
                return
            self.stop_evt.wait(wait)

    def run(self) -> None:
        try:
            from chat_downloader import ChatDownloader
        except ImportError:
            self.state = "chat-downloader missing"
            log("chat: install it with `pip install chat-downloader` (or run ./setup.sh)")
            return
        backoff, last_err = 15, None
        while not self.stop_evt.is_set():
            try:
                if self.use_fallback:
                    self._run_fallback()
                    if self.stop_evt.wait(backoff):
                        return
                    continue
                chat = ChatDownloader().get_chat(self.a.chat_url, message_types=self.TYPES)
                status = getattr(chat, "status", None)
                if status in ("upcoming", "past"):
                    self.state = f"stream {status}"
                    if last_err != status:
                        log(f"chat: the stream is '{status}', waiting for it to go live")
                        last_err = status
                    self.stop_evt.wait(30)
                    continue
                if status == "live":
                    self.state = "ok"
                    if getattr(chat, "title", None):
                        self.a.stream_title = chat.title
                    log(f"chat: connected to YouTube live chat"
                        f"{' - ' + chat.title if getattr(chat, 'title', None) else ''}")
                else:
                    self.state = "waiting for live"  # channel mode: library waits for the stream
                last_err, backoff = None, 15
                for m in chat:
                    if self.stop_evt.is_set():
                        return
                    if self.state != "ok":
                        self.state = "ok"
                        log("chat: connected to YouTube live chat")
                    if self._duplicate(m.get("message_id")):
                        continue
                    author = m.get("author") or {}
                    user = author.get("id") or author.get("name") or "?"
                    self.total += 1
                    self.on_message(time.time(), str(user), str(m.get("message") or ""))
                self.state = "stream ended"
                log("chat: stream ended or chat closed; waiting to reconnect")
            except Exception as e:  # noqa: BLE001
                msg = str(e).strip().splitlines()[0][:160] if str(e).strip() else type(e).__name__
                if "parse initial video data" in str(e) and not self.use_fallback:
                    self.use_fallback = True
                    log("chat: chat-downloader can't parse YouTube's chat page; switching to direct mode")
                    backoff = 2
                    continue
                if msg != last_err:
                    log(f"chat: {msg} (retrying)")
                    last_err = msg
                self.state = "connecting"
            if self.stop_evt.wait(backoff):
                return
            backoff = min(backoff * 2, 60)


# --------------------------------------------------------------------------- #
# recording
# --------------------------------------------------------------------------- #
class Recording:
    def __init__(self, path: Path):
        self.path = path
        self.t0 = time.time()
        self.finished = False
        self._n = 0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            self._n += 1

    def release(self) -> None:
        with self._lock:
            self._n -= 1

    @property
    def inflight(self) -> int:
        with self._lock:
            return self._n


class Recorder(threading.Thread):
    def __init__(self, a, stop: threading.Event):
        super().__init__(daemon=True)
        self.a, self.stop_evt = a, stop
        self.current: Recording | None = None
        self.history: list[Recording] = []
        self.lock = threading.Lock()
        self.proc: subprocess.Popen | None = None

    def acquire_current(self) -> Recording | None:
        with self.lock:
            r = self.current
            if r:
                r.acquire()
            return r

    def _terminate(self) -> None:
        p = self.proc
        if p and p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()

    def stop(self) -> None:
        self._terminate()

    def prune(self) -> None:
        cap = self.a.max_rec_gb * 1e9
        if not cap:
            return
        files = sorted(self.a.rec_dir.glob(f"{self.a.channel}_*.ts"), key=lambda p: p.stat().st_mtime)
        total = sum(f.stat().st_size for f in files)
        with self.lock:
            busy = {r.path for r in self.history if r.inflight or not r.finished}
        for f in files:
            if total <= cap:
                break
            if f in busy:
                continue
            size = f.stat().st_size
            f.unlink(missing_ok=True)
            total -= size
            log(f"rec: pruned old recording {f.name}")

    def run(self) -> None:
        a = self.a
        url = a.stream_url
        logf = open(a.rec_dir / "streamlink.log", "ab")
        offline_logged = False
        while not self.stop_evt.is_set():
            self.prune()
            path = a.rec_dir / f"{a.channel}_{datetime.now():%Y%m%d-%H%M%S}.ts"
            cmd = [find_tool("streamlink") or "streamlink", "--loglevel", "info"]
            if a.low_latency and a.platform == "twitch":
                cmd.append("--twitch-low-latency")
            cmd += a.streamlink_args + ["-o", str(path), url, a.quality]
            self.proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, stderr=logf)
            rec, rotate = None, False
            while self.proc.poll() is None and not self.stop_evt.is_set():
                if rec is None and path.exists() and path.stat().st_size > 0:
                    rec = Recording(path)
                    with self.lock:
                        self.current = rec
                        self.history.append(rec)
                    offline_logged = False
                    log(f"rec: recording -> {path.name}")
                if rec and a.max_rec_gb and path.stat().st_size > a.max_rec_gb * 0.5e9:
                    rotate = True
                    break
                self.stop_evt.wait(0.5)
            self._terminate()
            if rec:
                rec.finished = True
                with self.lock:
                    self.current = None
            elif path.exists() and path.stat().st_size == 0:
                path.unlink(missing_ok=True)
            if self.stop_evt.is_set():
                break
            if rotate:
                log("rec: rotating recording file (disk cap)")
                continue
            if rec is None:
                if not offline_logged:
                    log("rec: stream is offline (or streamlink failed, see rec/streamlink.log) "
                        "- waiting for it to go live")
                    offline_logged = True
                self.stop_evt.wait(20)
            else:
                log("rec: stream ended; waiting for it to come back")
                self.stop_evt.wait(5)
        logf.close()


# --------------------------------------------------------------------------- #
# audio + motion detection (reads the recording itself, so it works even if chat is quiet)
# --------------------------------------------------------------------------- #
class RiseDetector:
    """
    Spots a sudden rise of a 1-D signal (loudness, or amount of motion) above its own
    rolling median. Waits ~`settle` seconds so the clip centres on the peak, then fires once
    and stays quiet for `cooldown` seconds.
      mode "add": trigger when value >= median + margin   (decibels)
      mode "mul": trigger when value >= max(median * margin, minimum)
    """

    def __init__(self, step, window=120.0, warmup=45.0, cooldown=30.0, settle=2.0,
                 margin=8.0, mode="add", floor=None, minimum=0.0):
        self.step, self.warmup, self.cooldown, self.settle = step, warmup, cooldown, settle
        self.margin, self.mode, self.floor, self.minimum = margin, mode, floor, minimum
        self.hist: collections.deque = collections.deque(maxlen=max(10, int(window / step)))
        self.pending = None
        self.last_fire = -1e9
        self.base = self.thr = self.value = None

    def feed(self, t, v):
        """Returns (peak_t, peak_value, baseline) when a moment is detected, else None."""
        base = statistics.median(self.hist) if self.hist else v
        thr = base + self.margin if self.mode == "add" else max(base * self.margin, self.minimum)
        if self.floor is not None:
            thr = max(thr, self.floor)
        self.base, self.thr, self.value = base, thr, v
        ready = len(self.hist) * self.step >= self.warmup
        event = None
        if self.pending is None:
            if ready and v >= thr and t - self.last_fire >= self.cooldown:
                self.pending = [t, v, t, base]
        else:
            if v > self.pending[1]:
                self.pending[0], self.pending[1] = t, v
            if t - self.pending[2] >= self.settle:
                event = (self.pending[0], self.pending[1], self.pending[3])
                self.last_fire, self.pending = t, None
        self.hist.append(v)
        return event


class MediaWatcher(threading.Thread):
    """
    Tails the live recording and feeds it to two ffmpeg processes:
      audio  -> loudness (dBFS) once per second     -> "audio" event on a sudden loud moment
      motion -> mean frame difference twice a second -> "motion" event on sudden action
    Events are put on `events` as (Recording, kind, position_seconds, score, detail).
    """
    A_RATE = 8000
    FRAME_W, FRAME_H, FPS = 64, 36, 2

    def __init__(self, a, recorder, stop: threading.Event, events: queue.Queue):
        super().__init__(daemon=True)
        self.a, self.recorder, self.stop_evt, self.events = a, recorder, stop, events
        self.status = {}
        self.progress = {}
        self.best_jump = (0.0, 0.0, 0.0)     # biggest sound jump seen: (dB over normal, time, level) - for diagnostics
        self.best_motion = (0.0, 0.0, 0.0, 0.0) # biggest visual motion: (factor, time, level, baseline)
        self.direct, self.offset, self.span = False, 0.0, None     # video files: let ffmpeg read the file itself
        self.only = None                                           # video files: {'audio'} / {'audio','motion'}

    def run(self) -> None:
        seen = None
        while not self.stop_evt.is_set():
            rec = self.recorder.current
            if rec is not None and rec is not seen:
                seen = rec
                try:
                    self.analyze(rec)
                except Exception as e:  # noqa: BLE001
                    log(f"media: analysis error: {e}")
            self.stop_evt.wait(1)

    def analyze(self, rec: "Recording") -> None:
        """Analyse one recording until it ends (blocking)."""
        threads = []
        use = self.only if self.only is not None else {k for k in ("audio", "motion") if getattr(self.a, k, True)}
        if "audio" in use:
            threads.append(threading.Thread(target=self._audio, args=(rec,), daemon=True))
        if "motion" in use:
            threads.append(threading.Thread(target=self._motion, args=(rec,), daemon=True))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    # -- plumbing --------------------------------------------------------- #
    def _spawn(self, rec, args):
        logf = open(self.a.rec_dir / "media.log", "ab")
        if self.direct:
            pre = ["-ss", f"{self.offset:.3f}"] if self.offset else []
            if self.span:
                pre += ["-t", f"{self.span:.3f}"]
            proc = subprocess.Popen(["ffmpeg", "-hide_banner", "-loglevel", "error", *pre, "-i", str(rec.path)] + args,
                                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=logf)
            return proc, logf
        proc = subprocess.Popen(["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", "pipe:0"] + args,
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=logf)

        def feed():
            try:
                with open(rec.path, "rb") as f:
                    while not self.stop_evt.is_set():
                        chunk = f.read(1 << 16)
                        if chunk:
                            proc.stdin.write(chunk)
                        elif rec.finished:
                            break
                        else:
                            self.stop_evt.wait(0.2)
            except (BrokenPipeError, OSError, ValueError):
                pass
            finally:
                try:
                    proc.stdin.close()
                except Exception:  # noqa: BLE001
                    pass

        threading.Thread(target=feed, daemon=True).start()
        return proc, logf

    @staticmethod
    def _reap(proc, logf) -> None:
        try:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)
        except Exception:  # noqa: BLE001
            proc.kill()
        logf.close()

    def _emit(self, rec, kind, pos, score, detail) -> None:
        self.events.put((rec, kind, pos + self.offset, score, detail))

    # -- audio ------------------------------------------------------------- #
    def _audio(self, rec) -> None:
        a = self.a
        det = RiseDetector(step=1.0, window=120, warmup=a.warmup, cooldown=a.cooldown, settle=2.0,
                           margin=a.audio_db, mode="add", floor=-45.0)
        proc, logf = self._spawn(rec, ["-vn", "-ac", "1", "-ar", str(self.A_RATE), "-f", "s16le", "pipe:1"])
        n = 0
        try:
            while not self.stop_evt.is_set():
                buf = proc.stdout.read(self.A_RATE * 2)       # one second of 16-bit mono
                if len(buf) < self.A_RATE * 2:
                    break
                arr = array.array("h")
                arr.frombytes(buf)
                if sys.byteorder == "big":
                    arr.byteswap()
                rms = math.sqrt(sum(x * x for x in arr) / len(arr))
                db = 20 * math.log10(max(rms, 1.0) / 32768.0)
                n += 1
                self.progress["audio"] = n
                ev = det.feed(float(n), db)
                self.status["audio"] = (db, det.base)
                if n > 5 and db > -45.0 and det.base is not None and db - det.base > self.best_jump[0]:
                    self.best_jump = (db - det.base, float(n) + self.offset, db)
                if ev:
                    pt, pv, pb = ev
                    self._emit(rec, "audio", pt, pv - pb, f"loud +{pv - pb:.0f} dB ({pv:.0f} dBFS)")
        finally:
            self._reap(proc, logf)

    # -- motion -------------------------------------------------------------- #
    def _motion(self, rec) -> None:
        a = self.a
        m_margin = min(a.motion_factor, 1.8) if (self.span and self.span <= 3600) else a.motion_factor
        det = RiseDetector(step=1.0 / self.FPS, window=180, warmup=a.warmup, cooldown=a.cooldown, settle=2.0,
                           margin=m_margin, mode="mul", minimum=a.motion_min)
        size = self.FRAME_W * self.FRAME_H
        vf = f"fps={self.FPS},scale={self.FRAME_W}:{self.FRAME_H}:flags=area,format=gray"
        proc, logf = self._spawn(rec, ["-an", "-vf", vf, "-f", "rawvideo", "pipe:1"])
        prev, recent, n = None, collections.deque(maxlen=4), 0
        try:
            while not self.stop_evt.is_set():
                frame = proc.stdout.read(size)
                if len(frame) < size:
                    break
                n += 1
                self.progress["motion"] = n / self.FPS
                if prev is not None:
                    recent.append(sum(abs(x - y) for x, y in zip(frame, prev)) / size)
                    avg = sum(recent) / len(recent)           # ~2 s average
                    ev = det.feed(n / self.FPS, avg)
                    self.status["motion"] = (avg, det.base)
                    base_val = max(det.base or 0.5, 0.5)
                    factor = avg / base_val
                    if factor > self.best_motion[0]:
                        self.best_motion = (factor, (n / self.FPS) + self.offset, avg, base_val)
                    if ev:
                        pt, pv, pb = ev
                        self._emit(rec, "motion", pt, pv / max(pb, 0.5),
                                   f"action {pv:.0f} vs normal {pb:.0f}")
                prev = frame
        finally:
            self._reap(proc, logf)


# --------------------------------------------------------------------------- #
# media helpers
# --------------------------------------------------------------------------- #
def probe_duration(path: Path) -> float | None:
    """Duration of a (possibly still growing) .ts file, from its last timestamps."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=nw=1:nk=1", str(path)],
            capture_output=True, text=True, timeout=30)
        s = r.stdout.strip()
        return float(s) if s and s != "N/A" else None
    except (subprocess.SubprocessError, ValueError, OSError):
        return None


def motion_check(path: Path, pos: float, factor: float, minimum: float):
    """Is there sudden on-screen action around `pos`? Compares the 10 s after `pos - 3` with the 35 s before it.
    Returns (ratio, level) or None. Used for video files, where scanning every frame of a long video is too slow."""
    w, h, fps = MediaWatcher.FRAME_W, MediaWatcher.FRAME_H, MediaWatcher.FPS
    lo = max(0.0, pos - 40.0)
    try:
        r = subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-ss", f"{lo:.2f}", "-t", f"{(pos - lo) + 12:.2f}",
                            "-i", str(path), "-an", "-vf", f"fps={fps},scale={w}:{h}:flags=area,format=gray",
                            "-f", "rawvideo", "pipe:1"], capture_output=True, timeout=180)
    except (OSError, subprocess.SubprocessError):
        return None
    size = w * h
    n = len(r.stdout) // size
    if n < 12:
        return None
    frames = [r.stdout[i * size:(i + 1) * size] for i in range(n)]
    diffs = [sum(abs(x - y) for x, y in zip(frames[i], frames[i - 1])) / size for i in range(1, n)]
    cut = int((pos - 3.0 - lo) * fps)
    before, after = diffs[:max(1, cut)], diffs[max(1, cut):max(2, cut + 10 * fps)]
    if not after or len(before) < 6:
        return None
    base = statistics.median(before)
    peak = sum(sorted(after)[-4:]) / len(sorted(after)[-4:])
    ratio = peak / max(base, 0.5)
    return (ratio, peak) if (ratio >= factor and peak >= minimum) else None


CAM_PRESETS = {
    "mid-left": (0.0, 0.40, 0.22, 0.26),
    "top-left": (0.01, 0.02, 0.22, 0.26),
    "bottom-left": (0.01, 0.70, 0.22, 0.26),
    "top-right": (0.77, 0.02, 0.22, 0.26),
    "bottom-right": (0.77, 0.70, 0.22, 0.26),
    "mid-right": (0.77, 0.40, 0.22, 0.26),
}


def parse_cam(text: str) -> tuple:
    """'x,y,w,h' as fractions of the stream frame, or a preset name ('mid-left', 'bottom-right', etc.)."""
    raw = text.strip().lower()
    if raw in CAM_PRESETS:
        return CAM_PRESETS[raw]
    try:
        x, y, w, h = [float(v) for v in text.replace(" ", "").split(",")]
    except ValueError:
        raise ValueError("--cam needs 4 numbers (x,y,w,h) or a preset: mid-left, top-left, bottom-left, top-right, bottom-right")
    if not (0 <= x < 1 and 0 <= y < 1 and 0 < w <= 1 and 0 < h <= 1 and x + w <= 1.001 and y + h <= 1.001):
        raise ValueError("--cam values must be between 0 and 1 and stay inside the frame")
    return x, y, w, h


CAM_DEFAULT = "0,0.43,0.2,0.205"


def cam_from_faces(dets, W: int, H: int, share: float = 0.36):
    """
    dets: [(x, y, w, h), ...] face boxes in pixels from several frames of the same stream.
    The face cam is the spot where a face shows up again and again. Returns (cam, hits) where cam is
    (x, y, w, h) as fractions of the frame, shaped to fill the top part of the split layout, or None.
    """
    if not dets:
        return None
    groups: list[list] = []
    for d in dets:
        cx, cy = d[0] + d[2] / 2, d[1] + d[3] / 2
        for g in groups:
            gx, gy = g[0][0] + g[0][2] / 2, g[0][1] + g[0][3] / 2
            if abs(cx - gx) < 0.06 * W and abs(cy - gy) < 0.08 * H:
                g.append(d)
                break
        else:
            groups.append([d])
    def weight(g) -> float:
        # a face seen often counts most; face cams sit near the left/right edge and are small, game
        # characters are often bigger and nearer the middle
        cx = statistics.median(d[0] + d[2] / 2 for d in g) / W
        cy = statistics.median(d[1] + d[3] / 2 for d in g) / H
        edge = 3.0 if (min(cx, 1 - cx) < 0.3 or min(cy, 1 - cy) < 0.2) else 1.0
        size = 1.0 if statistics.median(d[2] for d in g) <= 0.13 * W else 0.6
        return len(g) * edge * size

    best = max(groups, key=weight)
    if len(best) < 2:
        return None
    fx = statistics.median(d[0] + d[2] / 2 for d in best)
    fy = statistics.median(d[1] + d[3] / 2 for d in best)
    fw = statistics.median(d[2] for d in best)
    aspect = (9 / 16) / share                                   # width / height of the top part
    bw = min(fw * 4.6, W * 0.5)
    bh = bw / aspect
    x0 = fx - bw / 2
    y0 = fy - 0.04 * bh - bh / 2
    x0 = min(max(0.0, x0), W - bw)
    y0 = min(max(0.0, y0), H - bh)
    if x0 < 0.03 * W:                                           # cams usually sit against an edge
        x0 = 0.0
    if x0 + bw > 0.97 * W:
        x0 = W - bw
    if y0 < 0.03 * H:
        y0 = 0.0
    if y0 + bh > 0.97 * H:
        y0 = H - bh
    cam = (round(x0 / W, 3), round(y0 / H, 3), round(bw / W, 3), round(bh / H, 3))
    return cam, len(best)


def detect_cam(path: Path, start: float, length: float, share: float = 0.36, n: int = 10):
    """Find the face cam by looking for a face that stays in the same place. Returns (cam, hits) or None."""
    try:
        import cv2
        import numpy as np
        xml = Path(cv2.data.haarcascades) / "haarcascade_frontalface_default.xml"
        if not xml.exists():
            return None
        casc = cv2.CascadeClassifier(str(xml))
        r = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                            "stream=width,height", "-of", "csv=p=0", str(path)], capture_output=True, text=True)
        line = next(ln for ln in r.stdout.splitlines() if ln.strip())
        w0, h0 = [int(v) for v in line.strip().split(",")[:2]]
    except (ImportError, OSError, StopIteration, ValueError, subprocess.SubprocessError):
        return None
    W = 960
    H = max(2, int(round(W * h0 / w0)))
    length = max(length, 2.0)
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-ss", f"{max(start, 0):.2f}", "-i", str(path),
           "-t", f"{length:.2f}", "-an", "-vf", f"fps={n / length:.4f},scale={W}:{H}",
           "-f", "rawvideo", "-pix_fmt", "gray", "-"]
    try:
        raw = subprocess.run(cmd, capture_output=True, timeout=120).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    dets = []
    for i in range(len(raw) // (W * H)):
        g = np.frombuffer(raw, np.uint8, W * H, i * W * H).reshape(H, W)
        g = cv2.equalizeHist(g)
        for (x, y, fw, fh) in casc.detectMultiScale(g, 1.1, 6, minSize=(26, 26), maxSize=(int(W * 0.22),) * 2):
            dets.append((int(x), int(y), int(fw), int(fh)))
    return cam_from_faces(dets, W, H, share)


def resolve_cam(a, path: Path, start: float, length: float) -> str:
    """The cam box to use: the one given with --cam, or an automatically found one (kept for the whole run)."""
    if a.cam != "auto":
        return a.cam
    got = getattr(a, "cam_resolved", None)
    if got:
        return got
    tries = getattr(a, "cam_tries", 0)
    if tries >= 3:
        return CAM_DEFAULT
    a.cam_tries = tries + 1
    found = detect_cam(path, start, length, a.cam_share)
    if found:
        cam, hits = found
        a.cam_resolved = ",".join(str(v) for v in cam)
        log(f"face cam found automatically: --cam {a.cam_resolved} (face seen in {hits} frames)")
        return a.cam_resolved
    if a.cam_tries >= 3:
        log(f"no face cam found - using the default box {CAM_DEFAULT} (set --cam x,y,w,h if it is wrong)")
    return CAM_DEFAULT


def split_cfg(a) -> dict:
    txt = getattr(a, "cam_resolved", None) or (CAM_DEFAULT if a.cam == "auto" else a.cam)
    return {"cam": parse_cam(txt), "share": a.cam_share, "game_x": a.game_x}


def video_filter_args(layout: str, out_h: int, split: dict | None = None) -> list[str]:
    """ffmpeg args that turn 16:9 into 9:16."""
    out_h = out_h or 0
    if layout == "split":
        # face cam on top, gameplay below (the layout most shorts use)
        cfg = split or {"cam": (0.0, 0.43, 0.2, 0.205), "share": 0.36, "game_x": 0.5}
        h = out_h or 1920
        w = int(h * 9 / 16) // 2 * 2
        ch = int(h * cfg["share"]) // 2 * 2
        gh = h - ch
        cx, cy, cw, cah = cfg["cam"]
        fc = (f"[0:v]split=2[a][b];"
              f"[a]crop=w='trunc(iw*{cw}/2)*2':h='trunc(ih*{cah}/2)*2':x='iw*{cx}':y='ih*{cy}',"
              f"scale={w}:{ch}:force_original_aspect_ratio=increase:flags=lanczos,crop={w}:{ch},"
              f"unsharp=5:5:0.8:5:5:0.3[top];"
              f"[b]crop=w='min(iw,trunc(ih*{w}/{gh}/2)*2)':h=ih:x='max(0,min(iw-ow,iw*{cfg['game_x']}-ow/2))':y=0,"
              f"scale={w}:{gh}:flags=lanczos,unsharp=5:5:0.6:5:5:0.2[bot];"
              f"[top][bot]vstack=inputs=2,setsar=1[v]")
        return ["-filter_complex", fc, "-map", "[v]", "-map", "0:a?"]
    if layout in ("podcast", "podcast-split"):
        h = out_h or 1920
        w = int(h * 9 / 16) // 2 * 2
        half_h = h // 2
        cfg = split or {"speaker1": 0.25, "speaker2": 0.75}
        x1_norm = cfg.get("speaker1", 0.25)
        x2_norm = cfg.get("speaker2", 0.75)
        fc = (f"[0:v]split=2[cam1][cam2];"
              f"[cam1]crop=w='min(iw,trunc(ih*0.95*{w}/{half_h}/2)*2)':h='trunc(ih*0.95/2)*2':"
              f"x='max(0,min(iw-ow,trunc((iw*{x1_norm}-ow/2)/2)*2))':y='max(0,trunc((ih*0.05)/2)*2)',"
              f"scale={w}:{half_h}:flags=lanczos,unsharp=5:5:0.5:5:5:0.0[top];"
              f"[cam2]crop=w='min(iw,trunc(ih*0.95*{w}/{half_h}/2)*2)':h='trunc(ih*0.95/2)*2':"
              f"x='max(0,min(iw-ow,trunc((iw*{x2_norm}-ow/2)/2)*2))':y='max(0,trunc((ih*0.05)/2)*2)',"
              f"scale={w}:{half_h}:flags=lanczos,unsharp=5:5:0.5:5:5:0.0[bot];"
              f"[top][bot]vstack=inputs=2,format=yuv420p,setsar=1[v]")
        return ["-filter_complex", fc, "-map", "[v]", "-map", "0:a?"]
    if layout == "blur":
        h = out_h or 1920
        w = int(h * 9 / 16) // 2 * 2
        fc = (f"[0:v]split=2[a][b];"
              f"[a]scale={w}:{h}:force_original_aspect_ratio=increase:flags=lanczos,crop={w}:{h},boxblur=25:5[bg];"
              f"[b]scale={w}:-2:flags=lanczos[fg];"
              f"[bg][fg]overlay=(W-w)/2:(H-h)/2:format=auto,format=yuv420p,setsar=1[v]")
        return ["-filter_complex", fc, "-map", "[v]", "-map", "0:a?"]
    vf = "crop=trunc(ih*9/16/2)*2:ih"
    if out_h:
        vf += f",scale={int(out_h * 9 / 16) // 2 * 2}:{out_h}:flags=lanczos,unsharp=5:5:0.5:5:5:0.0"
    return ["-vf", vf + ",format=yuv420p,setsar=1"]


def run_ffmpeg(cmd: list[str], cwd: Path | None = None) -> tuple[bool, str]:
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    return r.returncode == 0, r.stderr.strip().splitlines()[-1] if r.stderr.strip() else ""


def with_subtitles(args: list, ass_name: str) -> list:
    """Add the .ass overlay to the end of the video filter, so title + captions are drawn in the same encode."""
    args = list(args)
    if args[0] == "-filter_complex":
        fc = args[1]
        k = fc.rindex("[v]")
        args[1] = fc[:k] + "[v0];[v0]subtitles=" + ass_name + "[v]"
    else:                                                       # "-vf", "<chain>"
        args[1] = args[1] + ",subtitles=" + ass_name
    return args


def with_progress_bar(args: list, length: float, out_h: int) -> list:
    """Adds a sleek neon cyan progress bar (8px high) along the bottom edge."""
    h = out_h or 1920
    w = int(h * 9 / 16) // 2 * 2
    pb = f"drawbox=x=0:y={h - 10}:w='min({w},trunc(({w}*t/{max(1.0, length):.3f})/2)*2)':h=8:color=0x00F0FF@0.95:t=fill"
    args = list(args)
    if args[0] == "-filter_complex":
        fc = args[1]
        if "[v]" in fc:
            k = fc.rindex("[v]")
            args[1] = fc[:k] + f"[v_pb0];[v_pb0]{pb}[v]"
        else:
            args[1] = fc + f",{pb}"
    else:
        args[1] = args[1] + f",{pb}"
    return args


def audio_filter(av_offset_ms: int = 0, normalize: bool = False) -> str:
    """Keeps sound locked to the picture. av_offset_ms < 0 plays the sound earlier, > 0 later."""
    parts = []
    if av_offset_ms < 0:
        parts.append(f"atrim=start={-av_offset_ms / 1000:.3f},asetpts=PTS-STARTPTS")
    elif av_offset_ms > 0:
        parts.append(f"adelay={av_offset_ms}:all=1")
    parts.append("aresample=async=1:first_pts=0")
    if normalize:                                               # same loudness on every clip (-16 LUFS, like shorts apps)
        parts += ["loudnorm=I=-16:TP=-1.5:LRA=11", "aresample=48000"]
    return ",".join(parts)


def cut_vertical(src: Path, start: float, length: float, dst: Path, layout: str,
                 out_h: int, crf: int, preset: str, split: dict | None = None,
                 ass: Path | None = None, av_offset_ms: int = 0, normalize: bool = False,
                 progress_bar: bool = True) -> tuple[bool, str]:
    """One ffmpeg pass: cut + crop/layout + title/captions + progress bar + encode. No second encode, so no extra blur."""
    vargs = video_filter_args(layout, out_h, split)
    if progress_bar:
        vargs = with_progress_bar(vargs, length, out_h)
    if ass is not None:
        vargs = with_subtitles(vargs, ass.name)
    try:
        import viral_engine
        enc, enc_args = viral_engine.detect_gpu_encoder()
    except Exception:
        enc, enc_args = "libx264", ["-preset", preset, "-crf", str(crf)]
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-fflags", "+genpts",
           "-ss", f"{start:.3f}", "-i", str(Path(src).resolve()), "-t", f"{length:.3f}",
           *vargs, "-af", audio_filter(av_offset_ms, normalize),
           "-c:v", enc, *enc_args, "-pix_fmt", "yuv420p",
           "-profile:v", "high", "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", str(Path(dst).resolve())]
    return run_ffmpeg(cmd, cwd=ass.parent if ass is not None else None)


def cut_wide(src: Path, start: float, length: float, dst: Path) -> tuple[bool, str]:
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
           "-ss", f"{start:.3f}", "-i", str(src), "-t", f"{length:.3f}", "-c", "copy", str(dst)]
    return run_ffmpeg(cmd)


def srt_time(t: float) -> str:
    ms = int(round(max(t, 0) * 1000))
    h, ms = divmod(ms, 3_600_000)
    m, ms = divmod(ms, 60_000)
    s, ms = divmod(ms, 1000)
    return f"{h:02}:{m:02}:{s:02},{ms:03}"


def make_cues(result: dict, max_words: int = 6, max_dur: float = 3.5, gap: float = 0.8):
    """Whisper result -> short caption cues (vertical video wants 2-6 words per line)."""
    segs = [s for s in result.get("segments", []) if s.get("no_speech_prob", 0) < 0.6]
    words = [w for s in segs for w in s.get("words", [])]
    if not words:
        return [(s["start"], s["end"], s["text"].strip()) for s in segs if s["text"].strip()]
    cues, cur = [], []
    for w in words:
        if cur and (len(cur) >= max_words or w["end"] - cur[0]["start"] > max_dur
                    or w["start"] - cur[-1]["end"] > gap):
            cues.append(cur)
            cur = []
        cur.append(w)
        if re.search(r"[.!?]$", w["word"].strip()) and len(cur) >= 3:
            cues.append(cur)
            cur = []
    if cur:
        cues.append(cur)
    return [(c[0]["start"], c[-1]["end"], "".join(w["word"] for w in c).strip()) for c in cues]


def make_word_cues(result: dict, max_words: int = 4, max_dur: float = 2.8, gap: float = 0.7):
    """Whisper result -> lines of a few words, each word with its own timing, for highlighted captions.
    Returns [(start, end, [(word_start, word_end, word), ...]), ...]; [] when Whisper gave no word times."""
    segs = [s for s in result.get("segments", []) if s.get("no_speech_prob", 0) < 0.6]
    words = [w for s in segs for w in s.get("words", []) if w.get("word", "").strip()]
    lines, cur = [], []
    for w in words:
        if cur and (len(cur) >= max_words or w["end"] - cur[0]["start"] > max_dur
                    or w["start"] - cur[-1]["end"] > gap):
            lines.append(cur)
            cur = []
        cur.append(w)
        if re.search(r"[.!?,]$", w["word"].strip()) and len(cur) >= 2:
            lines.append(cur)
            cur = []
    if cur:
        lines.append(cur)
    return [(c[0]["start"], c[-1]["end"], [(w["start"], w["end"], w["word"].strip()) for w in c]) for c in lines]


def write_srt(cues, path: Path) -> None:
    out = [f"{i}\n{srt_time(s)} --> {srt_time(e)}\n{t}\n" for i, (s, e, t) in enumerate(cues, 1)]
    path.write_text("\n".join(out), encoding="utf-8")


# --------------------------------------------------------------------------- #
# on-screen text: hook title (written by a vision AI) + word captions, burned in with libass
# --------------------------------------------------------------------------- #
EMOJI_RE = re.compile("[\U00010000-\U0010ffff\u2600-\u27bf\ufe0f\u200d\u2b50\u2b06\u2934\u3030]")


def clean_title(raw: str) -> str:
    """Model output -> a short plain ALL-CAPS line (no quotes, hashtags, emoji or full stop)."""
    t = (raw or "").strip().splitlines()[0] if (raw or "").strip() else ""
    t = re.sub(r"^(title|hook)\s*[:\-]\s*", "", t, flags=re.I)
    t = t.strip(" \"'`*_#")
    t = EMOJI_RE.sub("", t)
    t = re.sub(r"#\w+", "", t)
    t = re.sub(r"\s+", " ", t).strip(" .")
    return t.upper()[:70]


def ass_time(t: float) -> str:
    cs = int(round(max(t, 0) * 100))
    h, cs = divmod(cs, 360000)
    m, cs = divmod(cs, 6000)
    sec, cs = divmod(cs, 100)
    return f"{h}:{m:02}:{sec:02}.{cs:02}"


def ass_text(t: str) -> str:
    return t.replace("\\", "").replace("{", "(").replace("}", ")").replace("\n", "\\N")


STYLES = {   # colours are ASS BGR: &H00BBGGRR
    "classic": dict(tsize=0.0385, csize=0.034, tcolor="&H00FFFFFF", ccolor="&H00FFFFFF", hl="&H0000E5FF", pop=114, thick=0.09, box=False),
    "bold":    dict(tsize=0.045, csize=0.046, tcolor="&H0000E5FF", ccolor="&H00FFFFFF", hl="&H000080FF", pop=120, thick=0.12, box=False),
    "neon":    dict(tsize=0.040, csize=0.036, tcolor="&H00FF40FF", ccolor="&H00FFFFFF", hl="&H00FFFF00", pop=116, thick=0.10, box=False),
    "minimal": dict(tsize=0.032, csize=0.028, tcolor="&H00FFFFFF", ccolor="&H00EEEEEE", hl="&H0000E5FF", pop=100, thick=0.06, box=False),
    "boxed":   dict(tsize=0.036, csize=0.034, tcolor="&H0000E5FF", ccolor="&H00FFFFFF", hl="&H0000E5FF", pop=108, thick=0.09, box=True),
}


def make_ass(cues, title: str, layout: str, out_h: int, cam_share: float, duration: float, caps: bool,
             wordcues=None, style: str = "classic", hook_banner: bool = False) -> str:
    """One .ass file with the hook title and the word captions."""
    h = out_h or 1920
    w = int(h * 9 / 16) // 2 * 2
    st_ = STYLES.get(style, STYLES["classic"])
    tsize, csize = round(h * st_["tsize"]), round(h * st_["csize"])
    title_y = int(h * cam_share) if layout == "split" else (int(h * 0.50) if layout in ("podcast", "podcast-split") else int(h * 0.20))
    cap_margin = round(h * 0.17)
    head = ("[Script Info]\nScriptType: v4.00+\nWrapStyle: 0\nScaledBorderAndShadow: yes\n"
            f"PlayResX: {w}\nPlayResY: {h}\n\n[V4+ Styles]\n"
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, "
            "Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
            "Alignment, MarginL, MarginR, MarginV, Encoding\n"
            f"Style: Title,Arial,{tsize},{st_['tcolor']},&H000000FF,&H00000000,&H64000000,-1,0,0,0,100,100,0,0,"
            f"{3 if st_['box'] else 1},{max(3, round(tsize * (0.30 if st_['box'] else st_['thick'])))},0,5,60,60,0,1\n"
            f"Style: Cap,Arial,{csize},{st_['ccolor']},&H000000FF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,"
            f"{max(3, round(csize * st_['thick'] * 0.9))},0,2,60,60,{cap_margin},1\n\n"
            "[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n")
    lines = []
    if title:
        wrapped = "\\N".join(textwrap.wrap(ass_text(title), 20))
        if hook_banner:
            # Top 0-4.5s Hook Banner: high-retention scale pop-in & smooth fade-out
            hook_dur = min(4.5, duration) if duration > 5.5 else duration
            t_fade_start = int(max(0.0, hook_dur - 0.5) * 1000)
            t_fade_end = int(hook_dur * 1000)
            lines.append(f"Dialogue: 1,{ass_time(0)},{ass_time(hook_dur)},Title,,0,0,0,,"
                         f"{{\\an5\\pos({w // 2},{title_y})\\fade(150,0,0,0,{t_fade_start},{t_fade_end})\\t(0,250,\\fscx100\\fscy100)}}{{\\fscx110\\fscy110}}{wrapped}")
        else:
            lines.append(f"Dialogue: 1,{ass_time(0)},{ass_time(duration)},Title,,0,0,0,,"
                         f"{{\\an5\\pos({w // 2},{title_y})}}{wrapped}")
    if wordcues:                                        # one event per spoken word: that word lights up
        for ci, (st, en, words) in enumerate(wordcues):
            line_after = wordcues[ci + 1][0] if ci + 1 < len(wordcues) else None
            texts = [ass_text(t.upper() if caps else t) for _, _, t in words]
            for i, (ws, we, _) in enumerate(words):
                nxt = words[i + 1][0] if i + 1 < len(words) else None
                end = nxt if (nxt is not None and nxt - we < 0.4) else we + (0.12 if nxt is not None else 0.25)
                if nxt is None and line_after is not None:
                    end = min(end, line_after)
                shown = " ".join(("{\\c" + st_["hl"] + "&\\fscx" + str(st_["pop"]) + "\\fscy" + str(st_["pop"]) + "}" + t + "{\\r}") if j == i else t
                                 for j, t in enumerate(texts))
                lines.append(f"Dialogue: 0,{ass_time(ws)},{ass_time(end)},Cap,,0,0,0,,{shown}")
    else:
        for st, en, t in cues:
            t = ass_text(t.upper() if caps else t)
            lines.append(f"Dialogue: 0,{ass_time(st)},{ass_time(en)},Cap,,0,0,0,,{t}")
    return head + "\n".join(lines) + "\n"


def burn_overlays(clip: Path, cues, title: str, a, wordcues=None) -> tuple[bool, str, Path]:
    """Burn title and/or captions into `<clip>_final.mp4`."""
    dur = probe_duration(clip) or 60.0
    ass = clip.with_suffix(".ass")
    ass.write_text(make_ass(cues, title, a.layout, a.out_height, a.cam_share, dur, a.layout == "split", wordcues, a.style),
                   encoding="utf-8")
    out = clip.with_name(clip.stem + "_final.mp4")
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", clip.name,
           "-vf", f"subtitles={ass.name}",
           "-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-pix_fmt", "yuv420p",
           "-c:a", "copy", "-movflags", "+faststart", out.name]
    ok, err = run_ffmpeg(cmd, cwd=clip.parent)
    ass.unlink(missing_ok=True)
    return ok, err, out


TITLE_PROMPT = """You are the editor of a viral short-form channel (TikTok / YouTube Shorts / Reels). A moment was cut automatically from a live stream. You get {n} frames from it in time order, the transcript and some context.

JOB 1 - judge it. Rate 1 to 10 how likely a stranger is to watch this to the end and share it as a standalone short. Be harsh: menus, loading screens, walking around, idle talking, chat reading, or a loud noise with nothing happening = 1 to 3. A clear event with a payoff (a fight, a fail, a surprise, a funny reaction, a clutch play, a real story) = 6 to 10. Say why in one short sentence.

JOB 2 - write FIVE different on-screen hook titles, 4 to 9 words each, ALL CAPS, no hashtags, no quotation marks, no emoji, no full stop. Each one must use a DIFFERENT angle from this list: {angles}.
Rules for the titles:
- Be specific: use a concrete detail you can see or that is said (an object, a number, a place, a name, the exact thing that went wrong). Generic hype is forbidden.
- Never use these overused words: {banned}.
- Do not repeat what the streamer says word for word; add what the viewer would not know.
- Only describe what you can see or what the context says. If you cannot tell what happens, rate it low.
- Do not identify real people from their faces. A streamer name may be used if given.
- Do not reuse the wording or sentence shape of these recent titles: {recent}

JOB 2b - find the real start and end. The clip window runs from 0 to {length} seconds and the strongest moment is at about {pos_rel} seconds. The transcript lines show [seconds from the window start]. Return keep_from and keep_to (seconds): the tightest range that contains the whole setup, the punchline or peak, and the reaction right after it. A joke or story needs its setup; a fight or action starts 2 to 3 seconds before the action begins and ends when it settles. Never start or end in the middle of a sentence. Keep at least 10 seconds. If the whole window is already right, return 0 and {length}.

JOB 3 - pick the best candidate, write a 1 to 2 sentence post description (plain words, no hashtags) and 5 relevant hashtags.

Context:
- Stream title: {stream_title}
- Streamer: {streamer}
- Why this moment was picked: {why}
- Chat reacted with: {chat}
- Transcript with times (may be empty or wrong):
{said}

Reply with JSON only, exactly in this shape:
{{"rating": 7, "reason": "...", "keep_from": 0, "keep_to": 40, "candidates": ["TITLE ONE", "TITLE TWO", "TITLE THREE", "TITLE FOUR", "TITLE FIVE"], "best": 0, "description": "...", "hashtags": ["#one", "#two", "#three", "#four", "#five"]}}
"best" is the 0-based number of the chosen candidate."""

TITLE_ANGLES = ["a short quote from the clip turned into a hook", "a question the viewer must see answered",
                "a cliffhanger that teases the outcome without giving it away", "deadpan understatement of chaos",
                "a specific number or detail from the clip", "the plan versus what actually happened",
                "what the streamer stands to lose", "POV: you are watching it happen", "the one tiny mistake that ruins everything",
                "a dramatic one-word-then-detail line"]
TITLE_BANNED = "INSANE, CRAZY, EPIC, UNBELIEVABLE, WILD, MIND-BLOWING, HILARIOUS, AMAZING, LEGENDARY, SHOCKING, GONE WRONG"


def title_prompt(n: int, info: dict) -> str:
    angles = random.sample(TITLE_ANGLES, 5)
    recent = info.get("avoid") or []
    return TITLE_PROMPT.format(
        n=n, angles="; ".join(angles), banned=TITLE_BANNED, recent=" | ".join(recent) or "none yet",
        length=f"{info.get('length', 0):.0f}", pos_rel=f"{info.get('pos_rel', 0):.0f}",
        stream_title=info.get("stream_title") or "unknown", streamer=info.get("streamer") or "unknown",
        why=info.get("why") or "unknown", said=(info.get("said") or "nothing clear")[:3500],
        chat=" | ".join(info.get("chat") or []) or "no chat available")


def parse_title_reply(text: str) -> dict:
    """Model reply -> {"title", "candidates", "description", "hashtags"}. Plain-text replies still work."""
    out = {"title": "", "candidates": [], "description": "", "hashtags": [], "rating": None, "reason": "",
           "keep_from": None, "keep_to": None}
    m = re.search(r"\{.*\}", text or "", re.S)
    data = None
    if m:
        try:
            data = json.loads(m.group(0))
        except ValueError:
            data = None
    if isinstance(data, dict):
        cands = [clean_title(str(c)) for c in (data.get("candidates") or []) if isinstance(c, str)]
        cands = [c for c in cands if c]
        try:
            best = int(data.get("best", 0))
        except (TypeError, ValueError):
            best = 0
        if cands:
            out["candidates"] = cands
            out["title"] = cands[best] if 0 <= best < len(cands) else cands[0]
        try:
            out["rating"] = max(1, min(10, int(round(float(data.get("rating"))))))
        except (TypeError, ValueError):
            out["rating"] = None
        for k_ in ("keep_from", "keep_to"):
            try:
                out[k_] = float(data.get(k_))
            except (TypeError, ValueError):
                out[k_] = None
        out["reason"] = re.sub(r"\s+", " ", str(data.get("reason") or "")).strip()[:200]
        out["description"] = re.sub(r"\s+", " ", str(data.get("description") or "")).strip()[:300]
        tags = [("#" + re.sub(r"\W", "", str(t))) for t in (data.get("hashtags") or [])]
        out["hashtags"] = [t for t in dict.fromkeys(tags) if len(t) > 1][:6]
        return out
    out["title"] = clean_title(text)
    if out["title"]:
        out["candidates"] = [out["title"]]
    return out


def title_request(provider: str, model: str, key: str, prompt: str, images: list, base: str = ""):
    """Build (url, headers, body) for a vision request. `images` = base64 JPEG strings."""
    if provider == "gemini":
        url = (base.rstrip("/") if base else "https://generativelanguage.googleapis.com") + \
              f"/v1beta/models/{model}:generateContent"
        parts = [{"text": prompt}] + [{"inline_data": {"mime_type": "image/jpeg", "data": im}} for im in images]
        body = {"contents": [{"parts": parts}], "generationConfig": {"temperature": 1.0, "maxOutputTokens": 2000,
                                                          "thinkingConfig": {"thinkingBudget": 0}}}
        return url, {"x-goog-api-key": key, "Content-Type": "application/json"}, body
    url = (base.rstrip("/") if base else "https://api.anthropic.com") + "/v1/messages"
    content = [{"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": im}}
               for im in images] + [{"type": "text", "text": prompt}]
    body = {"model": model, "max_tokens": 600, "messages": [{"role": "user", "content": content}]}
    return url, {"x-api-key": key, "anthropic-version": "2023-06-01", "Content-Type": "application/json"}, body


def title_response(provider: str, resp: dict) -> str:
    if provider == "gemini":
        cands = resp.get("candidates") or []
        parts = (cands[0].get("content", {}).get("parts") or []) if cands else []
        return "".join(p.get("text", "") for p in parts if not p.get("thought"))
    return "".join(b.get("text", "") for b in resp.get("content", []) if b.get("type") == "text")


SWEEP_PROMPT = """You are scouting a gaming / streaming video for moments worth turning into a short vertical clip.
You get {n} frames in time order. Frame times (seconds): {times}.
Spoken words with times (may be empty): {said}

Find up to {k} moments between {a}s and {b}s that a viewer would want to watch and share: a surprise or reveal, a funny or shocking event,
a big reaction, a fight or win/fail, something unexpected happening on screen - even if it is quiet.
Skip menus, loading screens, idle walking and plain talking.
Give the time (in seconds, as exact as the frames allow) when the moment STARTS to get interesting, a score 1-10, and a 6-12 word reason.
Reply with JSON only: {{"moments":[{{"at":123,"score":8,"why":"..."}}]}}  (empty list if nothing is clip-worthy)"""


def parse_sweep_reply(text: str, lo: float, hi: float) -> list:
    m = re.search(r"\{.*\}", text or "", re.S)
    if not m:
        return []
    try:
        data = json.loads(m.group(0))
    except ValueError:
        return []
    out = []
    for it in (data.get("moments") or []) if isinstance(data, dict) else []:
        try:
            at, sc = float(it["at"]), float(it.get("score", 6))
        except (KeyError, TypeError, ValueError):
            continue
        if lo - 1 <= at <= hi + 1:
            out.append((min(max(at, lo), hi), max(0.0, min(10.0, sc)), str(it.get("why", ""))[:100]))
    return sorted(out, key=lambda x: -x[1])


class TitleWriter:
    """Looks at a few frames of a finished clip and writes the hook title with a vision model
    (Google Gemini or Anthropic Claude, whichever API key is set)."""
    DEFAULT_MODEL = {"gemini": "gemini-1.5-flash", "claude": "claude-3-5-haiku-latest"}

    def __init__(self, a):
        self.a = a
        mode = a.title_ai
        gem = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        cla = os.environ.get("ANTHROPIC_API_KEY")
        self.provider, self.key = None, None
        if mode in ("auto", "gemini") and gem:
            self.provider, self.key = "gemini", gem
            if not gem.startswith("AIzaSy") and not gem.startswith("test"):
                self._warn("GEMINI_API_KEY does not look like a standard Google AI Studio key (expected 'AIzaSy...'). "
                           "Get a valid free key at https://aistudio.google.com")
        elif mode in ("auto", "claude") and cla and len(cla) > 30 and cla.startswith("sk-ant-"):
            self.provider, self.key = "claude", cla
        self.model = a.title_model or (self.DEFAULT_MODEL[self.provider] if self.provider else "")
        self.base = os.environ.get("CLIPWATCHER_TITLE_URL", "")      # for testing against a local fake server
        self.warned = set()
        self.recent: list = []                                       # last titles, so the next ones differ

    @property
    def ready(self) -> bool:
        return self.provider is not None

    def _warn(self, msg: str) -> None:
        if msg not in self.warned:
            self.warned.add(msg)
            log(f"title: {msg}")

    def _frames(self, clip: Path, length: float, n: int = 6, start: float = 0.0) -> list:
        with tempfile.TemporaryDirectory() as d:
            ok, err = run_ffmpeg(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                                  "-ss", f"{max(start, 0):.3f}", "-t", f"{max(length, 1.0):.3f}", "-i", str(clip),
                                  "-vf", f"fps={n / max(length, 1.0):.4f},scale=448:-2", "-frames:v", str(n),
                                  "-q:v", "4", str(Path(d) / "f%02d.jpg")])
            files = sorted(Path(d).glob("f*.jpg"))[:n]
            return [base64.b64encode(f.read_bytes()).decode() for f in files]

    def _ask(self, prompt: str, frames: list):
        """One call to the vision model. Returns the reply text, or None (after warning) on failure."""
        url, headers, body = title_request(self.provider, self.model, self.key, prompt, frames, self.base)
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers)
                with urllib.request.urlopen(req, timeout=150) as r:
                    resp = json.loads(r.read().decode("utf-8", "replace"))
                return title_response(self.provider, resp)
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", "replace")
                m = re.search(r'"message":\s*"([^"]+)"', detail)
                reason = (m.group(1) if m else detail[:150]).strip()
                gc = body.get("generationConfig", {})
                if e.code == 400 and "thinkingConfig" in gc and re.search(r"think", reason, re.I):
                    gc.pop("thinkingConfig")          # this model doesn't accept it
                    continue
                if e.code in (429, 500, 502, 503) and attempt < 2:
                    time.sleep(4 * (attempt + 1))
                    continue
                hint = " (check the key and --title-model)" if e.code in (400, 401, 403, 404) else ""
                self._warn(f"{self.provider} said HTTP {e.code}: {reason[:140]}{hint}")
                return None
            except (TimeoutError, OSError) as e:      # slow network / busy model: try again
                if attempt < 2:
                    log(f"title: the AI is slow to answer ({type(e).__name__}), trying again...")
                    continue
                self._warn(f"no answer from {self.provider} after 3 tries ({str(e)[:80]}) - check your internet / try again")
                return None
        return None

    def sweep(self, path: Path, start: float, length: float, transcript: str = "", k: int = 4, n: int = 16) -> list:
        """Ask the vision model for the most clip-worthy moments in [start, start+length] of a video,
        even quiet ones (a surprise, a reveal). Returns [(absolute_seconds, score 0-10, why)]."""
        if not self.ready:
            return []
        try:
            frames = self._frames(path, length, n=n, start=start)
            if not frames:
                return []
            step = length / max(len(frames), 1)
            times = ", ".join(f"{i + 1}={start + i * step:.0f}s" for i in range(len(frames)))
            text = self._ask(SWEEP_PROMPT.format(n=len(frames), times=times, k=k, said=(transcript or "nothing clear")[:6000],
                                                  a=f"{start:.0f}", b=f"{start + length:.0f}"), frames)
            return parse_sweep_reply(text or "", start, start + length)
        except Exception as e:  # noqa: BLE001
            self._warn(f"sweep failed: {str(e).splitlines()[0][:140] if str(e) else type(e).__name__}")
            return []

    def write(self, clip: Path, length: float, info: dict, start: float = 0.0) -> dict:
        """Returns {"title", "candidates", "description", "hashtags"}; title is '' if it could not be made."""
        empty = {"title": "", "candidates": [], "description": "", "hashtags": [], "rating": None, "reason": "",
                 "keep_from": None, "keep_to": None}
        if not self.ready:
            return empty
        try:
            frames = self._frames(clip, length, start=start)
            if not frames:
                self._warn("couldn't read frames from the clip")
                return empty
            text = self._ask(title_prompt(len(frames), dict(info, avoid=self.recent, length=length)), frames)
            if text is None:
                return empty
            res = parse_title_reply(text)
            if res["title"]:
                self.recent = (self.recent + [res["title"]])[-8:]
            return res
        except Exception as e:  # noqa: BLE001
            self._warn(f"failed: {str(e).splitlines()[0][:140] if str(e) else type(e).__name__}")
            return empty


# --------------------------------------------------------------------------- #
# subtitles worker (one thread, model loaded once)
# --------------------------------------------------------------------------- #
def info_dir(clip: Path) -> Path:
    d = clip.parent / "info"
    d.mkdir(exist_ok=True)
    return d


def save_post(clip: Path, res: dict, transcript: str = "") -> None:
    """Write the AI's extra output: info/<clip>.meta.json and <clip>_post.txt (ready to paste)."""
    if not (res.get("title") or res.get("description")):
        return
    meta = dict(res, transcript=transcript[:2000])
    (info_dir(clip) / f"{clip.stem}.meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    lines = [res.get("title", ""), "", res.get("description", ""), "", " ".join(res.get("hashtags", []))]
    if len(res.get("candidates", [])) > 1:
        lines += ["", "Other title ideas:"] + [f"- {c}" for c in res["candidates"] if c != res.get("title")]
    (info_dir(clip) / f"{clip.stem}_post.txt").write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def have_asr() -> bool:
    return bool(importlib.util.find_spec("faster_whisper") or importlib.util.find_spec("whisper"))


class Asr:
    """Speech to text. Uses faster-whisper when installed (about 4x faster on a normal PC, no PyTorch),
    otherwise openai-whisper. transcribe() always returns the same dict shape: {"segments": [...], "text": ...}."""

    def __init__(self, model: str, lang: str = ""):
        self.name, self.lang = model, (lang or None)
        self.model, self.kind = None, ""
        self.device = "auto"
        self.lock = threading.Lock()

    def _load(self) -> None:
        if self.model is not None:
            return
        if importlib.util.find_spec("faster_whisper"):
            from faster_whisper import WhisperModel  # noqa: PLC0415
            log(f"speech-to-text: loading '{self.name}' (faster-whisper; the first time it downloads the model)")
            self.model, self.kind = WhisperModel(self.name, device=self.device, compute_type="auto" if self.device == "auto" else "int8"), "fw"
        else:
            import whisper  # noqa: PLC0415
            log(f"speech-to-text: loading '{self.name}' (openai-whisper; the first time it downloads the model)")
            self.model, self.kind = whisper.load_model(self.name), "ow"

    def transcribe(self, wav: Path, words: bool = True) -> dict:
        with self.lock:
            self._load()
            if self.kind == "fw":
                try:
                    segs, _info = self.model.transcribe(str(wav), language=self.lang, word_timestamps=words,
                                                        vad_filter=True, condition_on_previous_text=False)
                    segs = list(segs)
                except Exception as e:  # noqa: BLE001
                    if self.device == "cpu" or not re.search(r"cublas|cudnn|cuda|\.dll", str(e), re.I):
                        raise
                    log("speech-to-text: the GPU libraries are missing, switching to the CPU (slower, but it works)")
                    self.device, self.model = "cpu", None
                    self._load()
                    segs, _info = self.model.transcribe(str(wav), language=self.lang, word_timestamps=words,
                                                        vad_filter=True, condition_on_previous_text=False)
                    segs = list(segs)
                out = []
                for sg in segs:
                    out.append({"start": sg.start, "end": sg.end, "text": sg.text,
                                "no_speech_prob": getattr(sg, "no_speech_prob", 0.0) or 0.0,
                                "words": [{"word": w.word, "start": w.start, "end": w.end} for w in (sg.words or [])]})
                return {"segments": out, "text": "".join(x["text"] for x in out)}
            import torch  # noqa: PLC0415
            return self.model.transcribe(str(wav), language=self.lang, word_timestamps=words,
                                         fp16=torch.cuda.is_available(), condition_on_previous_text=False)


def extract_wav(src: Path, start: float, length: float, dst: Path) -> bool:
    ok, _ = run_ffmpeg(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", f"{max(0.0, start):.3f}",
                        "-t", f"{length:.3f}", "-i", str(src), "-vn", "-ac", "1", "-ar", "16000", str(dst)])
    return ok


class Transcriber:
    """Captions for one clip, straight from the recording (before the clip is encoded)."""

    def __init__(self, a):
        self.a = a
        self.asr = Asr(a.whisper_model, a.lang)
        self.failed = False

    def transcribe(self, src: Path, start: float, length: float):
        """Whisper-style result for [start, start+length] of the recording, or None."""
        if self.failed:
            return None
        wav = Path(tempfile.gettempdir()) / f"cw_sub_{threading.get_ident()}.wav"
        try:
            if not extract_wav(src, start, length, wav):
                return None
            return self.asr.transcribe(wav)
        except Exception as e:  # noqa: BLE001
            log(f"subs: speech-to-text failed, clips will have no captions "
                f"({str(e).splitlines()[0][:140] if str(e) else type(e).__name__})")
            self.failed = True
            return None
        finally:
            wav.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# clip maker
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# moments: combine chat + audio + motion + speech into one scored, ranked clip
# --------------------------------------------------------------------------- #
SPEECH_HYPE = ["no way", "let's go", "lets go", "oh my god", "omg", "oh my gosh", "what the hell", "holy shit",
               "holy", "insane", "crazy", "wtf", "oh no", "clip that", "clip it", "bruh", "no no no", "yes yes yes",
               "i'm dead", "i'm dying", "yooo", "what is that", "are you serious", "i can't", "shut up",
               "oh shit", "we won", "i did it", "let him cook", "sheesh", "what the", "oh my", "no no", "yes yes"]
SPEECH_WEAK = ["bro", "dude", "wow", "damn", "come on", "ahh", "ayo", "yes", "lol", "haha"]   # common filler: counts a little
WEIGHTS = {"chat": 1.0, "audio": 1.0, "motion": 0.7, "speech": 1.0, "ai": 1.3}
BASE_WEIGHTS = dict(WEIGHTS)
CONF_RANK = {"low": 0, "med": 1, "high": 2}
SNAP_FWD = 4.0   # extra footage needed after the clip end when snapping to a pause


def text_score(text: str, extra=()) -> tuple[float, list]:
    """How hype is what the streamer said? Returns (strength 0..2, matched phrases).
    Strong reactions ("no way", "let's go") count a lot, filler words ("bro", "damn") only a little."""
    low = re.sub(r"[^a-z0-9' ]+", " ", text.lower().replace("’", "'"))
    low = " " + re.sub(r"\s+", " ", low) + " "
    strong, weak = [], []
    for ph in list(SPEECH_HYPE) + [w.strip().lower() for w in extra if w.strip()]:
        n = low.count(" " + ph + " ")
        if n:
            strong += [ph] * min(n, 2)
    for ph in SPEECH_WEAK:
        n = low.count(" " + ph + " ")
        if n:
            weak += [ph] * min(n, 2)
    if not strong and len(weak) < 2:        # one "bro" on its own means nothing
        return 0.0, []
    return min(2.0, 0.7 * len(strong) + 0.2 * len(weak) + (0.3 if strong else 0.0)), strong + weak


def score_moment(ev: dict) -> tuple[float, str]:
    """ev: source -> {"strength": float, ...}. More independent signals = more confidence."""
    score = sum(WEIGHTS.get(k, 1.0) * v["strength"] for k, v in ev.items())
    n = len(ev)
    if n >= 3 or (n == 2 and score >= 2.5):
        conf = "high"
    elif n == 2 or score >= 1.8:
        conf = "med"
    else:
        conf = "low"
    return score, conf


@dataclasses.dataclass
class Moment:
    rec: "Recording"
    first_seen: float
    ev: dict = dataclasses.field(default_factory=dict)   # source -> {strength, pos, detail}
    chat: "Spike | None" = None
    speech_text: str = ""

    def ref_pos(self) -> float:
        """Best position estimate: audio/motion know it exactly; chat's is lagged."""
        media = [v["pos"] for k, v in self.ev.items() if k in ("audio", "motion")]
        if media:
            return statistics.median(media)
        pos = [v["pos"] for v in self.ev.values()]
        return statistics.median(pos) if pos else 0.0


class Fusion:
    """Groups signals that happen within `window` seconds of each other into one Moment."""

    def __init__(self, window=15.0, wait=20.0, cooldown=30.0):
        self.window, self.wait, self.cooldown = window, wait, cooldown
        self.open: list[Moment] = []
        self.done: list[tuple] = []

    def add(self, rec, src, pos, strength, detail, spike=None, now=None) -> str:
        """Returns 'new', 'joined' (same moment as an open one) or 'dup' (already clipped)."""
        now = time.time() if now is None else now
        for r, p, _ in self.done:
            if r is rec and abs(pos - p) <= self.cooldown:
                return "dup"
        entry = {"strength": strength, "pos": pos, "detail": detail}
        for m in self.open:
            if m.rec is rec and abs(pos - m.ref_pos()) <= self.window:
                if src not in m.ev or strength > m.ev[src]["strength"]:
                    m.ev[src] = entry
                if spike is not None:
                    m.chat = spike
                return "joined"
        m = Moment(rec, now, {src: entry}, spike)
        self.open.append(m)
        return "new"

    def due(self, now=None) -> list:
        """Moments that have waited long enough for the slower signals (chat) to show up."""
        now = time.time() if now is None else now
        out = [m for m in self.open if now - m.first_seen >= self.wait]
        for m in out:
            self.open.remove(m)
            self.done.append((m.rec, m.ref_pos(), now))
        return out


class SpeechScorer:
    """Transcribes a few seconds around a moment and looks for reactions."""

    def __init__(self, a):
        self.a, self.failed = a, False
        self.asr = Asr(a.speech_model, a.lang)

    def score(self, path: Path, pos: float):
        """Returns (strength, matched_phrases, text) or None."""
        if self.failed:
            return None
        a = self.a
        wav = Path(tempfile.gettempdir()) / f"cw_speech_{threading.get_ident()}.wav"
        try:
            if not extract_wav(path, max(0.0, pos - 10), 14, wav):
                return None
            res = self.asr.transcribe(wav, words=False)
            text = (res.get("text") or "").strip()
            strength, hits = text_score(text, [w for w in a.hype_words.split(",")] if a.hype_words else [])
            return strength, hits, text
        except Exception as e:  # noqa: BLE001
            log(f"speech: scoring disabled ({str(e).splitlines()[0][:120] if str(e) else type(e).__name__})")
            self.failed = True
            return None
        finally:
            wav.unlink(missing_ok=True)


def find_pauses(levels, step: float, t0: float = 0.0, min_len: float = 0.25, drop: float = 14.0) -> list:
    """Midpoints of pauses in a loudness series (dB per `step` seconds). A pause is a stretch at least
    `drop` dB below the typical level that lasts `min_len` seconds, so it adapts to quiet and loud streams."""
    if not levels:
        return []
    thr = statistics.median(levels) - drop
    need = max(1, int(round(min_len / step)))
    out, run = [], 0
    for i, v in enumerate(levels + [1e9]):
        if v < thr:
            run += 1
        else:
            if run >= need:
                out.append(t0 + (i - run / 2) * step)
            run = 0
    return out


def snap_to_pause(path: Path, target: float, back: float, fwd: float) -> float:
    """Move `target` to the nearest pause in the audio (within back/fwd seconds), so clips don't
    start or end in the middle of a word. Returns `target` unchanged if there is no pause."""
    lo = max(0.0, target - back)
    span = (target - lo) + fwd
    rate, hop = 8000, 400                       # 50 ms frames
    try:
        r = subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-ss", f"{lo:.3f}", "-t", f"{span:.3f}",
                            "-i", str(path), "-vn", "-ac", "1", "-ar", str(rate), "-f", "s16le", "pipe:1"],
                           capture_output=True, timeout=60)
    except (subprocess.SubprocessError, OSError):
        return target
    arr = array.array("h")
    arr.frombytes(r.stdout[: len(r.stdout) // 2 * 2])
    if sys.byteorder == "big":
        arr.byteswap()
    levels = []
    for i in range(0, len(arr) - hop + 1, hop):
        rms = math.sqrt(sum(x * x for x in arr[i:i + hop]) / hop)
        levels.append(20 * math.log10(max(rms, 1.0) / 32768.0))
    pauses = find_pauses(levels, hop / rate, lo)
    return min(pauses, key=lambda t: abs(t - target)) if pauses else target


def audio_levels(path: Path, lo: float, span: float, hop_s: float = 0.5):
    """Loudness (dB) of `span` seconds of audio from `lo`, one value per `hop_s`. [] if it can't be read."""
    rate = 8000
    hop = int(rate * hop_s)
    try:
        r = subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-ss", f"{max(lo, 0):.3f}",
                            "-t", f"{span:.3f}", "-i", str(path), "-vn", "-ac", "1", "-ar", str(rate),
                            "-f", "s16le", "pipe:1"], capture_output=True, timeout=60)
    except (subprocess.SubprocessError, OSError):
        return []
    arr = array.array("h")
    arr.frombytes(r.stdout[: len(r.stdout) // 2 * 2])
    if sys.byteorder == "big":
        arr.byteswap()
    out = []
    for i in range(0, len(arr) - hop + 1, hop):
        rms = math.sqrt(sum(x * x for x in arr[i:i + hop]) / hop)
        out.append(20 * math.log10(max(rms, 1.0) / 32768.0))
    return out


def reaction_end(levels, step: float, t0: float, pos: float, min_post: float, max_post: float,
                 margin: float = 3.0, hold: float = 2.5, tail: float = 1.5) -> float:
    """
    Seconds after `pos` at which the reaction has died down. The reaction is alive while the (2 s smoothed)
    loudness is more than `margin` dB above the stream's normal level; the clip ends `tail` s after it has
    stayed calm for `hold` s, but never before `min_post` or after `max_post`.
    """
    if len(levels) < 8:
        return max_post
    before = levels[:max(0, int((pos - t0) / step))]
    base = statistics.median(before if len(before) >= 8 else levels)      # what 'normal' sounded like
    k = max(1, int(round(2.0 / step)))
    sm = [sum(levels[max(0, i - k + 1):i + 1]) / len(levels[max(0, i - k + 1):i + 1]) for i in range(len(levels))]
    need = max(1, int(round(hold / step)))
    calm = 0
    for i, v in enumerate(sm):
        t = t0 + i * step
        if t < pos + min_post - tail:
            continue
        calm = calm + 1 if v <= base + margin else 0
        if calm >= need:
            end = (t - (need - 1) * step) + tail - pos
            return max(min_post, min(max_post, end))
    return max_post


def abs_words(result, offset: float) -> list:
    """Whisper-style result for a window that starts at `offset` -> flat list of words in recording time."""
    out = []
    for sg in (result or {}).get("segments", []):
        if sg.get("no_speech_prob", 0) >= 0.6:
            continue
        for w in sg.get("words", []) or []:
            if w.get("word", "").strip():
                out.append({"word": w["word"], "start": w["start"] + offset, "end": w["end"] + offset})
    return out


def utterances(words: list, gap: float = 0.6) -> list:
    """Words -> sentences/phrases [(start, end, text)], split at . ! ? or a pause of `gap` seconds."""
    out, cur = [], []
    for w in words:
        if cur and w["start"] - cur[-1]["end"] > gap:
            out.append(cur)
            cur = []
        cur.append(w)
        if re.search(r"[.!?]$", w["word"].strip()):
            out.append(cur)
            cur = []
    if cur:
        out.append(cur)
    return [(c[0]["start"], c[-1]["end"], "".join(w["word"] for w in c).strip()) for c in out]


def sentence_bounds(utts: list, start: float, end: float, pos: float, lo: float, hi: float,
                    max_len: float = 75.0, lead: float = 0.25, tail: float = 0.5) -> tuple:
    """
    Never start or end in the middle of a sentence. If `start` falls inside a sentence, begin where that sentence
    begins (if that is not before `lo`), otherwise at the next one. If `end` falls inside a sentence, finish it
    (if that is not after `hi`), otherwise end after the previous one. The moment `pos` always stays inside.
    """
    for s0, e0, _ in utts:
        if s0 + 0.3 < start < e0 - 0.3:
            start = s0 - lead if s0 - lead >= lo else e0 + 0.1
            break
    for s0, e0, _ in utts:
        if s0 + 0.3 < end < e0 - 0.3:
            end = e0 + tail if e0 + tail <= hi else s0 - 0.1
            break
    if end - start > max_len:                                   # too long: keep the end, start at a sentence start
        start = end - max_len
        for s0, e0, _ in utts:
            if s0 >= start:
                start = s0 - lead
                break
    if not (start < pos - 1.0 and end > pos + 1.0):             # snapping must never lose the moment itself
        return max(lo, min(start, pos - 3.0)), max(end, pos + 3.0)
    return max(lo, start), end


def refine_window(utts: list, wide: tuple, keep_from, keep_to, pos: float, min_len: float = 10.0, max_len: float = 75.0):
    """Apply the AI's keep_from / keep_to (seconds from the start of `wide`) after sanity checks, then
    re-snap to sentence edges. Returns (start, end) or None if the suggestion is unusable."""
    try:
        kf, kt = float(keep_from), float(keep_to)
    except (TypeError, ValueError):
        return None
    ws, we = wide
    ns, ne = ws + max(0.0, kf), min(we, ws + kt)
    if ne - ns < min_len or not (ns < pos - 1.0 and ne > pos + 1.0):
        return None
    ns, ne = sentence_bounds(utts, ns, ne, pos, ws, we, max_len)
    return (ns, ne) if (ne - ns >= min_len and ns < pos < ne) else None


def slice_result(result: dict, offset: float, start: float, end: float) -> dict:
    """Captions for [start, end] of the recording out of a transcript made for a window starting at `offset`."""
    shift, length = offset - start, end - start
    segs = []
    for sg in (result or {}).get("segments", []):
        words = [{"word": w["word"], "start": w["start"] + shift, "end": w["end"] + shift}
                 for w in (sg.get("words") or []) if w["start"] + shift >= -0.05 and w["end"] + shift <= length + 0.3]
        if words:
            segs.append({"start": words[0]["start"], "end": words[-1]["end"], "no_speech_prob": sg.get("no_speech_prob", 0),
                         "text": "".join(w["word"] for w in words), "words": words})
        elif not sg.get("words") and sg["start"] + shift >= -0.05 and sg["end"] + shift <= length + 0.3:
            segs.append(dict(sg, start=sg["start"] + shift, end=sg["end"] + shift))
    return {"segments": segs, "text": "".join(x["text"] for x in segs)}


def transcript_lines(utts: list, start: float, end: float) -> str:
    """'[12s] sentence' lines for the AI, with times counted from the start of the clip window."""
    return "\n".join(f"[{max(0.0, s0 - start):.0f}s] {t}" for s0, e0, t in utts if e0 >= start and s0 <= end)[:3500]


def compute_virality_metrics(score: float, ev: dict, ai_rating: float | None = None,
                             said: str = "", length_s: float = 0.0) -> dict:
    """
    Computes a comprehensive commercial virality assessment (0-100 score, hook, pacing).
    """
    raw_v = 48.0 + (score * 12.5)
    if ai_rating is not None and ai_rating > 0:
        raw_v = (raw_v * 0.45) + (float(ai_rating) * 10.0 * 0.55)
    virality_score = int(round(min(98.0, max(45.0, raw_v))))

    audio_str = ev.get("audio", {}).get("strength", 1.0)
    speech_str = ev.get("speech", {}).get("strength", 0.0)
    hook_val = 5.2 + (audio_str * 1.5) + (speech_str * 1.8)
    if ai_rating is not None:
        hook_val = (hook_val * 0.5) + (float(ai_rating) * 0.5)
    hook_strength = round(min(9.9, max(5.0, hook_val)), 1)

    motion_str = ev.get("motion", {}).get("strength", 1.0)
    w_count = len(said.split()) if said else int(length_s * 2.2)
    wps = w_count / max(3.0, length_s)
    pacing_val = 5.5 + (motion_str * 1.6) + min(2.5, wps * 0.7)
    pacing_score = round(min(9.8, max(5.5, pacing_val)), 1)

    if virality_score >= 90:
        tier = "Top 5% (Viral Outlier)"
    elif virality_score >= 80:
        tier = "Top 15% (Strong Performer)"
    elif virality_score >= 70:
        tier = "Top 30% (Solid Clip)"
    else:
        tier = "Standard"

    signals = []
    if speech_str >= 1.0:
        signals.append("high-energy vocal reaction")
    if audio_str >= 1.2:
        signals.append("loudness climax spike")
    if motion_str >= 1.2:
        signals.append("dynamic on-screen action")
    if not signals:
        signals.append("standalone narrative moment")
    reason = f"Fast curiosity hook with {', '.join(signals)}. High probability of complete watch-through on Shorts & Reels."

    return {
        "virality_score": virality_score,
        "hook_strength": hook_strength,
        "pacing_score": pacing_score,
        "retention_tier": tier,
        "virality_reason": reason,
    }


class ClipMaker:
    def __init__(self, a, stop: threading.Event, transcriber: Transcriber | None, speech=None, titles=None):
        self.a, self.stop, self.transcriber, self.speech, self.titles = a, stop, transcriber, speech, titles
        self.jsonl_lock = threading.Lock()
        self.span_lock = threading.Lock()
        self.spans: list = []
        self.limit = 0                      # video mode: stop after this many clips
        self.made = 0
        self.warned_probe = False
        self.results: list[tuple] = []     # (score, confidence, file name) for the end-of-run summary

    def make(self, m: Moment) -> None:
        try:
            self._make(m)
        except Exception as e:  # noqa: BLE001
            log(f"clip: unexpected error: {e}")
        finally:
            m.rec.release()

    def _locate(self, spike: Spike, rec: Recording):
        """
        Where in the recording is the moment that caused this spike?

        Chat reacts a few seconds after the event, and the file's end sits at (roughly)
        the same point viewers are watching, so:
            position = file_duration_now - (now - spike_time) - lag
        Constant stream delay cancels out; --lag absorbs typing/reaction time.
        """
        dur = probe_duration(rec.path)
        now = time.time()
        if spike.pos is not None:            # audio/motion events already know their position in the file
            return spike.pos, (dur if dur is not None else now - rec.t0)
        moment = spike.peak_ts - self.a.lag
        if dur is None:
            if not self.warned_probe:
                log("clip: ffprobe couldn't read the live file; falling back to wall-clock timing")
                self.warned_probe = True
            return (moment - rec.t0), (now - rec.t0)
        return dur - (now - moment), dur

    def _make(self, m: Moment) -> None:
        a, rec = self.a, m.rec
        pos = max(0.0, m.ref_pos())
        extra = 0.0 if a.no_snap else SNAP_FWD
        wait_post = a.max_post if a.smart_length else a.post
        deadline = time.time() + wait_post + extra + 120
        while True:  # wait until the "after" footage actually exists on disk
            dur = probe_duration(rec.path)
            have = dur if dur is not None else time.time() - rec.t0
            if have >= pos + wait_post + extra + 0.5:
                break
            if self.stop.is_set() or rec.finished or time.time() > deadline:
                break
            self.stop.wait(1.0)

        ctx_lo = max(0.0, pos - a.max_pre)
        start = max(0.0, pos - a.pre)
        post = a.post
        if a.smart_length:                                # stay with the action until it dies down
            lo = max(0.0, pos - 30)
            lv = audio_levels(rec.path, lo, (pos - lo) + min(a.max_post, max(0.0, have - pos)))
            if lv:
                post = reaction_end(lv, 0.5, lo, pos, a.min_post, a.max_post)
                log(f"      clip length: the action lasted ~{post:.0f}s after the moment")
        end = min(pos + post, have)
        clip_max_len = a.max_len

        # Short video copyright & fair-use rule:
        # Never take 100% of a short video (avoids Content ID duplicate video flags / copyright matches).
        # For videos under 90s, extract a punchy highlight capped at ~25-30s.
        if have <= 90.0:
            target_dur = min(30.0, max(18.0, have * 0.60))
            clip_max_len = min(clip_max_len, 32.0)
            start = max(0.0, pos - (target_dur * 0.45))
            end = min(have, start + target_dur)
            if end >= have:
                start = max(0.0, have - target_dur)
                end = have
            elif start == 0.0:
                end = min(have, target_dur)

        # What was said? One speech-to-text pass over the whole area around the moment gives us the sentences
        # (so we never cut mid-sentence), the speech check, the captions and the AI's transcript.
        w0, w1 = max(0.0, ctx_lo - 4.0), min(have, end + 6.0)
        result = self.transcriber.transcribe(rec.path, w0, w1 - w0) if self.transcriber else None
        utts = utterances(abs_words(result, w0))
        if result is not None and a.speech:
            near = " ".join(t for s0, e0, t in utts if e0 >= pos - 10 and s0 <= pos + 4)
            m.speech_text = near
            sc_, hits = text_score(near, [w for w in a.hype_words.split(",")] if a.hype_words else [])
            if hits:
                m.ev["speech"] = {"strength": sc_, "pos": pos,
                                  "detail": 'said "' + ", ".join(dict.fromkeys(hits))[:60] + '"'}
        elif result is None and self.speech is not None and a.speech:     # no captions wanted: quick separate check
            r = self.speech.score(rec.path, pos)
            if r:
                m.speech_text = r[2]
                if r[1]:
                    m.ev["speech"] = {"strength": r[0], "pos": pos,
                                      "detail": 'said "' + ", ".join(dict.fromkeys(r[1]))[:60] + '"'}

        score, conf = score_moment(m.ev)
        if CONF_RANK[conf] < CONF_RANK[a.min_conf]:
            log(f"clip: skipped a {conf.upper()} moment at {pos:.0f}s (--min-conf {a.min_conf})")
            return

        hi = min(have, end + 8.0)
        if utts:                                           # sentence-aware cut
            start, end = sentence_bounds(utts, start, end, pos, ctx_lo, hi, clip_max_len)
            wide = sentence_bounds(utts, ctx_lo, end, pos, ctx_lo, hi, clip_max_len)
        else:
            if not a.no_snap:                              # no speech found: at least don't cut inside a sound
                s2 = snap_to_pause(rec.path, start, 3.0, 3.0)
                e2 = min(snap_to_pause(rec.path, end, 1.0, SNAP_FWD), have)
                if e2 - s2 >= 3 and s2 < pos < e2:
                    start, end = s2, e2
            if end - start > clip_max_len:
                end = min(have, start + clip_max_len)
            wide = (start, end)
        length = end - start
        if length < 3:
            log("clip: not enough footage for this moment, skipped")
            return

        sources = "+".join(sorted(m.ev, key=lambda k: -m.ev[k]["strength"]))
        why = " + ".join(m.ev[k]["detail"] for k in sorted(m.ev, key=lambda k: -m.ev[k]["strength"]))

        # Title + a second opinion on whether it is worth posting + where the story really starts and ends.
        # The AI looks at the wider window and may trim it (one call).
        title, res = "", {"title": "", "candidates": [], "description": "", "hashtags": [], "rating": None, "reason": "",
                          "keep_from": None, "keep_to": None}
        if a.title:                                        # fixed text for every clip
            title = clean_title(a.title)
        elif self.titles is not None and self.titles.ready:
            sp0 = m.chat
            wlen = wide[1] - wide[0]
            res = self.titles.write(rec.path, wlen, {
                "stream_title": getattr(a, "stream_title", ""), "streamer": a.streamer, "why": why,
                "said": transcript_lines(utts, wide[0], wide[1]) or m.speech_text, "pos_rel": pos - wide[0],
                "chat": [x[:80] for x in (sp0.samples if sp0 else [])]}, start=wide[0])
            title = res["title"]
            if res["rating"] is not None and a.min_rating and res["rating"] < a.min_rating:
                log(f"clip: skipped a boring moment at {pos:.0f}s - AI rated it {res['rating']}/10 "
                    f"({res['reason'] or 'nothing happens'})")
                return
            if title:
                log(f'title: "{title}"' + (f"  (AI rating {res['rating']}/10, best of {len(res['candidates'])})"
                                           if res["rating"] is not None else ""))
            ref = refine_window(utts, wide, res["keep_from"], res["keep_to"], pos, max_len=a.max_len)
            if ref is not None:
                start, end = ref
                log(f"      AI picked the start and end of the story: {end - start:.0f}s")
            else:
                start, end = max(wide[0], start), min(wide[1], end)
            length = end - start
        why_not = self._reserve(rec.path, start, end, pos)
        if why_not == "overlap":
            log(f"clip: {pos:.0f}s is inside a clip that was just made, skipped (no half-clips)")
            return
        if why_not == "full":
            return

        # captions for exactly the final window
        cres = slice_result(result, w0, start, end) if result else None
        cues = make_cues(cres) if cres else []
        wc = make_word_cues(cres) if (cres and a.caption_style == "words") else None
        transcript = " ".join(c[2] for c in cues) or m.speech_text

        # 3) one encode with everything burned in
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        base = f"{a.channel}_{stamp}_{conf.upper()}_{sources}"
        dst = a.clip_dir / f"{base}.mp4"
        ass = None
        use_caps = bool(cues) and a.burn_subs and not a.no_subs
        if title or use_caps:
            ass = a.clip_dir / f"{base}.ass"
            ass.write_text(make_ass(cues if use_caps else [], title, a.layout, a.out_height, a.cam_share,
                                    length, a.layout == "split", wc if use_caps else None, a.style,
                                    hook_banner=True), encoding="utf-8")
        if not getattr(a, "_res_checked", False):
            a._res_checked = True
            try:
                r_ = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                                     "stream=height", "-of", "csv=p=0", str(rec.path)], capture_output=True, text=True)
                hh = int(next(x for x in r_.stdout.split() if x.strip()).strip().split(",")[0])
                log(f"source video is {hh}p" + ("" if hh >= 1080 else
                    " - lower than 1080p, so vertical clips will look soft; use --quality best (or a 1080p stream)"))
            except (StopIteration, ValueError, OSError):
                pass
        if a.layout in ("podcast", "podcast-split"):
            try:
                import viral_engine
                out_w = int((a.out_height or 1920) * 9 / 16) // 2 * 2
                mfc = viral_engine.MultiFaceClusterer(target_w=out_w, target_h=a.out_height or 1920)
                x1, x2, _ = mfc.detect_speakers(rec.path, start, length)
                split = {"speaker1": x1, "speaker2": x2}
            except Exception:
                split = {"speaker1": 0.25, "speaker2": 0.75}
        elif a.layout == "split":
            resolve_cam(a, rec.path, start, length)
            split = split_cfg(a)
        else:
            split = None
        t_enc = time.time()
        ok, err = cut_vertical(rec.path, start, length, dst, a.layout, a.out_height, a.crf, a.preset,
                               split, ass, a.av_offset, not a.no_normalize)
        if not ok and ass is not None:
            log(f"clip: text overlay failed ({err}); making the clip without text")
            ok, err = cut_vertical(rec.path, start, length, dst, a.layout, a.out_height, a.crf, a.preset,
                                   split, None, a.av_offset, not a.no_normalize)
        if ass is not None:
            ass.unlink(missing_ok=True)
        if not ok:
            log(f"clip: ffmpeg failed: {err}")
            return
        if a.keep_wide:
            cut_wide(rec.path, start, length, a.clip_dir / f"{base}_wide.mp4")
        run_ffmpeg(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", f"{max(0.5, min(pos - start, length - 0.5)):.2f}",
                    "-i", str(dst), "-frames:v", "1", "-q:v", "3", str(info_dir(dst) / f"{base}.jpg")])   # thumbnail = the peak
        if cues and not a.no_subs:
            write_srt(cues, info_dir(dst) / f"{base}.srt")
        save_post(dst, res if res["title"] else dict(res, title=title), transcript)

        sync_target = getattr(a, "sync_dir", None)
        if sync_target:
            try:
                sync_p = Path(sync_target)
                sync_p.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(dst), str(sync_p / dst.name))
                thumb_f = info_dir(dst) / f"{base}.jpg"
                if thumb_f.exists():
                    shutil.copy2(str(thumb_f), str(sync_p / f"{base}.jpg"))
                srt_f = info_dir(dst) / f"{base}.srt"
                if srt_f.exists():
                    shutil.copy2(str(srt_f), str(sync_p / f"{base}.srt"))
                post_f = info_dir(dst) / f"{base}_post.txt"
                if post_f.exists():
                    shutil.copy2(str(post_f), str(sync_p / f"{base}_post.txt"))
                log(f"drive: synced '{dst.name}' to {sync_p}")
            except Exception as e:  # noqa: BLE001
                log(f"drive: sync error ({e})")

        self.made += 1
        self.results.append((score, conf, dst.name))
        log(f"CLIP  {dst.name}  [{conf.upper()} {score:.1f}]  {why}  ({length:.0f}s, encoded in {time.time() - t_enc:.0f}s)")

        sp = m.chat
        v_metrics = compute_virality_metrics(score, m.ev, res.get("rating"), transcript, length)
        meta = {
            "time": datetime.now().isoformat(timespec="seconds"),
            "platform": a.platform, "channel": a.channel,
            "stream_title": getattr(a, "stream_title", ""), "clip": dst.name,
            "title": title, "ai_rating": res["rating"], "ai_reason": res["reason"],
            "virality_score": v_metrics["virality_score"],
            "hook_strength": v_metrics["hook_strength"],
            "pacing_score": v_metrics["pacing_score"],
            "retention_tier": v_metrics["retention_tier"],
            "virality_reason": v_metrics["virality_reason"],
            "description": res.get("description") or f"Highlight from {getattr(a, 'stream_title', '') or a.channel}. Watch until the end!",
            "hashtags": res.get("hashtags") or ["#shorts", "#trending", "#viral", "#fyp"],
            "confidence": conf, "score": round(score, 2), "sources": sorted(m.ev),
            "signals": {k: {"strength": round(v["strength"], 2), "detail": v["detail"]} for k, v in m.ev.items()},
            "said": transcript[:400],
            "messages": sp.raw if sp else 0, "counted_messages": sp.count if sp else 0,
            "baseline_median": sp.median if sp else 0, "ratio": round(sp.ratio, 2) if sp else 0,
            "chatters": sp.users if sp else 0, "hype_share": round(sp.hype, 2) if sp else 0,
            "top_terms": sp.terms if sp else [], "samples": sp.samples if sp else [],
            "recording": rec.path.name, "position_s": round(pos, 1),
            "rec_start_s": round(start, 2), "length_s": round(length, 2),
        }
        with self.jsonl_lock:
            with open(a.clip_dir / "clips.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(meta, ensure_ascii=False) + "\n")

        if a.webhook:
            notify(a.webhook, f"New {conf.upper()} clip from **{a.channel}** ({a.platform}): "
                              f"`{dst.name}` ({why})")

    def _reserve(self, path: Path, start: float, end: float, pos: float) -> str:
        """'ok', 'overlap' (inside or right after a clip already made from this recording) or 'full' (--top reached)."""
        with self.span_lock:
            if self.limit and len(self.spans) >= self.limit:
                return "full"
            for p, s0, e0 in self.spans:
                if p == path and s0 - 3 <= pos <= e0 + 6:
                    return "overlap"
            self.spans.append((path, start, end))
            return "ok"


def notify(url: str, text: str) -> None:
    """Discord-style webhook ({"content": ...}); failures never break the run."""
    try:
        req = urllib.request.Request(
            url, data=json.dumps({"content": text}).encode(),
            headers={"Content-Type": "application/json", "User-Agent": "clip-watcher/1.0"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:  # noqa: BLE001
        log(f"webhook failed: {e}")


# --------------------------------------------------------------------------- #
# self-test (no network, no ffmpeg needed)
# --------------------------------------------------------------------------- #
def selftest() -> int:
    rnd = random.Random(7)
    base_t = 1_000_000.0

    def run(events, **kw):
        det = SpikeDetector(**kw)
        found = []
        for ts, u, txt in sorted(events):
            found += det.tick(ts)
            det.add(ts, u, txt)
        found += det.tick(events[-1][0] + 60)
        return found

    def steady(t0, t1, per_10s, users=40):
        n = int((t1 - t0) / 10 * per_10s)
        step = (t1 - t0) / n
        return [(t0 + i * step + rnd.random() * step * 0.3, f"u{rnd.randrange(users)}", "hello chat")
                for i in range(n)]

    def burst(t0, dur, n, users=30, txt="LUL LUL Pog"):
        return [(t0 + rnd.random() * dur, f"b{rnd.randrange(users)}", txt) for _ in range(n)]

    checks = []
    f = run(steady(base_t, base_t + 600, 5) + burst(base_t + 400, 8, 70), min_msgs=8)
    checks.append(("one burst -> one spike", len(f) == 1 and 396 <= f[0].peak_ts - base_t <= 412))
    f = run(steady(base_t, base_t + 600, 5) + burst(base_t + 400, 8, 70) + burst(base_t + 418, 6, 60), min_msgs=8)
    checks.append(("cooldown merges follow-up burst", len(f) == 1))
    f = run(steady(base_t, base_t + 600, 5) + [(base_t + 400 + i * .05, "spammer", "AAAA") for i in range(100)])
    checks.append(("single-user spam ignored", len(f) == 0))
    f = run(steady(base_t, base_t + 600, 1) + burst(base_t + 400, 8, 6), min_msgs=8)
    checks.append(("quiet chat: 6 msgs below --min", len(f) == 0))
    f = run(steady(base_t, base_t + 600, 1) + burst(base_t + 400, 8, 14), min_msgs=8)
    checks.append(("quiet chat: 14 msgs above --min", len(f) == 1))
    f = run(steady(base_t, base_t + 600, 20) + burst(base_t + 400, 8, 25), min_msgs=8)
    checks.append(("busy chat: 25 msgs is not 2.5x of 20", len(f) == 0))
    f = run(burst(base_t + 10, 8, 70) + steady(base_t, base_t + 300, 5), min_msgs=8)
    checks.append(("no clips during warm-up", len(f) == 0))
    f = run(steady(base_t, base_t + 600, 5) + burst(base_t + 400, 8, 70), min_msgs=8)
    checks.append(("emote terms reported", bool(f) and f[0].terms and f[0].terms[0][0] in ("LUL", "Pog")))
    checks.append(("srt timestamp", srt_time(3661.5) == "01:01:01,500"))
    checks.append(("crop filter is 9:16", "crop=trunc(ih*9/16/2)*2:ih" in video_filter_args("crop", 1920)[1]))
    words = [{"word": " Hello", "start": 0.0, "end": 0.4}, {"word": " there.", "start": 0.4, "end": 0.9},
             {"word": " No", "start": 0.95, "end": 1.1}, {"word": " way", "start": 1.1, "end": 1.4},
             {"word": " dude", "start": 1.4, "end": 1.8}, {"word": " that", "start": 1.8, "end": 2.0}]
    cues = make_cues({"segments": [{"start": 0, "end": 2, "text": "", "words": words}]})
    checks.append(("caption grouping", len(cues) >= 1 and cues[0][2].startswith("Hello")))

    T = parse_target
    vid = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    checks.append(("twitch name/url", T("Shroud")["label"] == "shroud"
                   and T("https://www.twitch.tv/Shroud/videos")["label"] == "shroud"))
    checks.append(("youtube watch/youtu.be/live URLs", all(
        T(u)["stream_url"] == vid for u in (
            vid + "&t=5", "https://youtu.be/dQw4w9WgXcQ?si=x", "youtube.com/watch?v=dQw4w9WgXcQ",
            "https://www.youtube.com/live/dQw4w9WgXcQ?feature=share"))))
    h = T("@LofiGirl")
    checks.append(("youtube @handle -> /live", h["platform"] == "youtube"
                   and h["stream_url"] == "https://www.youtube.com/@LofiGirl/live"
                   and h["chat_url"] == "https://www.youtube.com/@LofiGirl" and h["label"] == "yt_LofiGirl"))
    checks.append(("youtube channel id + bare id", T("https://www.youtube.com/channel/UCabc_DEF-123/live")[
        "stream_url"].endswith("/channel/UCabc_DEF-123/live") and T("dQw4w9WgXcQ", "youtube")["stream_url"] == vid))
    checks.append(("video id never starts with '-'", not T("https://youtu.be/-abcdefghij")["label"].startswith("-")))
    try:
        T("https://www.youtube.com/feed/trending")
        checks.append(("bad youtube link rejected", False))
    except ValueError:
        checks.append(("bad youtube link rejected", True))

    # audio/motion rise detector
    def rise(series, **kw):
        d, evs = RiseDetector(**kw), []
        for i, v in enumerate(series):
            e = d.feed((i + 1) * kw["step"], v)
            if e:
                evs.append(e)
        return evs

    quiet = [-30 + rnd.uniform(-3, 3) for _ in range(120)]
    shout = quiet[:80] + [-14, -12, -13, -20] + quiet[84:]
    ev = rise(shout, step=1.0, warmup=30, margin=9.0, mode="add", floor=-45.0)
    checks.append(("audio: one shout -> one event at the peak", len(ev) == 1 and 80 <= ev[0][0] <= 84))
    checks.append(("audio: steady speech -> no event", not rise(quiet, step=1.0, warmup=30, margin=9.0, mode="add")))
    ev = rise(quiet[:50] + [-12] * 3 + quiet[53:70] + [-12] * 3 + quiet[73:], step=1.0, warmup=30, cooldown=30,
              margin=9.0, mode="add")
    checks.append(("audio: cooldown blocks a repeat 20s later", len(ev) == 1))
    calm = [2.0 + rnd.uniform(-0.5, 0.5) for _ in range(240)]
    fight = calm[:150] + [14.0] * 8 + calm[158:]
    ev = rise(fight, step=0.5, warmup=30, margin=2.5, mode="mul", minimum=6.0)
    checks.append(("motion: sudden action -> one event", len(ev) == 1 and 75 <= ev[0][0] <= 80))
    checks.append(("motion: calm gameplay -> no event", not rise(calm, step=0.5, warmup=30, margin=2.5, mode="mul", minimum=6.0)))
    checks.append(("no event during warm-up", not rise([-30] * 5 + [-10] * 5, step=1.0, warmup=30, margin=9.0, mode="add")))

    # chat content, speech, scoring, fusion
    hf, ask = chat_hype(["W W W", "KEKW", "clip it", "clip that!!", "hello", "what song is this", "💀💀"])
    checks.append(("chat hype: 5 of 7 are hype, 2 ask for a clip", abs(hf - 5 / 7) < 0.01 and ask == 2))
    checks.append(("chat hype: normal chat = 0", chat_hype(["hello", "nice car", "what game is this"]) == (0.0, 0)))
    st, hits = text_score("No way! Bro, let's go, oh my god")
    checks.append(("speech: reaction words found", st >= 1.5 and "no way" in hits and "bro" in hits))
    checks.append(("speech: normal talking = 0", text_score("so today we are driving to the shop")[0] == 0.0))
    checks.append(("speech: lone filler word ('bro') = 0", text_score("bro that is a nice car")[0] == 0.0))
    checks.append(("speech: reaction beats filler",
                   text_score("no way")[0] > text_score("bro dude")[0] > 0))
    lv = [-30.0] * 40 + [-80.0] * 8 + [-30.0] * 40 + [-80.0] * 2 + [-30.0] * 10
    ps = find_pauses(lv, 0.05)
    checks.append(("pauses: finds the 0.4 s gap, ignores the 0.1 s one", len(ps) == 1 and abs(ps[0] - 2.2) < 0.06))
    checks.append(("pauses: steady loudness -> none", find_pauses([-30.0 + (i % 3) for i in range(100)], 0.05) == []))
    checks.append(("speech: custom words", text_score("that was sus", ["sus"])[0] > 0))
    checks.append(("score: 1 weak signal = low",
                   score_moment({"motion": {"strength": 1.0}})[1] == "low"))
    checks.append(("score: 1 strong signal = med",
                   score_moment({"audio": {"strength": 2.0}})[1] == "med"))
    checks.append(("score: 2 signals = med+, 3 = high",
                   score_moment({"chat": {"strength": 1.0}, "audio": {"strength": 1.0}})[1] == "med"
                   and score_moment({"chat": {"strength": 1.0}, "audio": {"strength": 1.0},
                                     "speech": {"strength": 1.0}})[1] == "high"))
    fu, rec0 = Fusion(window=15, wait=20, cooldown=30), object()
    r1 = fu.add(rec0, "audio", 100.0, 1.5, "loud", now=1000)
    r2 = fu.add(rec0, "chat", 108.0, 1.2, "chat", now=1012)
    r3 = fu.add(rec0, "motion", 300.0, 1.0, "action", now=1013)
    checks.append(("fusion: nearby signals share one moment", (r1, r2, r3) == ("new", "joined", "new")))
    checks.append(("fusion: waits for slow signals", fu.due(now=1015) == []))
    due = fu.due(now=1025)
    checks.append(("fusion: first moment due after wait, has 2 sources",
                   len(due) == 1 and sorted(due[0].ev) == ["audio", "chat"] and abs(due[0].ref_pos() - 100) < 1))
    checks.append(("fusion: late signal for a clipped moment is a duplicate",
                   fu.add(rec0, "chat", 112.0, 1.0, "late", now=1030) == "dup"))

    sp = video_filter_args("split", 1920, {"cam": (0.0, 0.43, 0.2, 0.205), "share": 0.36, "game_x": 0.5})
    checks.append(("split layout: stacks cam over game", "vstack" in sp[1] and "scale=1080:690" in sp[1]))
    checks.append(("split layout: cam share changes panel heights", "scale=1080:576" in
                   video_filter_args("split", 1920, {"cam": (0, 0.4, 0.2, 0.2), "share": 0.30, "game_x": 0.5})[1]))
    pod = video_filter_args("podcast", 1920)
    checks.append(("podcast layout: stacks dual speakers 9:16", "vstack" in pod[1] and "scale=1080:960" in pod[1]))
    cf = cam_from_faces([(84, 273, 34, 34)] * 6 + [(585, 287, 111, 111)] * 6, 960, 540)
    checks.append(("auto cam: edge face beats a centred one", bool(cf) and cf[0][0] < 0.2 and cf[1] == 6))
    checks.append(("auto cam: one-off detection ignored", cam_from_faces([(10, 10, 30, 30)], 960, 540) is None))
    checks.append(("auto cam: box stays inside frame", bool(cf) and cf[0][0] >= 0 and cf[0][0] + cf[0][2] <= 1.001
                   and cf[0][1] >= 0 and cf[0][1] + cf[0][3] <= 1.001))
    lv = [-30.0] * 40 + [-12.0] * 12 + [-30.0] * 40            # calm, 6 s of shouting from 20 s, calm
    e1 = reaction_end(lv, 0.5, 0.0, 20.0, 4.0, 20.0)
    checks.append(("smart length: ends after the shouting stops", 6.0 <= e1 <= 11.0))
    checks.append(("smart length: never below the minimum", reaction_end([-30.0] * 90, 0.5, 0.0, 20.0, 6.0, 20.0) == 6.0))
    checks.append(("smart length: keeps going up to the maximum", reaction_end([-30.0] * 40 + [-12.0] * 50, 0.5, 0.0, 20.0, 4.0, 12.0) == 12.0))
    pr = parse_title_reply('Here you go ```json {"candidates": ["Speed robs a bank", "He forgot the mask 💀", "x"], "best": 1, '
                           '"description": "Chaos at the vault.", "hashtags": ["gta", "#funny", "bad tag!"]} ```')
    checks.append(("title reply: JSON, best pick, hashtags", pr["title"] == "HE FORGOT THE MASK" and len(pr["candidates"]) == 3
                   and pr["hashtags"] == ["#gta", "#funny", "#badtag"] and pr["description"] == "Chaos at the vault."))
    checks.append(("title reply: plain text still works", parse_title_reply('"Wait for it."')["title"] == "WAIT FOR IT"))
    checks.append(("title reply: out-of-range best falls back", parse_title_reply('{"candidates": ["A B C D"], "best": 7}')["title"] == "A B C D"))
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import review
        with tempfile.TemporaryDirectory() as td:
            cd = Path(td)
            nm = "ch_20260101-000000_HIGH_audio+chat.mp4"
            (cd / nm).write_bytes(b"x")
            (cd / nm.replace(".mp4", "_final.mp4")).write_bytes(b"y")
            (cd / "clips.jsonl").write_text(json.dumps({"clip": nm, "score": 3.1, "confidence": "high",
                                                        "sources": ["audio", "chat"], "title": ""}) + "\n")
            rows = review.load_clips(cd)
            ok1 = len(rows) == 1 and rows[0]["video"].endswith("_final.mp4")
            ok2 = review.decide(cd, nm, "delete") and not (cd / nm).exists() and (cd / "_deleted" / nm).exists()
            ok3 = review.decide(cd, nm, "undo") and (cd / nm).exists()
            ok4 = not review.decide(cd, "../evil.mp4", "delete")
            st = {"audio": {"keep": 1, "delete": 6}}
            ok5 = bool(review.advice(st))
        checks.append(("review page: load, delete, restore, safe names, advice", ok1 and ok2 and ok3 and ok4 and ok5))
    except Exception as e:  # noqa: BLE001
        checks.append((f"review page ({e})", False))
    fc = with_subtitles(video_filter_args("split", 1920, None), "x.ass")
    checks.append(("one-pass overlay: split", fc[0] == "-filter_complex" and fc[1].endswith("[v0];[v0]subtitles=x.ass[v]")))
    vf = with_subtitles(video_filter_args("crop", 1920), "x.ass")
    checks.append(("one-pass overlay: crop", vf[0] == "-vf" and vf[1].endswith(",subtitles=x.ass")))
    checks.append(("lip-sync filter", audio_filter(0) == "aresample=async=1:first_pts=0" and "loudnorm" in audio_filter(0, True)
                   and audio_filter(-300).startswith("atrim=start=0.300") and audio_filter(250).startswith("adelay=250")))
    pr2 = parse_title_reply('{"rating": 3, "reason": "menu screen", "candidates": ["A B C D"], "best": 0}')
    checks.append(("title reply: AI rating", pr2["rating"] == 3 and pr2["reason"] == "menu screen"))
    mk_ = ClipMaker.__new__(ClipMaker)
    mk_.span_lock, mk_.spans = threading.Lock(), []
    pth = Path("a.ts")
    mk_.limit = 0
    checks.append(("no half-clips: overlapping moment skipped", mk_._reserve(pth, 10, 50, 30) == "ok"
                   and mk_._reserve(pth, 40, 80, 52) == "overlap" and mk_._reserve(pth, 70, 90, 80) == "ok"))
    mk_.limit = 2
    checks.append(("--top: stops at the limit", mk_._reserve(pth, 200, 230, 210) == "full"))
    fake = types.ModuleType("faster_whisper")

    class _W:
        def __init__(self, *a, **k):
            pass

        def transcribe(self, path, **k):
            w = types.SimpleNamespace(word=" hi", start=0.1, end=0.4)
            return iter([types.SimpleNamespace(start=0.0, end=0.5, text=" hi", no_speech_prob=0.01, words=[w])]), None
    fake.WhisperModel = _W
    sys.modules["faster_whisper"] = fake
    try:
        fake.__spec__ = importlib.machinery.ModuleSpec("faster_whisper", None)
        ar = Asr("tiny").transcribe(Path("x.wav"))
        checks.append(("speech-to-text: faster-whisper result converted",
                       ar["text"].strip() == "hi" and ar["segments"][0]["words"][0]["word"] == " hi"))
    finally:
        sys.modules.pop("faster_whisper", None)
    try:
        import webui
        sf = Path("stop")
        a1, _ = webui.build_args({"url": "shroud", "profile": "gaming", "min_rating": "6", "extra": "--av-offset -300"}, sf)
        bad5 = webui.build_args({"url": "x", "style": "evil"}, sf)[0] is None
        a2, _ = webui.build_args({"url": "x", "style": "neon"}, sf)
        a3, _ = webui.build_args({"source": "vod", "url": "https://youtu.be/x", "top": "5"}, sf)
        bad1 = webui.build_args({"url": "--evil"}, sf)[0] is None
        bad2 = webui.build_args({"url": "x", "extra": "--ui"}, sf)[0] is None
        bad3 = webui.build_args({"url": "x", "min_rating": "99"}, sf)[0] is None
        with tempfile.TemporaryDirectory() as td:
            hd = Path(td)
            (hd / ".env").write_text("OTHER=1\nGEMINI_API_KEY=old\n")
            webui.save_key(hd, "gemini", "newkey-1")
            env_txt = (hd / ".env").read_text()
            webui.save_key(hd, "gemini", "")
            env_txt2 = (hd / ".env").read_text()
            bad4 = not webui.save_key(hd, "gemini", "bad key; rm -rf")
        os.environ.pop("GEMINI_API_KEY", None)
        checks.append(("web interface: command built safely", a1[:5] == ["shroud", "--profile", "gaming", "--min-rating", "6"]
                       and a1[-2:] == ["--stop-file", "stop"] and bad1 and bad2 and bad3 and bad5
                       and "--style" in a2 and a3[:2] == ["--vod", "https://youtu.be/x"] and "--top" in a3))
        checks.append(("web interface: keys saved to .env, others kept", "OTHER=1" in env_txt and "GEMINI_API_KEY=newkey-1" in env_txt
                       and "old" not in env_txt and "GEMINI" not in env_txt2 and "OTHER=1" in env_txt2 and bad4))
    except Exception as e:  # noqa: BLE001
        checks.append((f"web interface ({e})", False))
    def _w(t0, txt, d=0.35):
        o, t = [], t0
        for x in txt.split():
            o.append({"word": " " + x, "start": t, "end": t + d})
            t += d + 0.05
        return o
    ws_ = (_w(40, "So yesterday I walked into the bank right.") + _w(44, "And the guy asks for my ID and I gave him my library card.")
           + _w(52, "Then he says sir this is a library card.") + _w(58, "And I said exactly it is overdue!") + _w(63, "Hahaha no way"))
    ut_ = utterances(ws_)
    sb_ = sentence_bounds(ut_, 47.0, 68.0, 62.0, 32.0, 76.0)
    checks.append(("story: sentences found", len(ut_) == 5))
    checks.append(("story: start moves to the sentence start, not mid-sentence", abs(sb_[0] - 43.75) < 0.01 and sb_[1] == 68.0))
    checks.append(("story: end finishes the sentence", sentence_bounds(ut_, 40.0, 54.0, 50.0, 30.0, 80.0)[1] >= 55.5))
    checks.append(("story: AI trim accepted and snapped", refine_window(ut_, (32.0, 70.0), 13.0, 38.0, 62.0) == (43.75, 70.0)))
    checks.append(("story: AI trim that loses the moment is ignored",
                   refine_window(ut_, (32.0, 70.0), 0, 10, 62.0) is None and refine_window(ut_, (32.0, 70.0), 28.0, 32.0, 62.0) is None))
    sl_ = slice_result({"segments": [{"no_speech_prob": 0.1, "start": 40, "end": 66, "text": "x", "words": ws_}]}, 0.0, 44.0, 66.0)
    checks.append(("story: captions re-timed to the clip", sl_["segments"][0]["words"][0]["word"] == " And"
                   and abs(sl_["segments"][0]["words"][0]["start"]) < 0.01))
    lw_ = learned_weights({"chat": {"keep": 1, "delete": 11}, "audio": {"keep": 10, "delete": 2}, "motion": {"keep": 2, "delete": 1}},
                          {"chat": 1.0, "audio": 1.0, "motion": 0.7})
    checks.append(("learning: weights follow your choices (needs 8+ judged)", lw_["chat"] < 0.85 and lw_["audio"] > 1.15 and lw_["motion"] == 0.7))
    checks.append(("video range parsing", parse_range("1:00:00-2:30:00", 36000.0) == (3600.0, 5400.0)
                   and parse_range("", 500.0) == (0.0, 500.0) and parse_range("10-", 100.0) == (10.0, 90.0)
                   and parse_clock("45:10") == 2710.0))
    try:
        parse_range("0-5", 100.0)
        checks.append(("video range: too short rejected", False))
    except ValueError:
        checks.append(("video range: too short rejected", True))
    checks.append(("--cam parsing", parse_cam("0, 0.43, 0.2, 0.205") == (0.0, 0.43, 0.2, 0.205)))
    try:
        parse_cam("0.9,0.9,0.5,0.5")
        checks.append(("--cam out of frame rejected", False))
    except ValueError:
        checks.append(("--cam out of frame rejected", True))
    ass = make_ass([(0.5, 2.0, "no way bro")], "speed robs a bank", "split", 1920, 0.36, 20.0, True)
    checks.append(("title: on the seam, whole clip", "\\pos(540,691)" in ass and "0:00:20.00,Title" in ass))
    checks.append(("captions: capitals, own style", "Cap,,0,0,0,,NO WAY BRO" in ass))
    checks.append(("title clean-up", clean_title('Title: "Speed robs a bank 💀 #gta."') == "SPEED ROBS A BANK"))
    gi = title_request("gemini", "m", "K", "P", ["AAA"])
    ci = title_request("claude", "m", "K", "P", ["AAA"])
    checks.append(("title request: gemini + claude shapes",
                   gi[0].endswith("/models/m:generateContent") and gi[1]["x-goog-api-key"] == "K"
                   and gi[2]["contents"][0]["parts"][1]["inline_data"]["data"] == "AAA"
                   and ci[0].endswith("/v1/messages") and ci[1]["x-api-key"] == "K"
                   and ci[2]["messages"][0]["content"][0]["source"]["data"] == "AAA"))
    sw = parse_sweep_reply('x {"moments":[{"at":70,"score":9,"why":"hulk"},{"at":999,"score":9},{"at":5,"score":"a"}]}', 0, 120)
    checks.append(("AI sweep reply parsing", len(sw) == 1 and sw[0][0] == 70 and sw[0][1] == 9))
    checks.append(("AI moments count as evidence", score_moment({"ai": {"strength": 2.0}})[1] == "med"))
    checks.append(("title response parsing",
                   title_response("gemini", {"candidates": [{"content": {"parts": [{"text": "A"}, {"text": "B"}]}}]}) == "AB"
                   and title_response("claude", {"content": [{"type": "text", "text": "HI"}]}) == "HI"))

    bad = 0
    for name, ok in checks:
        print(("  ok   " if ok else "  FAIL ") + name)
        bad += not ok
    print("selftest passed" if not bad else f"selftest: {bad} failure(s)")
    return 1 if bad else 0


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
YT_ID = re.compile(r"[A-Za-z0-9_-]{11}")


def resolve_playlist_or_channel(url: str, max_items: int = 25) -> list[str]:
    """
    Expands a YouTube playlist or channel URL into a list of individual watch URLs.
    If the URL is a regular single video or stream, returns [url].
    """
    u_low = url.lower()
    is_playlist = "playlist?list=" in u_low or "&list=" in u_low
    is_channel_vids = ("youtube.com/@" in u_low or "youtube.com/channel/" in u_low) and ("/videos" in u_low or not "/watch" in u_low)
    if not (is_playlist or is_channel_vids):
        return [url]

    ytdlp_bin = shutil.which("yt-dlp")
    if not ytdlp_bin:
        for c in [Path(__file__).resolve().parent / ".venv" / "Scripts" / "yt-dlp.exe",
                  Path(__file__).resolve().parent / ".venv" / "bin" / "yt-dlp"]:
            if c.exists():
                ytdlp_bin = str(c)
                break
    if not ytdlp_bin:
        return [url]

    try:
        cmd = [
            ytdlp_bin, "--flat-playlist", "--dump-json",
            "--playlist-end", str(max_items),
            url
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        vids = []
        for line in r.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                vid_id = data.get("id") or data.get("url")
                if vid_id and len(vid_id) == 11:
                    vids.append(f"https://www.youtube.com/watch?v={vid_id}")
                elif data.get("url") and "watch?v=" in data.get("url"):
                    vids.append(data["url"])
            except Exception:
                continue
        if vids:
            return vids
    except Exception as e:
        log(f"playlist: failed to expand {url}: {e}")
    return [url]


def parse_target(raw: str, platform: str = "auto") -> dict:
    """
    Turn whatever the user typed into platform, a safe file label, the URL streamlink should
    record, and the URL the chat reader should use.
    """
    raw = raw.strip()
    low = raw.lower()
    if platform == "auto":
        yt = "youtube.com" in low or "youtu.be" in low or raw.startswith("@")
        platform = "youtube" if yt else "twitch"

    if platform == "twitch":
        name = re.sub(r"^.*twitch\.tv/", "", raw, flags=re.I).strip("/@# ").lower()
        name = name.split("/")[0].split("?")[0]
        if not re.fullmatch(r"[a-z0-9_]{1,25}", name):
            raise ValueError(f"'{raw}' doesn't look like a Twitch channel name")
        return dict(platform="twitch", label=name, stream_url=f"twitch.tv/{name}",
                    chat_url=f"https://www.twitch.tv/{name}")

    # ---- YouTube
    s = raw
    if raw.startswith("@"):
        s = "https://www.youtube.com/" + raw
    elif "youtube.com" not in low and "youtu.be" not in low:
        s = ("https://www.youtube.com/watch?v=" + raw if YT_ID.fullmatch(raw)
             else "https://www.youtube.com/@" + raw)
    if "://" not in s:
        s = "https://" + s
    u = urllib.parse.urlparse(s)
    parts = [x for x in u.path.split("/") if x]
    q = urllib.parse.parse_qs(u.query)

    def video(vid: str) -> dict:
        if not YT_ID.fullmatch(vid):
            raise ValueError(f"couldn't read a YouTube video ID from '{raw}'")
        url = f"https://www.youtube.com/watch?v={vid}"
        return dict(platform="youtube", label="yt_" + vid.replace("-", "_"), stream_url=url, chat_url=url)

    def channel(kind: str, ident: str) -> dict:
        base = f"https://www.youtube.com/{kind}{ident}"
        label = "yt_" + re.sub(r"[^A-Za-z0-9_]", "_", ident)
        return dict(platform="youtube", label=label, stream_url=base + "/live", chat_url=base)

    if u.netloc.endswith("youtu.be") and parts:
        return video(parts[0])
    if parts and parts[0] == "watch":
        return video(q.get("v", [""])[0])
    if parts and parts[0] in ("live", "embed", "shorts", "v") and len(parts) > 1:
        return video(parts[1])
    if parts and parts[0].startswith("@"):
        return channel("@", parts[0][1:])
    if len(parts) >= 2 and parts[0] in ("channel", "c", "user"):
        return channel(parts[0] + "/", parts[1])
    raise ValueError(f"couldn't understand the YouTube link '{raw}'. Use the watch URL of the live "
                     f"stream, or the channel's @handle")


PROFILES = {
    "gaming": dict(layout="split", min_conf="med"),
    "just-chatting": dict(layout="crop", motion=False, audio_db=9.0, factor=2.5, min_conf="med"),
    "irl": dict(layout="crop", motion_factor=3.0, min_conf="med"),
    "highlights": dict(layout="crop", min_conf="high", cooldown=60.0),
    "cricket": dict(layout="crop", audio_db=8.0, min_conf="med", pre=12.0, post=14.0,
                    hype_words="six,out,bowled,caught,wicket,four,boundary,century,catch,unbelievable,review"),
    "sports": dict(layout="crop", audio_db=8.0, min_conf="med", pre=10.0, post=14.0,
                   hype_words="goal,six,out,wicket,touchdown,save,penalty,unbelievable,insane"),
    "podcast": dict(layout="podcast", min_conf="med", audio_db=4.0, pre=14.0, post=16.0),
}


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="watch.sh", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Record a live Twitch/YouTube stream, detect chat spikes, cut 9:16 clips, add subtitles.")
    p.add_argument("--mode", choices=["auto", "spike", "viral"], default="auto",
                   help="clip engine: auto (uses viral semantic story engine if key is set, else spike), "
                        "viral (full transcript AI story & hook segmentation), spike (audio/chat/motion loudness)")
    p.add_argument("--karaoke", action="store_true", default=True,
                   help="burn in animated word-by-word active-highlight karaoke subtitles")
    p.add_argument("--no-karaoke", dest="karaoke", action="store_false",
                   help="disable word-by-word karaoke subtitles")
    p.add_argument("--min-viral", type=int, default=65,
                   help="minimum virality score (1-100) for semantic viral clipping")
    p.add_argument("--gpu", action="store_true", default=True,
                   help="use hardware-accelerated video encoding (NVENC/QSV/AMF)")
    p.add_argument("--no-gpu", dest="gpu", action="store_false",
                   help="disable GPU encoding, force CPU libx264")
    p.add_argument("channel", nargs="?",
                   help="Twitch channel (name or URL), or YouTube live: watch URL, @handle, "
                        "/channel/ID URL, or 11-char video ID with --platform youtube")
    p.add_argument("--file", default="", metavar="VIDEO", help="find the best moments in a video file on this computer")
    p.add_argument("--vod", default="", metavar="URL", help="download a past stream / video link (yt-dlp) and find its best moments")
    p.add_argument("--top", type=int, default=10, help="video / VOD mode: make this many clips (0 = every moment found)")
    p.add_argument("--scan", choices=["fast", "full", "ai"], default="fast",
                   help="video mode: fast = scan the sound (very quick), then check the picture only around the best moments; "
                        "full = also scan every frame (slow on long videos)")
    p.add_argument("--range", default="", metavar="FROM-TO", help="video mode: only look at this part, e.g. 2:00:00-5:30:00")
    p.add_argument("--fresh", action="store_true", help="video mode: scan again even if a saved scan exists")
    p.add_argument("--no-learn", action="store_true", help="don't adjust scoring from your Keep/Delete history")
    p.add_argument("--platform", choices=["auto", "twitch", "youtube"], default="auto",
                   help="auto-detected from the URL; set it for bare YouTube IDs")
    p.add_argument("--profile", choices=sorted(PROFILES),
                   help="preset for a kind of stream; any flag you add still overrides it "
                        "(gaming=split layout, just-chatting=no motion, irl=calmer motion, highlights=only the best)")
    g = p.add_argument_group("spike detection")
    g.add_argument("--min", dest="min_msgs", type=int, default=8,
                   help="minimum messages in a bucket (quiet chats: try 5)")
    g.add_argument("--factor", type=float, default=2.5, help="spike = factor x median")
    g.add_argument("--bucket", type=float, default=10, help="bucket size, seconds")
    g.add_argument("--window", type=float, default=180, help="baseline window, seconds")
    g.add_argument("--min-users", type=int, default=3, help="distinct chatters needed in the spike")
    g.add_argument("--user-cap", type=int, default=5, help="max messages counted per chatter per bucket (anti-spam)")
    g.add_argument("--sigma", type=float, default=5.0, help="also require median + sigma*sqrt(median) (noise guard)")
    g.add_argument("--warmup", type=float, default=60, help="seconds of history before clipping starts")
    g.add_argument("--cooldown", type=float, default=45, help="min seconds between clips")
    g = p.add_argument_group("audio / motion detection (reads the recording; works even when chat is quiet)")
    g.add_argument("--no-audio", dest="audio", action="store_false", help="don't clip loud moments (shouting, screams, explosions)")
    g.add_argument("--no-motion", dest="motion", action="store_false", help="don't clip sudden on-screen action (fights, scene changes)")
    g.add_argument("--no-chat", action="store_true", help="ignore chat entirely; clip only from audio/motion")
    g.add_argument("--audio-db", type=float, default=9.0, help="clip when loudness jumps this many dB above normal (lower = more clips)")
    g.add_argument("--motion-factor", type=float, default=2.5, help="clip when on-screen motion is this many times normal (lower = more clips)")
    g.add_argument("--motion-min", type=float, default=6.0, help="ignore motion below this absolute level (0-255 scale)")
    g = p.add_argument_group("scoring (signals that agree = a better clip)")
    g.add_argument("--min-conf", choices=["low", "med", "high"], default="med",
                   help="only keep clips at least this confident (high = several signals agree)")
    g.add_argument("--fuse-wait", type=float, default=20, help="seconds to wait for other signals before cutting")
    g.add_argument("--merge-window", type=float, default=15, help="signals within this many seconds are one moment")
    g.add_argument("--no-speech", dest="speech", action="store_false",
                   help="don't transcribe the moment to look for reactions ('no way', 'let's go'...)")
    g.add_argument("--speech-model", default="tiny", help="Whisper model for the speech check (tiny/base/small)")
    g.add_argument("--hype-words", default="", help="extra reaction phrases, comma separated")
    g.add_argument("--no-snap", action="store_true", help="don't move clip start/end to the nearest pause")
    g = p.add_argument_group("on-screen title (a vision AI looks at the clip and writes the hook line)")
    g.add_argument("--title-ai", choices=["auto", "gemini", "claude", "off"], default="auto",
                   help="auto = use GEMINI_API_KEY or ANTHROPIC_API_KEY from the environment if one is set")
    g.add_argument("--title-model", default="", help="override the model name (default: gemini-flash-latest / claude-haiku-5-5)")
    g.add_argument("--streamer", default="", help="streamer's name; the AI may use it in titles (e.g. Speed)")
    g.add_argument("--title", default="", help="skip the AI and put this exact text on every clip")
    g = p.add_argument_group("clip timing")
    g.add_argument("--pre", type=float, default=15, help="seconds before the moment")
    g.add_argument("--post", type=float, default=12, help="seconds after the moment (when --no-smart-length)")
    g.add_argument("--max-pre", type=float, default=30,
                   help="smart start: how far before the moment a joke/story setup may begin (seconds)")
    g.add_argument("--max-len", type=float, default=75, help="longest clip allowed (seconds)")
    g.add_argument("--no-smart-length", dest="smart_length", action="store_false",
                   help="always keep --post seconds after the moment instead of ending when the reaction dies down")
    g.add_argument("--min-post", type=float, default=6, help="smart length: shortest time kept after the moment")
    g.add_argument("--max-post", type=float, default=45, help="smart length: longest time kept after the moment")
    g.add_argument("--lag", type=float, default=None,
                   help="seconds chat reacts after the event (default: 5 Twitch, 8 YouTube); raise it "
                        "if clips START too late (you miss the setup), lower it if too early")
    g = p.add_argument_group("video")
    g.add_argument("--layout", choices=["crop", "blur", "split", "podcast"], default="crop",
                   help="crop = center crop; podcast = dual-box stacked interview/podcast; "
                        "split = face cam on top + gameplay below; blur = full frame over blurred background")
    g.add_argument("--cam", default="auto",
                   help="split layout: 'auto' finds the face cam by itself, or give x,y,w,h as fractions of the frame")
    g.add_argument("--cam-share", type=float, default=0.36, help="split layout: share of the height used by the face cam")
    g.add_argument("--game-x", type=float, default=0.5,
                   help="split layout: which part of the gameplay to keep, 0=left edge 0.5=centre 1=right edge")
    g.add_argument("--style", choices=sorted(STYLES), default="classic",
                   help="look of the title and captions: classic, bold, neon, minimal, boxed")
    g.add_argument("--no-normalize", action="store_true", help="don't even out the loudness of each clip")
    g.add_argument("--caption-style", choices=["words", "lines"], default="words",
                   help="words = each spoken word lights up as it is said (needs Whisper word times); lines = plain lines")
    g.add_argument("--no-burn-subs", action="store_true", help="don't burn captions into the video (a .srt file is still written to clips/info/)")
    g.add_argument("--preview", metavar="FILE", default=None,
                   help="save preview images of the layout for a recording/clip (rec/*.ts) and exit")
    g.add_argument("--out-height", type=int, default=1920, help="output height (0 = native)")
    g.add_argument("--crf", type=int, default=18, help="x264 quality (lower = better, bigger file)")
    g.add_argument("--preset", default="fast", help="x264 preset (veryfast = quicker, medium = smaller)")
    g.add_argument("--av-offset", type=int, default=0, metavar="MS",
                   help="fix lip-sync: -300 plays the sound 300 ms earlier (use if the voice comes AFTER the lips), +300 later")
    g.add_argument("--min-rating", type=int, default=5, metavar="1-10",
                   help="with an AI key: skip clips the AI rates below this (0 = keep everything)")
    g.add_argument("--keep-wide", action="store_true", help="also save the uncropped 16:9 clip (lossless copy)")
    g = p.add_argument_group("subtitles")
    g.add_argument("--no-subs", action="store_true", help="skip Whisper")
    g.add_argument("--whisper-model", default="base", help="tiny/base/small/medium/large")
    g.add_argument("--lang", default="", help="spoken language code, e.g. en (default: auto-detect)")
    g.add_argument("--burn-subs", action="store_true", help="(captions are burned in by default now; kept for old commands)")
    g = p.add_argument_group("recording")
    g.add_argument("--quality", default="best", help="streamlink quality, e.g. best or 720p60,720p,best")
    g.add_argument("--low-latency", action="store_true", help="Twitch only: pass --twitch-low-latency to streamlink")
    g.add_argument("--streamlink-args", default="",
                   help='extra streamlink args, e.g. \'--twitch-api-header "Authorization=OAuth xxx"\'')
    g.add_argument("--max-rec-gb", type=float, default=0, help="disk cap for rec/ (0 = unlimited)")
    g.add_argument("--chat-only", action="store_true", help="no recording; just show spikes (tune thresholds)")
    g = p.add_argument_group("output")
    g.add_argument("--rec-dir", type=Path, default=HERE / "rec")
    g.add_argument("--clip-dir", type=Path, default=HERE / "clips")
    g.add_argument("--sync-dir", type=Path, default=None,
                   help="auto-sync clips to a Google Drive or cloud sync folder (e.g. 'G:\\My Drive\\Viral_Clips')")
    g.add_argument("--webhook", default="", help="Discord webhook URL to ping when a clip is ready")
    p.add_argument("--ui", action="store_true", help="open the web interface (start runs, watch the log, review clips)")
    p.add_argument("--review", action="store_true", help="open the web interface on the Clips tab")
    p.add_argument("--stop-file", default="", help=argparse.SUPPRESS)
    p.add_argument("--port", type=int, default=8765, help="port for --ui / --review")
    p.add_argument("--doctor", action="store_true", help="check ffmpeg, streamlink, Whisper, keys and exit")
    p.add_argument("--selftest", action="store_true", help="run offline logic tests and exit")
    p.add_argument("--version", action="version", version=f"clip-watcher {VERSION}")
    pre, _ = p.parse_known_args(argv)
    if pre.profile:
        p.set_defaults(**PROFILES[pre.profile])
    a = p.parse_args(argv)
    if a.selftest or a.doctor or a.review or a.ui:
        return a
    try:
        if a.cam != "auto":
            parse_cam(a.cam)
    except ValueError as e:
        p.error(str(e))
    if not (0.15 <= a.cam_share <= 0.6):
        p.error("--cam-share should be between 0.15 and 0.6")
    if a.preview:
        return a
    if not a.no_burn_subs:
        a.burn_subs = True                    # shorts-style clips need their captions, whatever the layout
    if a.file or a.vod:
        a.platform, a.channel, a.stream_title = "file", "video", ""
        a.streamlink_args = []
        a.lag = 0.0
        return a
    if not a.channel:
        p.error("give a stream link or channel, or use --file video.mp4 / --vod <link>")
    try:
        t = parse_target(a.channel, a.platform)
    except ValueError as e:
        p.error(str(e))
    a.platform, a.channel, a.stream_url, a.chat_url = t["platform"], t["label"], t["stream_url"], t["chat_url"]
    a.stream_title = ""
    if a.lag is None:
        a.lag = 8.0 if a.platform == "youtube" else 5.0
    a.streamlink_args = shlex.split(a.streamlink_args)
    return a


def check_deps(a) -> None:
    missing = [t for t in (["ffmpeg", "ffprobe"] + ([] if (a.chat_only or a.file or a.vod) else ["streamlink"]))
               if not find_tool(t)]
    if missing:
        sys.exit(f"Missing: {', '.join(missing)}. Install ffmpeg (brew/apt) and "
                 f"`pip install streamlink` (or run ./setup.sh).")
    if a.platform == "youtube" and not a.no_chat and not importlib.util.find_spec("chat_downloader"):
        sys.exit("YouTube chat needs the chat-downloader package: pip install chat-downloader "
                 "(or run ./setup.sh)")
    if a.speech and not have_asr():
        log("note: faster-whisper isn't installed -> the speech check is off")
        a.speech = False
    if not a.no_subs and not have_asr():
        log("WARNING: faster-whisper isn't installed -> clips will have NO captions "
            "(pip install faster-whisper, or pass --no-subs to silence this)")
        a.no_subs = True


def preview_layout(a) -> int:
    """Save two images: the stream frame with the face-cam / gameplay boxes drawn on it, and what the
    split layout produces. Lets you adjust --cam / --game-x without recording anything."""
    src = Path(a.preview)
    if not src.exists():
        print(f"file not found: {src}")
        return 1
    try:
        r = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                            "stream=width,height", "-of", "csv=p=0", str(src)], capture_output=True, text=True)
        line = next(ln for ln in r.stdout.splitlines() if ln.strip())      # .ts files repeat the line
        w, h = [int(v) for v in line.strip().split(",")[:2]]
    except (ValueError, OSError, StopIteration):
        print("couldn't read the video size (is it a video file?)")
        return 1
    dur = probe_duration(src) or 10.0
    t = max(0.0, min(dur * 0.5, 20.0))
    if a.cam == "auto":
        found = detect_cam(src, max(0.0, t - 6), min(12.0, dur), a.cam_share)
        if found:
            a.cam_resolved = ",".join(str(v) for v in found[0])
            print(f"face cam found automatically: --cam {a.cam_resolved} (face seen in {found[1]} frames)")
        else:
            print("no face cam found (needs a visible face and opencv-python-headless); using the default box")
    cfg = split_cfg(a)
    cx, cy, cw, ch = cfg["cam"]
    out_h = a.out_height or 1920
    out_w = int(out_h * 9 / 16) // 2 * 2
    gh = out_h - int(out_h * cfg["share"]) // 2 * 2
    gw = min(w, int(h * out_w / gh) // 2 * 2)
    gx = int(max(0, min(w - gw, w * cfg["game_x"] - gw / 2)))
    boxes = (f"drawbox=x={int(w * cx)}:y={int(h * cy)}:w={int(w * cw)}:h={int(h * ch)}:color=red:t=6,"
             f"drawbox=x={gx}:y=0:w={gw}:h={h}:color=lime:t=6")
    out1, out2 = Path("preview_boxes.jpg"), Path("preview_result.jpg")
    ok1, e1 = run_ffmpeg(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", f"{t:.2f}", "-i", str(src),
                          "-frames:v", "1", "-vf", boxes, str(out1)])
    ok2, e2 = run_ffmpeg(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", f"{t:.2f}", "-i", str(src),
                          "-frames:v", "1", "-an", *video_filter_args("split", a.out_height, cfg), str(out2)])
    if not (ok1 and ok2):
        print(f"preview failed: {e1 or e2}")
        return 1
    print(f"saved {out1.resolve()}  (RED = face cam, GREEN = gameplay part that is kept)")
    print(f"saved {out2.resolve()}  (what the final clip looks like)")
    print("Wrong? Change --cam x,y,w,h (fractions of the frame), --cam-share, or --game-x and run --preview again.")
    return 0


def load_dotenv(path: Path) -> None:
    """Read KEY=value lines from a .env file; real environment variables win."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip().removeprefix("export ").strip()
        v = v.strip().strip('"').strip("'")
        if k and v and k not in os.environ:
            os.environ[k] = v


def doctor() -> int:
    """Check everything Clip Watcher needs and say how to fix what is missing."""
    bad = 0

    def row(ok, name, detail="", fix=""):
        nonlocal bad
        print(f"  [{'ok' if ok else 'XX'}] {name}" + (f" - {detail}" if detail else ""))
        if not ok:
            bad += 1
            if fix:
                print(f"       fix: {fix}")

    print("Clip Watcher doctor\n")
    row(sys.version_info >= (3, 9), "Python", sys.version.split()[0], "install Python 3.9 or newer")
    for tool in ("ffmpeg", "ffprobe"):
        row(bool(find_tool(tool)), tool, find_tool(tool) or "",
            "Windows: winget install ffmpeg | macOS: brew install ffmpeg | Ubuntu: sudo apt install ffmpeg")
    if find_tool("ffmpeg"):
        try:
            out = subprocess.run(["ffmpeg", "-hide_banner", "-filters"], capture_output=True,
                                 text=True, timeout=20).stdout
            row(" subtitles " in out, "ffmpeg subtitle burning (libass)", "",
                "install a full ffmpeg build (on Windows: the 'gyan.dev full' build)")
        except (OSError, subprocess.SubprocessError):
            row(False, "ffmpeg runs", "", "reinstall ffmpeg")
    row(bool(find_tool("streamlink")), "streamlink", find_tool("streamlink") or "",
        "run setup.sh again (it installs into .venv)")
    print(f"  [{'ok' if find_tool('yt-dlp') else '--'}] yt-dlp (only for --vod links)"
          + ("" if shutil.which("yt-dlp") else " - optional: run setup.sh again"))
    row(have_asr(), "speech-to-text (faster-whisper) for captions", "",
        "run setup.sh again, or add --no-subs --no-speech to skip it")
    try:
        import cv2  # noqa: F401
        cam_ok = (Path(cv2.data.haarcascades) / "haarcascade_frontalface_default.xml").exists()
    except ImportError:
        cam_ok = False
    print(f"  [{'ok' if cam_ok else '--'}] automatic face-cam detection - "
          + ("ready" if cam_ok else "needs opencv (optional): run setup.sh again, or pass --cam x,y,w,h"))
    gem = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    cla = os.environ.get("ANTHROPIC_API_KEY")
    key_ok = bool(gem or cla) and "your-key" not in (gem or cla or "")
    print(f"  [{'ok' if key_ok else '--'}] AI titles - "
          + ("key found" if key_ok else "no key set (optional): put GEMINI_API_KEY=... in .env"))
    for d in (Path("rec"), Path("clips")):
        try:
            d.mkdir(exist_ok=True)
            t = d / ".write_test"
            t.write_text("x")
            t.unlink()
            row(True, f"can write to {d}/")
        except OSError as e:
            row(False, f"can write to {d}/", str(e), "run from a folder you can write to")
    print("\n" + ("All good. Try: ./watch.sh --selftest" if not bad else f"{bad} problem(s) above."))
    return 1 if bad else 0


def parse_clock(text: str) -> float:
    """'1:30:00', '45:10' or '90' (seconds) -> seconds."""
    parts = [float(x) for x in text.strip().split(":")]
    if not 1 <= len(parts) <= 3 or any(x < 0 for x in parts):
        raise ValueError(text)
    sec = 0.0
    for x in parts:
        sec = sec * 60 + x
    return sec


def parse_range(text: str, dur: float) -> tuple:
    """'2:00:00-5:30:00' -> (offset, span) clamped to the video; '' -> the whole video."""
    if not text:
        return 0.0, dur
    try:
        a_, _, b_ = text.partition("-")
        lo = parse_clock(a_) if a_.strip() else 0.0
        hi = parse_clock(b_) if b_.strip() else dur
    except ValueError:
        raise ValueError("--range should look like 1:00:00-3:30:00 (hours:minutes:seconds)")
    lo, hi = max(0.0, lo), min(dur, hi)
    if hi - lo < 30:
        raise ValueError("--range must cover at least 30 seconds of the video")
    return lo, hi - lo


def learned_weights(st: dict, base: dict, min_n: int = 8) -> dict:
    """Nudge how much each signal counts, from your Keep/Delete history: a source you keep often counts more
    (up to x1.4), one you delete often counts less (down to x0.6). Needs min_n judged clips per source."""
    out = dict(base)
    for src, c in st.items():
        n = c.get("keep", 0) + c.get("delete", 0)
        if src in base and n >= min_n:
            rate = (c["keep"] + 2) / (n + 4)                    # starts neutral, moves as evidence builds
            out[src] = round(base[src] * (0.6 + 0.8 * rate), 2)
    return out


def apply_learning(a) -> None:
    if getattr(a, "no_learn", False):
        return
    try:
        sys.path.insert(0, str(HERE))
        import review
        st = review.stats(a.clip_dir)
    except Exception:  # noqa: BLE001
        return
    new = learned_weights(st, WEIGHTS)
    changed = {k: v for k, v in new.items() if v != WEIGHTS.get(k)}
    if changed:
        WEIGHTS.update(new)
        log("learning from your Keep/Delete choices: " + ", ".join(f"{k} x{v / BASE_WEIGHTS[k]:.2f}" for k, v in changed.items()))


def fetch_vod(a) -> Path | None:
    """Download a VOD / video link with yt-dlp (YouTube, Twitch VODs and more) into rec/."""
    m = re.search(r"(?:v=|youtu\.be/|videos/)([\w-]{6,})", a.vod)
    if m and not a.fresh:                      # already downloaded earlier? don't download again
        old = sorted(a.rec_dir.glob(f"vod_{m.group(1)}.*"))
        old = [f for f in old if f.suffix.lower() in (".mp4", ".mkv", ".webm") and f.stat().st_size > 100_000]
        if old:
            log(f"using the video downloaded earlier: {old[0].name} (use --fresh to download again)")
            return old[0]
    exe = find_tool("yt-dlp")
    base_exe = [exe] if exe else None
    if not base_exe:
        try:
            import yt_dlp
            base_exe = [sys.executable, "-m", "yt_dlp"]
        except ImportError:
            base_exe = None
    if not base_exe:
        log("yt-dlp is missing: run setup.sh again (or pip install yt-dlp)")
        return None
    qual_choice = str(getattr(a, "quality", "best")).lower()
    out_h = getattr(a, "out_height", 1920) or 1920
    if qual_choice in ("2k", "1440p") or out_h >= 2560:
        fmt_sel = "bv*[height<=1440][ext=mp4]+ba[ext=m4a]/bv*[height<=1440]+ba/bv*+ba/b"
        qual_msg = "highest available up to 2K (1440p)"
    elif qual_choice in ("4k", "2160p"):
        fmt_sel = "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/b"
        qual_msg = "highest available up to 4K (2160p)"
    elif qual_choice == "720p":
        fmt_sel = "bv*[height<=720][ext=mp4]+ba[ext=m4a]/bv*[height<=720]+ba/b[height<=720]/b"
        qual_msg = "up to 720p"
    elif qual_choice == "1080p":
        fmt_sel = "bv*[height<=1080][ext=mp4]+ba[ext=m4a]/bv*[height<=1080]+ba/b[height<=1080]/b"
        qual_msg = "up to 1080p"
    else:  # "best" or default: preserve highest native broadcast clarity
        fmt_sel = "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/b"
        qual_msg = "highest available (2K/4K/1080p)"
    log(f"downloading {a.vod} ({qual_msg}; this can take a while for long streams)...")
    base = base_exe + ["--no-playlist", "-f", fmt_sel, "--merge-output-format", "mp4",
            "-o", str(a.rec_dir / "vod_%(id)s.%(ext)s"), "--print", "after_move:filepath", "--no-simulate"]
    tries = [("", []),
             ("trying another YouTube client", ["--extractor-args", "youtube:player_client=tv,android,web"]),
             ("trying with your Chrome sign-in", ["--cookies-from-browser", "chrome"]),
             ("trying with your Edge sign-in", ["--cookies-from-browser", "edge"]),
             ("trying with your Firefox sign-in", ["--cookies-from-browser", "firefox"])]
    err, upgraded = "unknown error", False
    for label, extra in tries:
        if label:
            log(f"download failed ({err[:120]}) - {label}...")
        try:
            r = subprocess.run(base + extra + [a.vod], capture_output=True, text=True, timeout=6 * 3600)
        except (OSError, subprocess.SubprocessError) as e:
            log(f"download failed: {e}")
            return None
        lines = [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]
        if r.returncode == 0 and lines and Path(lines[-1]).exists():
            return Path(lines[-1])
        err = (r.stderr.strip().splitlines() or ["unknown error"])[-1]
        if not upgraded and re.search(r"403|forbidden|sign in|unable to extract|nsig|player", err, re.I):
            upgraded = True      # YouTube changes often; an old yt-dlp is the usual cause of 403
            log("updating yt-dlp (YouTube changes often)...")
            subprocess.run([sys.executable, "-m", "pip", "install", "-U", "--quiet", "yt-dlp"], capture_output=True, text=True)
            exe2 = shutil.which("yt-dlp")
            if exe2:
                base[0] = exe2
            try:
                r = subprocess.run(base + [a.vod], capture_output=True, text=True, timeout=6 * 3600)
                lines = [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]
                if r.returncode == 0 and lines and Path(lines[-1]).exists():
                    return Path(lines[-1])
                err = (r.stderr.strip().splitlines() or ["unknown error"])[-1]
            except (OSError, subprocess.SubprocessError):
                pass
    log("download failed: " + err[:200])
    log("tip: download the video yourself (yt-dlp, or any downloader) and run with --file path\\to\\video.mp4")
    return None


def ai_sweep(a, tw, transcriber, path: Path, rec, fusion, offset: float, span: float, stop, chunk: float = 480.0) -> int:
    """Walk the video in ~8 minute chunks, show the vision model frames + the words said, and add what it picks
    as "ai" moments. Costs one API call per chunk."""
    added, t = 0, offset
    end = offset + span
    per = max(1, min(6, int(chunk // 120)))
    while t < end - 3 and not stop.is_set():
        length = min(chunk, end - t)
        said = ""
        if transcriber is not None and not transcriber.failed and length <= 900:
            res = transcriber.transcribe(path, t, length)
            if res:
                said = " ".join(f"[{u[0]:.0f}s] {u[2]}" for u in utterances(abs_words(res, t))[:80])
        for at, score, why in tw.sweep(path, t, length, said, k=per, n=12 if length > 60 else 8):
            if score < 6:
                continue
            fusion.add(rec, "ai", at, min(2.0, score / 4), f"AI: {why}", None, now=0.0)
            log(f"  AI spotted {at / 60:.1f} min ({score:.0f}/10): {why}")
            added += 1
        t += chunk
    return added


def run_viral_pipeline(a, path: Path, rec: Recording, transcriber, offset: float, span: float, stop) -> int:
    """Professional full-transcript semantic story extraction (OpusClip/Klap architecture)."""
    import viral_engine
    gem_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or ""
    segmenter = viral_engine.SemanticSegmenter(gem_key, getattr(a, "title_model", "") or "gemini-1.5-flash")
    if not segmenter.is_available():
        log("viral engine: no valid GEMINI_API_KEY set (expected 'AIzaSy...'). Set it in .env to enable semantic viral clipping.")
        return 0

    log("viral engine: transcribing full audio with Whisper for semantic story extraction...")
    t0_trans = time.time()
    res = transcriber.transcribe(path, offset, span)
    if not res:
        log("viral engine: speech transcription returned no content.")
        return 0
    log(f"viral engine: transcription finished in {time.time() - t0_trans:.1f}s")

    words = []
    for sg in res.get("segments", []):
        for w in sg.get("words", []):
            words.append({"word": w.get("word", ""), "start": w.get("start", 0.0), "end": w.get("end", 0.0)})

    if not words:
        log("viral engine: no spoken words found in transcript.")
        return 0

    log(f"viral engine: analyzing {len(words)} spoken words with Gemini to find viral hooks & narratives...")
    segments = segmenter.extract_viral_segments(words, top_k=a.top or 5)
    if not segments:
        log("viral engine: AI identified no high-retention viral segments.")
        return 0

    log(f"viral engine: found {len(segments)} viral story segment(s)!")
    target_h = getattr(a, "out_height", 1920) or 1920
    target_w = int(target_h * 9 / 16) // 2 * 2
    framer = viral_engine.DynamicAutoFramer(target_w=target_w, target_h=target_h)
    made = 0

    for idx, seg in enumerate(segments):
        if stop.is_set():
            break
        v_score = seg["virality_score"]
        h_score = seg["hook_strength"]
        if getattr(a, "min_viral", 0) and v_score < a.min_viral:
            log(f"  skipped segment at {seg['start_s']:.1f}s (score {v_score} < min-viral {a.min_viral})")
            continue

        s0 = seg["start_s"]
        e0 = seg["end_s"]
        length = e0 - s0
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        base = f"{a.channel}_{stamp}_VIRAL_{v_score}pts_{idx+1}"
        dst = a.clip_dir / f"{base}.mp4"
        ass_path = a.clip_dir / f"{base}.ass"

        # Slice words for this clip
        clip_words = [w for w in words if w["start"] >= s0 - 0.2 and w["end"] <= e0 + 0.5]
        if getattr(a, "karaoke", True) and clip_words and not a.no_subs:
            viral_engine.generate_karaoke_ass(clip_words, ass_path, hook_title=seg["suggested_title"], clip_start_s=s0)
        else:
            ass_path = None

        log(f"  [VIRAL {v_score}/100 | Hook {h_score}/10] {s0:.1f}s - {e0:.1f}s ({length:.0f}s): {seg['suggested_title']}")
        log(f"    Hook: \"{seg['hook']}\"")
        log(f"    Topic: {seg['topic']} | Reason: {seg['reason']}")

        # Compute dynamic auto-framer or podcast multi-framer crop
        if a.layout in ("podcast", "podcast-split"):
            multi_framer = getattr(a, "_multi_framer", None)
            if multi_framer is None:
                multi_framer = viral_engine.MultiFaceClusterer(target_w=target_w, target_h=target_h)
                a._multi_framer = multi_framer
            crop_filter = multi_framer.compute_dual_box_filter(path, s0, length)
        elif a.layout == "crop":
            crop_filter = framer.compute_crop_box(path, s0, length)
        else:
            crop_filter = None
        t_enc = time.time()
        ok, err = viral_engine.render_viral_vertical_clip(
            path, s0, length, dst, ass_path=ass_path, crop_filter=crop_filter,
            use_gpu=getattr(a, "gpu", True), target_w=target_w, target_h=target_h
        )

        if ass_path and ass_path.exists():
            ass_path.unlink(missing_ok=True)

        if not ok:
            log(f"  clip rendering failed: {err}")
            continue

        # Thumbnail
        run_ffmpeg(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", f"{s0 + 1.0:.2f}",
                    "-i", str(path), "-frames:v", "1", "-q:v", "3", str(info_dir(dst) / f"{base}.jpg")])

        # Write SRT
        if clip_words and not a.no_subs:
            cues = make_cues({"segments": [{"start": w["start"], "end": w["end"], "text": w["word"]} for w in clip_words]})
            if cues:
                write_srt(cues, info_dir(dst) / f"{base}.srt")

        # Save post text
        post_lines = [
            f"Title: {seg['suggested_title']}",
            f"Hook: {seg['hook']}",
            f"Virality Score: {v_score}/100 (Hook: {h_score}/10)",
            f"Why: {seg['reason']}",
            f"Tags: {' '.join(seg['hashtags'])}",
            "",
            "Description:",
            f"{seg['hook']} Watch until the end to see what happens!",
        ]
        (info_dir(dst) / f"{base}_post.txt").write_text("\n".join(post_lines) + "\n", encoding="utf-8")

        # Save JSON metadata
        meta = {
            "time": datetime.now().isoformat(timespec="seconds"),
            "platform": a.platform, "channel": a.channel,
            "stream_title": getattr(a, "stream_title", ""), "clip": dst.name,
            "title": seg["suggested_title"], "virality_score": v_score, "hook_strength": h_score,
            "hook": seg["hook"], "topic": seg["topic"], "reason": seg["reason"],
            "hashtags": seg["hashtags"], "confidence": "high", "score": v_score / 10.0,
            "sources": ["semantic_ai", "whisper"],
            "signals": {"semantic": {"strength": v_score / 50.0, "detail": f"Virality {v_score}/100"}},
            "said": seg["hook"], "recording": path.name, "position_s": round(s0, 1),
            "rec_start_s": round(s0, 2), "length_s": round(length, 2),
        }
        with open(a.clip_dir / "clips.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(meta, ensure_ascii=False) + "\n")

        made += 1
        log(f"  CLIP CREATED: {dst.name} (exported in {time.time() - t_enc:.1f}s)")

    return made


def run_file(a) -> int:
    """Find the best moments in a finished video (a file, or a downloaded VOD) and cut clips from them."""
    a.rec_dir.mkdir(parents=True, exist_ok=True)
    a.clip_dir.mkdir(parents=True, exist_ok=True)
    if a.vod:
        path = fetch_vod(a)
        if path is None:
            return 1
    else:
        path = Path(a.file.strip().strip('"').strip("'")).expanduser()
    if not path.is_file():
        log(f"file not found: {path}")
        return 1
    dur = probe_duration(path)
    if not dur:
        log("couldn't read that video (is it a video file?)")
        return 1
    a.platform, a.channel, a.stream_title = "file", re.sub(r"[^\w.-]+", "_", path.stem)[:40] or "video", path.stem
    a.lag = 0.0
    stop = threading.Event()
    transcriber = None if (a.no_subs or not have_asr()) else Transcriber(a)
    speech = SpeechScorer(a) if (transcriber is None and a.speech and have_asr()) else None
    titles = TitleWriter(a) if (a.title_ai != "off" and not a.title) else None
    maker = ClipMaker(a, stop, transcriber, speech, titles)
    maker.limit = a.top
    apply_learning(a)
    log(f"clip-watcher {VERSION} | video: {path.name} ({dur / 60:.0f} min) | layout={a.layout} | "
        f"best {a.top or 'all'} moments")
    if titles is not None:
        log(f"titles: AI on ({titles.provider}, {titles.model})" if titles.ready else
            "titles: off (add a GEMINI_API_KEY in .env for AI titles and the 'worth posting?' check)")

    rec = Recording(path)
    rec.finished = True
    try:
        offset, span = parse_range(a.range, dur)
    except ValueError as e:
        log(str(e))
        return 1
    if a.range:
        log(f"only looking at {offset / 3600:.1f}h to {(offset + span) / 3600:.1f}h of the video")

    if a.min_conf == "high":
        a.min_conf = "med"
        log("note: --min-conf high needs three kinds of evidence at once, which a video without chat can't give - "
            "using med instead")
    if span < a.warmup * 4:                      # short video: don't waste a third of it on warm-up
        a.warmup = max(3.0, round(min(span * 0.05, 8.0), 1))
        log(f"short video: warm-up shortened to {a.warmup:.0f} s")
    ai_scan = a.scan == "ai"
    if ai_scan:
        a.scan = "fast"

    # --- 0) semantic viral story extraction (OpusClip/Klap mode) ---
    gem_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or ""
    use_viral = (getattr(a, "mode", "auto") == "viral") or (
        getattr(a, "mode", "auto") == "auto" and gem_key.startswith("AIzaSy") and transcriber is not None and not a.no_subs
    )
    if use_viral and transcriber is not None:
        made = run_viral_pipeline(a, path, rec, transcriber, offset, span, stop)
        if made > 0:
            log(f"done. {made} viral clip(s) in {a.clip_dir}")
            return 0
        if getattr(a, "mode", "auto") == "viral":
            log(f"done. 0 clip(s) in {a.clip_dir}")
            return 0
        log("viral engine: falling back to loudness and motion spike detection...")

    # --- 1) scan the video once (saved, so a second run or a resume does not repeat it) ---
    st_ = path.stat()
    key = "|".join(map(str, (st_.st_size, int(st_.st_mtime), a.audio_db, a.motion_factor, a.motion_min, a.warmup,
                             a.cooldown, a.scan, a.audio, a.motion, round(offset), round(span))))
    cache = a.rec_dir / f"{path.stem}.analysis.json"
    found: list = []
    jump = (0.0, 0.0, 0.0)
    best_motion = (0.0, 0.0, 0.0, 0.0)
    mw = None
    try:
        saved = json.loads(cache.read_text(encoding="utf-8")) if (cache.exists() and not a.fresh) else None
    except (OSError, ValueError):
        saved = None
    if saved and saved.get("key") == key:
        found = [tuple(e) for e in saved["events"]]
        jump = tuple(saved.get("jump") or jump)
        best_motion = tuple(saved.get("best_motion") or best_motion)
        log(f"reusing the saved scan of this video ({len(found)} loud/fast moments); use --fresh to scan again")
    else:
        events: queue.Queue = queue.Queue()
        mw = MediaWatcher(a, None, stop, events)
        mw.direct, mw.offset, mw.span = True, offset, span
        is_deep_visual = (span <= 3600) and getattr(a, "motion", True)
        if is_deep_visual or a.scan == "full":
            mw.only = {k for k in ("audio", "motion") if getattr(a, k, True)}
            mode_desc = "deep visual & sound scan (motion + audio from second 0)"
        else:
            mw.only = {k for k in (("audio", "motion") if a.scan == "full" else ("audio",)) if getattr(a, k, True)}
            mode_desc = "fast audio scan (motion checked around candidate peaks)"
        th = threading.Thread(target=mw.analyze, args=(rec,), daemon=True)
        log(f"scanning {span / 60:.1f} minutes | mode: {mode_desc}")
        t0 = time.time()
        th.start()
        try:
            last = 0.0
            while th.is_alive():
                th.join(5)
                if time.time() - last >= 20 and th.is_alive():
                    last = time.time()
                    done = max(mw.progress.values(), default=0.0)
                    eta = (time.time() - t0) * (span - done) / done if done > 5 else 0
                    log(f"  scanned {done / 60:.0f} of {span / 60:.0f} min" + (f" - about {eta / 60:.0f} min left" if eta else ""))
        except KeyboardInterrupt:
            stop.set()
            log("stopped")
            return 130
        log(f"scan done in {(time.time() - t0) / 60:.1f} min")
        jump = mw.best_jump
        best_motion = getattr(mw, "best_motion", (0.0, 0.0, 0.0, 0.0))
        while True:
            try:
                _r, kind, pos, score, detail = events.get_nowait()
            except queue.Empty:
                break
            found.append((kind, pos, score, detail))
        try:
            cache.write_text(json.dumps({"key": key, "events": found, "jump": list(jump), "best_motion": list(best_motion)}), encoding="utf-8")
        except OSError:
            pass

    # --- 2) rank the moments ---
    fusion = Fusion(a.merge_window, a.fuse_wait, a.cooldown)
    for kind, pos, score, detail in found:
        strength = min(2.0, score / (a.audio_db if kind == "audio" else a.motion_factor))
        fusion.add(rec, kind, pos, strength, detail, None, now=0.0)
    if not found and jump[0] > 0:
        if jump[1] <= a.warmup:
            log(f"diagnosis: the biggest sound jump was +{jump[0]:.0f} dB at {jump[1]:.1f}s, "
                f"which meets the {a.audio_db:.0f} dB threshold, but occurred during the initial {a.warmup:.0f}s warm-up period. "
                f"Use --warmup 3 to capture early moments.")
        elif jump[0] < a.audio_db:
            log(f"diagnosis: the biggest sound jump was +{jump[0]:.0f} dB at {jump[1] / 60:.1f} min, "
                f"but {a.audio_db:.0f} dB is needed (--audio-db to lower it). A quiet visual surprise has no sound jump - "
                "the AI picture sweep below looks for those")
        else:
            log(f"diagnosis: a sound jump of +{jump[0]:.0f} dB was seen at {jump[1] / 60:.1f} min, but did not trigger a candidate clip.")

    # Adaptive VOD thresholding:
    # Modern YouTube videos/VODs are mastered and dynamic-range compressed,
    # so relative sound spikes are ~1.8 to 5.0 dB rather than +9 dB.
    if not fusion.open and jump[0] >= 1.8:
        fusion.add(rec, "audio", jump[1], max(0.9, jump[0] / 3.0),
                   f"relative sound peak +{jump[0]:.1f} dB ({jump[2]:.0f} dBFS)", None, now=0.0)
        log(f"adaptive scan: captured relative sound peak +{jump[0]:.1f} dB at {jump[1]:.1f}s")
    elif not fusion.open and dur <= 90:
        peak_t = jump[1] if jump[0] > 0 else (dur / 2.0)
        fusion.add(rec, "audio", peak_t, 1.0, f"short video highlight ({dur:.0f}s)", None, now=0.0)
        log(f"short video: video is {dur:.0f}s; clipping highlight around peak at {peak_t:.1f}s")

    # Adaptive Computer Vision Motion Peak:
    b_motion = getattr(mw, "best_motion", best_motion) if mw is not None else best_motion
    if b_motion[0] >= 1.4 and (not fusion.open or len(fusion.open) < (a.top or 3)):
        m_factor, m_t, m_avg, m_base = b_motion
        fusion.add(rec, "motion", m_t, min(2.0, m_factor / 1.5),
                   f"peak visual action {m_factor:.1f}x (diff {m_avg:.0f} vs {m_base:.0f})", None, now=0.0)
        log(f"adaptive scan: captured peak visual action {m_factor:.1f}x at {m_t:.1f}s")

    # Offline Speech Hype Detection:
    if transcriber is not None and a.speech and (not fusion.open or len(fusion.open) < (a.top or 3)):
        try:
            res_speech = transcriber.transcribe(path, offset, span)
            utts_speech = utterances(abs_words(res_speech, offset))
            speech_hits = []
            for s0, e0, txt in utts_speech:
                sc, hits = text_score(txt)
                if sc >= 0.8:
                    speech_hits.append(((s0 + e0) / 2.0, sc, hits, txt))
            if speech_hits:
                speech_hits.sort(key=lambda x: -x[1])
                for pos_t, sc, hits, txt in speech_hits[:(a.top or 3)]:
                    fusion.add(rec, "speech", pos_t, sc, f'speech reaction: "{", ".join(dict.fromkeys(hits))[:40]}"', None, now=0.0)
                log(f"speech scan: added {len(speech_hits[:(a.top or 3)])} moment(s) from spoken reactions")
        except Exception:  # noqa: BLE001
            pass

    # AI sweep: ask the vision model for moments that are interesting to LOOK at, even when quiet.
    # Always on with --scan ai, and as a fallback when the sound scan found nothing (needs an AI key).
    sweep_titles = titles if titles is not None else TitleWriter(a)
    use_sweep = sweep_titles.ready and (ai_scan or not fusion.open or (dur <= 600 and a.scan == "fast"))
    if use_sweep:
        added = ai_sweep(a, sweep_titles, transcriber, path, rec, fusion, offset, span, stop)
        log(f"AI sweep: {added} moment(s) found by looking at the picture")
    elif ai_scan:
        log("--scan ai needs an AI key (GEMINI_API_KEY in .env)")
    elif not fusion.open:
        log("no AI key set, so the picture can't be checked - add GEMINI_API_KEY to .env to catch quiet moments")
    cands = sorted(fusion.open, key=lambda mo: -score_moment(mo.ev)[0])
    limit = (a.top * 4) if a.top else len(cands)
    cands = cands[:limit]
    if cands and (a.top or dur <= 300) and CONF_RANK[a.min_conf] > CONF_RANK["low"]:
        a.min_conf = "low"
        log("video mode: adjusted min-conf to 'low' so single-signal moments are preserved")
    log(f"found {len(found)} loud/fast moments -> {len(fusion.open)} candidate moments; checking the best {len(cands)}")
    if a.scan == "fast" and a.motion and cands:
        log("checking the picture around each candidate...")
        for mo in cands:
            if stop.is_set():
                break
            r_ = motion_check(path, mo.ref_pos(), a.motion_factor, a.motion_min)
            if r_:
                mo.ev["motion"] = {"strength": min(2.0, r_[0] / a.motion_factor), "pos": mo.ref_pos(),
                                   "detail": f"action {r_[1]:.0f} vs normal {r_[1] / r_[0]:.0f}"}
        cands.sort(key=lambda mo: -score_moment(mo.ev)[0])

    # --- 3) resume: clips already made from this video count, and their time spans are not cut again ---
    try:
        for line in (a.clip_dir / "clips.jsonl").read_text(encoding="utf-8", errors="replace").splitlines():
            r0 = json.loads(line)
            if r0.get("recording") == path.name and (a.clip_dir / r0.get("clip", "-")).exists():
                maker.spans.append((rec.path, r0["rec_start_s"], r0["rec_start_s"] + r0["length_s"]))
    except (OSError, ValueError, KeyError):
        pass
    if maker.spans:
        log(f"resuming: {len(maker.spans)} clip(s) from this video already exist" + (f" (target {a.top})" if a.top else ""))
    pool = ThreadPoolExecutor(max_workers=3)
    try:
        for i in range(0, len(cands), 3):
            if a.top and len(maker.spans) >= a.top:
                break
            futs = []
            for mo in cands[i:i + 3]:
                rec.acquire()
                futs.append(pool.submit(maker.make, mo))
            for f in futs:
                f.result()
    except KeyboardInterrupt:
        log("stopping...")
        stop.set()
    pool.shutdown(wait=True)
    log(f"done. {maker.made} clip(s) in {a.clip_dir}")
    if maker.results:
        best = sorted(maker.results, reverse=True)[:5]
        log("best clips: " + " | ".join(f"{c.upper()} {sc:.1f} {n}" for sc, c, n in best))
    return 0


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent / ".env")
    a = parse_args()
    if a.doctor:
        return doctor()
    if a.review or a.ui:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import webui
        return webui.serve(HERE, a.clip_dir, a.port, "clips" if a.review else "run")
    if a.selftest:
        return selftest()
    if a.preview:
        return preview_layout(a)
    check_deps(a)
    a.rec_dir.mkdir(parents=True, exist_ok=True)
    a.clip_dir.mkdir(parents=True, exist_ok=True)
    if a.file or a.vod:
        return run_file(a)
    apply_learning(a)

    stop = threading.Event()
    det_lock = threading.Lock()
    det = SpikeDetector(a.bucket, a.window, a.factor, a.min_msgs, a.min_users, a.warmup, a.cooldown, a.user_cap, a.sigma)

    def on_message(ts, user, text):
        with det_lock:
            det.add(ts, user, text)

    chat = None if a.no_chat else (YouTubeChatReader(a, on_message, stop) if a.platform == "youtube"
                                   else ChatReader(a.channel, on_message, stop))
    recorder = None if a.chat_only else Recorder(a, stop)
    media_events: queue.Queue = queue.Queue()
    media = (MediaWatcher(a, recorder, stop, media_events)
             if recorder and (a.audio or a.motion) else None)
    
    transcriber = None if (a.no_subs or a.chat_only) else Transcriber(a)
    speech = SpeechScorer(a) if (a.speech and not a.chat_only) else None
    titles = TitleWriter(a) if (not a.chat_only and a.title_ai != "off" and not a.title) else None
    maker = ClipMaker(a, stop, transcriber, speech, titles)
    fusion = Fusion(a.merge_window, a.fuse_wait, a.cooldown)
    pool = ThreadPoolExecutor(max_workers=4)

    def chat_strength(sp) -> float:
        s_ = min(2.0, sp.ratio / max(a.factor, 0.1)) * (1 + 0.5 * sp.hype)   # hype emotes count extra
        if sp.clipit >= 2:                                                      # "clip it" in chat
            s_ += 0.5
        return min(s_, 2.5)

    def ingest(rec, src, pos, strength, detail, spike=None) -> None:
        """Hand one signal to the fusion; `rec` was acquired for this signal."""
        r = fusion.add(rec, src, pos, strength, detail, spike)
        if r != "new":
            rec.release()                      # the moment already holds its own reference
        if r == "dup":
            log("      same moment as the clip just made")
        elif r == "joined":
            log("      same moment as another signal -> higher confidence")

    triggers = ([] if a.no_chat else [f"chat >= {a.factor}x median and >= {a.min_msgs} msgs/{a.bucket:.0f}s"])
    if media and a.audio:
        triggers.append(f"audio +{a.audio_db:.0f} dB")
    if media and a.motion:
        triggers.append(f"motion {a.motion_factor}x")
    log(f"clip-watcher {VERSION} | {a.platform}:{a.channel} | triggers: " + "; ".join(triggers or ["none!"]))
    log(f"clips: -{a.pre:.0f}s/+{a.post:.0f}s around the moment, layout={a.layout}, "
        f"subs={'off' if transcriber is None else a.whisper_model}  |  Ctrl-C to stop")
    if titles is not None:
        if titles.ready:
            log(f"titles: AI hook titles on ({titles.provider}, {titles.model})")
        else:
            log("titles: off (set GEMINI_API_KEY or ANTHROPIC_API_KEY to get AI-written titles; "
                "or use --title \"text\")" if a.title_ai == "auto"
                else f"titles: --title-ai {a.title_ai} needs its API key in the environment; titles are off")
    if chat:
        chat.start()
    if recorder:
        recorder.start()
    if media:
        media.start()
    try:
        while True:
            time.sleep(1)
            if a.stop_file and Path(a.stop_file).exists():
                log("stop requested from the web page")
                break
            with det_lock:
                spikes = det.tick(time.time())
                last = det.last
            for sp in spikes:
                top = " ".join(f"{t}x{c}" for t, c in sp.terms[:3])
                hype = f", {sp.hype:.0%} hype" if sp.hype >= 0.2 else ""
                ask = f", {sp.clipit}x 'clip it'" if sp.clipit else ""
                log(f"SPIKE {sp.raw} msgs / {a.bucket:.0f}s "
                    f"({sp.ratio:.1f}x normal, {sp.users} chatters{hype}{ask}) {top}")
                if recorder is None:
                    continue
                rec = recorder.acquire_current()
                if rec is None:
                    log("      ignored: not recording (stream offline?)")
                    continue
                pos = max(0.0, maker._locate(sp, rec)[0])
                ingest(rec, "chat", pos, chat_strength(sp),
                       f"chat {sp.ratio:.1f}x ({sp.raw} msgs{hype})", sp)

            while True:
                try:
                    rec, kind, pos, score, detail = media_events.get_nowait()
                except queue.Empty:
                    break
                log(f"MOMENT {kind}: {detail} at {pos:.0f}s")
                strength = min(2.0, score / (a.audio_db if kind == "audio" else a.motion_factor))
                rec.acquire()
                ingest(rec, kind, pos, strength, detail)

            for mo in fusion.due():
                pool.submit(maker.make, mo)

            rec_txt = "chat-only"
            if recorder:
                r = recorder.current
                rec_txt = f"REC {time.strftime('%H:%M:%S', time.gmtime(time.time() - r.t0))}" if r else "waiting for stream"
            chat_txt = f"chat {chat.state}" if chat else "chat off"
            bucket_txt = ""
            if last:
                c, m, thr, n = last
                warm = "" if n * a.bucket >= a.warmup else " (warming up)"
                bucket_txt = f" | last {a.bucket:.0f}s: {c} msgs, median {m:.0f}, trigger >= {thr:.0f}{warm}"
            media_txt = ""
            if media:
                st = dict(media.status)
                if "audio" in st:
                    media_txt += f" | audio {st['audio'][0]:.0f}dB (normal {st['audio'][1]:.0f})"
                if "motion" in st:
                    media_txt += f" | motion {st['motion'][0]:.0f} (normal {st['motion'][1]:.0f})"
            status_line(f"[{rec_txt}] {chat_txt}{bucket_txt}{media_txt} | clips {maker.made}")
    except KeyboardInterrupt:
        pass

    log("stopping... (finishing pending clips; Ctrl-C again to force quit)")
    try:
        stop.set()
        if recorder:
            recorder.stop()
        pool.shutdown(wait=True)
    except KeyboardInterrupt:
        log("force quit")
        return 130
    log(f"done. {maker.made} clip(s) in {a.clip_dir}")
    if maker.results:
        best = sorted(maker.results, reverse=True)[:5]
        log("best clips: " + " | ".join(f"{c.upper()} {sc:.1f} {n}" for sc, c, n in best))
    return 0


if __name__ == "__main__":
    sys.exit(main())
