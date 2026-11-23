"""Clip Watcher web interface:  ./watch.sh --ui

A small web app that runs on YOUR computer only (127.0.0.1). From one page you can
  * paste a stream link, pick a profile and press Start (it runs clipwatcher.py for you),
  * watch the live log,
  * review the clips (keep / delete / copy the post text),
  * check your setup and save your AI key.
Nothing is uploaded anywhere. Clip Watcher itself still talks only to the stream and (if you set a key) the AI you chose.
"""
from __future__ import annotations

import collections
import json
import mimetypes
import os
import re
import secrets
import shlex
import shutil
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import review

HERE = Path(__file__).resolve().parent

# Ensure local .venv/Scripts, bin, and python scripts directories are always in PATH
for _cand in (HERE / ".venv" / "Scripts", HERE / ".venv" / "bin",
             Path(sys.executable).parent, Path(sys.prefix) / "Scripts"):
    if _cand.exists() and str(_cand) not in os.environ.get("PATH", ""):
        os.environ["PATH"] = str(_cand) + os.pathsep + os.environ.get("PATH", "")

PROFILES = ["gaming", "cricket", "movies", "sports", "podcast", "just-chatting", "irl", "highlights"]
LAYOUTS = ["crop", "blur", "split", "podcast"]
KEY_NAMES = {"gemini": "GEMINI_API_KEY", "anthropic": "ANTHROPIC_API_KEY"}
DEFAULTS = {"source": "live", "top": "", "range": "", "url": "", "url2": "", "url3": "", "url4": "", "profile": "highlights", "category": "auto", "layout": "crop", "cam": "mid-left", "quality": "2k", "streamer": "", "min_conf": "", "min_rating": "",
            "caption_style": "", "style": "", "extra": "", "sync_dir": "",
            "profile2": "", "layout2": "", "category2": "", "streamer2": "",
            "profile3": "", "layout3": "", "category3": "", "streamer3": "",
            "profile4": "", "layout4": "", "category4": "", "streamer4": ""}
BAD_EXTRA = {"--ui", "--review", "--selftest", "--doctor", "--preview", "--stop-file"}


# --------------------------------------------------------------------------- #
# settings + keys
# --------------------------------------------------------------------------- #
def load_settings(here: Path) -> dict:
    try:
        data = json.loads((here / "ui_settings.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        data = {}
    return {k: str(data.get(k, v)) for k, v in DEFAULTS.items()}


def save_settings(here: Path, s: dict) -> None:
    clean = {k: str(s.get(k, DEFAULTS[k]))[:500] for k in DEFAULTS}
    (here / "ui_settings.json").write_text(json.dumps(clean, indent=1), encoding="utf-8")


def read_env(here: Path) -> dict:
    out = {}
    try:
        for line in (here / ".env").read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    except OSError:
        pass
    return out


def key_state(here: Path) -> dict:
    env = read_env(here)
    res = {}
    for n, v in KEY_NAMES.items():
        val = env.get(v) or os.environ.get(v, "")
        if n == "gemini":
            res[n] = bool(val and (val.startswith("AIzaSy") or len(val) >= 20))
        elif n == "anthropic":
            res[n] = bool(val and val.startswith("sk-ant-") and len(val) >= 30)
        else:
            res[n] = bool(val)
    return res


def save_key(here: Path, name: str, value: str) -> bool:
    """Write KEY=value into .env (keeping other lines). Empty value removes the key."""
    var = KEY_NAMES.get(name)
    if not var or not re.fullmatch(r"[A-Za-z0-9_\-.]{0,300}", value or ""):
        return False
    path = here / ".env"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        lines = []
    out, done = [], False
    for line in lines:
        if line.strip().removeprefix("export ").split("=", 1)[0].strip() == var:
            if value and not done:
                out.append(f"{var}={value}")
            done = True
        else:
            out.append(line)
    if value and not done:
        out.append(f"{var}={value}")
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    if value:
        os.environ[var] = value
    else:
        os.environ.pop(var, None)
    return True


# --------------------------------------------------------------------------- #
# build the command line(s) from the form (supports 1 to 4 streams simultaneously)
# --------------------------------------------------------------------------- #
def extract_label(url: str, idx: int) -> str:
    url = url.strip()
    m_yt_h = re.search(r"youtube\.com/@([\w.-]+)", url, re.I)
    if m_yt_h:
        return m_yt_h.group(1).upper()
    m_yt_id = re.search(r"(?:v=|youtu\.be/)([\w-]{6,})", url)
    if m_yt_id:
        return f"YT_{m_yt_id.group(1)[:8]}"
    m_tw = re.search(r"twitch\.tv/([\w]+)", url, re.I)
    if m_tw:
        return m_tw.group(1).upper()
    if "/" not in url and "\\" not in url and "." not in url and len(url) > 2:
        return url.upper()
    p = Path(url)
    if p.suffix:
        return p.stem[:12].upper()
    return f"STREAM_{idx+1}"


def build_jobs(s: dict, here: Path, stop_file: Path | None = None) -> tuple[list[tuple[str, list[str], Path]] | None, str]:
    src = s.get("source", "live") or "live"
    raw_urls = []
    for k in ("url", "url2", "url3", "url4"):
        v = (s.get(k) or "").strip()
        if v:
            for part in re.split(r"[\r\n,]+", v):
                p_clean = part.strip()
                if p_clean and not p_clean.startswith("#") and p_clean not in raw_urls:
                    raw_urls.append(p_clean)

    if not raw_urls:
        return None, "Paste a stream link or channel name first." if src == "live" else "Paste the video link first."

    # Batch playlist & channel handle expansion
    expanded = []
    for u in raw_urls:
        try:
            import clipwatcher
            resolved = clipwatcher.resolve_playlist_or_channel(u)
            expanded.extend(resolved)
        except Exception:
            expanded.append(u)
    raw_urls = expanded

    if src not in ("live", "file", "vod"):
        return None, "Unknown source."

    top = str(s.get("top", "")).strip()
    if top and src != "live":
        if not (top.isdigit() and 0 <= int(top) <= 200):
            return None, "Number of clips must be 0 to 200."
    prof = s.get("profile", "")
    if prof and prof not in PROFILES:
        return None, "Unknown profile."
    lay = s.get("layout", "")
    if lay and lay not in LAYOUTS:
        return None, "Unknown layout."
    mc = s.get("min_conf", "")
    if mc and mc not in ("low", "med", "high"):
        return None, "Unknown confidence."
    mr = str(s.get("min_rating", "")).strip()
    if mr:
        if not (mr.isdigit() and 0 <= int(mr) <= 10):
            return None, "Minimum AI rating must be 0 to 10."
    rg = str(s.get("range", "")).strip()
    if rg and src != "live":
        if not re.fullmatch(r"[0-9:.]*-[0-9:.]*", rg):
            return None, "Time range should look like 1:00:00-3:30:00."
    cs = s.get("caption_style", "")
    if cs and cs not in ("words", "lines"):
        return None, "Unknown caption style."
    sty = s.get("style", "")
    if sty and sty not in ("classic", "bold", "neon", "minimal", "boxed"):
        return None, "Unknown text style."
    extra = (s.get("extra") or "").strip()
    parts = []
    if extra:
        try:
            parts = shlex.split(extra)
        except ValueError:
            return None, "The extra options have an unclosed quote."
        if any(p.split("=")[0] in BAD_EXTRA for p in parts):
            return None, "Those extra options can't be used here."

    st = (s.get("streamer") or "").strip()
    if st and (st.startswith("-") or len(st) > 60):
        return None, "Streamer name looks wrong."
    cat = (s.get("category") or "").strip()

    jobs = []
    for idx, url in enumerate(raw_urls):
        if src == "file":
            url_clean = url.strip('"').strip("'")
            if not url_clean:
                return None, "Paste the path of the video file."
            if not Path(url_clean).expanduser().is_file():
                return None, "I can't find that file. On Windows: hold Shift, right-click the file, 'Copy as path'."
            target_args = ["--file", url_clean]
        elif src == "vod":
            if url.startswith("-"):
                return None, "That doesn't look like a video link."
            target_args = ["--vod", url]
        else:  # live
            if url.startswith("-") or any(c in url for c in "\r\n\0"):
                return None, "That doesn't look like a stream link."
            target_args = [url]

        prof_i = (s.get(f"profile{idx+1}") or "").strip() if idx > 0 and (s.get(f"profile{idx+1}") or "").strip() else prof
        lay_i = (s.get(f"layout{idx+1}") or "").strip() if idx > 0 and (s.get(f"layout{idx+1}") or "").strip() else lay
        cat_i = (s.get(f"category{idx+1}") or "").strip() if idx > 0 and (s.get(f"category{idx+1}") or "").strip() else cat
        st_i = (s.get(f"streamer{idx+1}") or "").strip() if idx > 0 and (s.get(f"streamer{idx+1}") or "").strip() else st

        shared_args = []
        if top and src != "live":
            shared_args += ["--top", top]
        if prof_i:
            shared_args += ["--profile", prof_i]
        if lay_i:
            shared_args += ["--layout", lay_i]
        cam = (s.get("cam") or "").strip()
        if cam:
            shared_args += ["--cam", cam]
        qual = (s.get("quality") or "").strip()
        if qual == "2k":
            shared_args += ["--out-height", "2560", "--crf", "14", "--quality", "best"]
        elif qual == "1080p":
            shared_args += ["--out-height", "1920", "--crf", "16", "--quality", "best"]
        elif qual == "720p":
            shared_args += ["--out-height", "1280", "--crf", "18", "--quality", "720p"]

        label = extract_label(url, idx)
        if st_i:
            shared_args += ["--streamer", st_i]
        elif len(raw_urls) > 1:
            shared_args += ["--streamer", label]

        if mc:
            shared_args += ["--min-conf", mc]
        if mr:
            shared_args += ["--min-rating", mr]
        if rg and src != "live":
            shared_args += ["--range", rg]
        if cs:
            shared_args += ["--caption-style", cs]
        if sty:
            shared_args += ["--style", sty]
        if cat_i:
            shared_args += ["--category", cat_i]
        sd = (s.get("sync_dir") or "").strip()
        if sd:
            shared_args += ["--sync-dir", sd]
        if parts:
            shared_args += parts

        if stop_file is not None and len(raw_urls) == 1:
            sf = stop_file
        elif stop_file is not None:
            sf = stop_file.parent / f"{stop_file.stem}_{idx}{stop_file.suffix}"
        else:
            sf = here / f".stop_requested_{idx}"

        shared_args += ["--stop-file", str(sf)]
        jobs.append((label, target_args + shared_args, sf))

    return jobs, ""


def build_args(s: dict, stop_file: Path) -> tuple[list[str] | None, str]:
    jobs, err = build_jobs(s, stop_file.parent, stop_file)
    if not jobs:
        return None, err
    return jobs[0][1], ""


# runs clipwatcher.py as child process(es) - supports 1 to 4 streams
# --------------------------------------------------------------------------- #
class StreamWorker:
    def __init__(self, label: str, proc: subprocess.Popen, stop_file: Path, index: int, args: list[str]):
        self.label = label
        self.proc = proc
        self.stop_file = stop_file
        self.index = index
        self.args = args
        self.exit_code: int | None = None

    @property
    def running(self) -> bool:
        return self.proc.poll() is None


class Runner:
    def __init__(self, here: Path):
        self.here = here
        self.stop_file = here / ".stop_requested"
        self.workers: list[StreamWorker] = []
        self.lines: collections.deque = collections.deque(maxlen=4000)
        self.seq = 0
        self.clips = 0
        self.started = 0.0
        self.stopping = False
        self.lock = threading.Lock()

    @property
    def running(self) -> bool:
        return any(w.running for w in self.workers)

    def _add(self, text: str) -> None:
        text = text.rstrip()
        if not text:
            return
        with self.lock:
            self.seq += 1
            self.lines.append((self.seq, text))
            if " CLIP  " in text or " generated" in text.lower():
                self.clips += 1

    def start_multi(self, jobs: list[tuple[str, list[str], Path]]) -> tuple[bool, str]:
        if self.running:
            return False, "Engine is already clipping active streams."
        self.stop_file.unlink(missing_ok=True)
        for _, _, sf in jobs:
            sf.unlink(missing_ok=True)

        env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8", CLIPWATCHER_UI="1")
        for _c in (self.here / ".venv" / "Scripts", self.here / ".venv" / "bin",
                  Path(sys.executable).parent, Path(sys.prefix) / "Scripts"):
            if _c.exists():
                env["PATH"] = str(_c.resolve()) + os.pathsep + env.get("PATH", "")
        kw = {}
        if sys.platform == "win32":
            kw["creationflags"] = 0x08000000                       # no console window

        self.workers = []
        self.stopping = False
        self.clips = 0
        self.started = time.time()

        stream_names = [j[0] for j in jobs]
        self._add(f"--- Launching {len(jobs)} stream(s): {', '.join(stream_names)}")

        py_exe = str(self.here / ".venv" / "Scripts" / "python.exe")
        if not Path(py_exe).exists():
            py_exe = str(self.here / ".venv" / "bin" / "python")
        if not Path(py_exe).exists():
            py_exe = sys.executable

        for idx, (label, args, sf) in enumerate(jobs):
            try:
                proc = subprocess.Popen([py_exe, "-u", str(self.here / "clipwatcher.py"), *args],
                                         cwd=str(self.here), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                         stdin=subprocess.DEVNULL, env=env, **kw)
            except OSError as e:
                return False, f"Could not start [{label}]: {e}"
            worker = StreamWorker(label, proc, sf, idx, args)
            self.workers.append(worker)
            threading.Thread(target=self._pump, args=(worker, len(jobs) > 1), daemon=True).start()

        return True, ""

    def start(self, args: list[str]) -> tuple[bool, str]:
        return self.start_multi([("STREAM", args, self.stop_file)])

    def _pump(self, worker: StreamWorker, multi: bool) -> None:
        proc = worker.proc
        label_prefix = f"[{worker.label}] " if multi else ""
        buf = b""
        while True:
            chunk = proc.stdout.read1(4096) if hasattr(proc.stdout, "read1") else proc.stdout.read(1)
            if not chunk:
                break
            buf += chunk
            parts = re.split(rb"[\r\n]", buf)
            buf = parts.pop()
            for p in parts:
                line = p.decode("utf-8", "replace").strip()
                if line:
                    if line.startswith("@@STATUS "):
                        continue
                    self._add(f"{label_prefix}{line}")
        if buf:
            line = buf.decode("utf-8", "replace").strip()
            if line and not line.startswith("@@STATUS "):
                self._add(f"{label_prefix}{line}")
        worker.exit_code = proc.wait()
        self._add(f"--- [{worker.label}] finished (exit code {worker.exit_code})" if multi
                  else f"--- finished (exit code {worker.exit_code})")
        if not self.running:
            self.stopping = False

    def stop(self) -> None:
        if not self.running:
            return
        self.stopping = True
        try:
            self.stop_file.write_text("stop", encoding="utf-8")
        except OSError:
            pass
        for w in self.workers:
            try:
                w.stop_file.write_text("stop", encoding="utf-8")
            except OSError:
                pass

        def killer():
            for _ in range(180):
                if not self.running:
                    return
                time.sleep(1)
            for w in self.workers:
                if w.running:
                    w.proc.terminate()
        threading.Thread(target=killer, daemon=True).start()

    def log_since(self, since: int) -> dict:
        with self.lock:
            rows = [(n, t) for n, t in self.lines if n > since]
        return {"lines": [{"n": n, "t": t} for n, t in rows], "next": self.seq}

    def state(self) -> dict:
        active = [w.label for w in self.workers if w.running]
        status = f"{len(active)} stream(s) running: {', '.join(active)}" if active else "Idle"
        return {"running": self.running, "stopping": self.stopping and self.running, "status": status,
                "clips": self.clips, "started": self.started if self.running else 0,
                "active_streams": active, "stream_count": len(self.workers)}


def run_doctor(here: Path) -> str:
    py_exe = str(here / ".venv" / "Scripts" / "python.exe")
    if not Path(py_exe).exists():
        py_exe = str(here / ".venv" / "bin" / "python")
    if not Path(py_exe).exists():
        py_exe = sys.executable
    try:
        r = subprocess.run([py_exe, str(here / "clipwatcher.py"), "--doctor"], cwd=str(here),
                           capture_output=True, text=True, timeout=60, encoding="utf-8", errors="replace")
        return (r.stdout + r.stderr).strip()
    except (OSError, subprocess.SubprocessError) as e:
        return f"Could not run the check: {e}"


def open_folder(path: Path) -> bool:
    try:
        path.mkdir(exist_ok=True)
        if sys.platform == "win32":
            os.startfile(str(path))  # noqa: S606
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
        return True
    except OSError:
        return False


# --------------------------------------------------------------------------- #
# the page
# --------------------------------------------------------------------------- #
PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ClipWatcher AI Pro · Studio</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg-dark: #0a0c13;
  --bg-card: rgba(18, 22, 34, 0.75);
  --bg-card-hover: rgba(26, 32, 48, 0.85);
  --bg-input: #121622;
  --border: rgba(255, 255, 255, 0.08);
  --border-focus: rgba(99, 102, 241, 0.6);
  --primary: #6366f1;
  --primary-hover: #4f46e5;
  --primary-gradient: linear-gradient(135deg, #6366f1 0%, #8b5cf6 50%, #06b6d4 100%);
  --accent-cyan: #06b6d4;
  --accent-purple: #8b5cf6;
  --accent-gold: #f59e0b;
  --success: #10b981;
  --success-glow: rgba(16, 185, 129, 0.25);
  --danger: #ef4444;
  --text-main: #f8fafc;
  --text-muted: #94a3b8;
  --text-dim: #64748b;
  --log-bg: #07090e;
  --radius-lg: 16px;
  --radius-md: 12px;
  --radius-sm: 8px;
  --radius-full: 9999px;
  --shadow-glow: 0 0 25px rgba(99, 102, 241, 0.15);
  --shadow-card: 0 8px 32px rgba(0, 0, 0, 0.4);
}

* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background-color: var(--bg-dark);
  background-image: 
    radial-gradient(at 10% 10%, rgba(99, 102, 241, 0.07) 0px, transparent 50%),
    radial-gradient(at 90% 0%, rgba(139, 92, 246, 0.07) 0px, transparent 50%),
    radial-gradient(at 50% 90%, rgba(6, 182, 212, 0.05) 0px, transparent 50%);
  color: var(--text-main);
  font-family: 'Plus Jakarta Sans', system-ui, -apple-system, sans-serif;
  line-height: 1.5;
  min-height: 100vh;
  padding-bottom: 60px;
}

/* Custom Scrollbar */
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: var(--bg-dark); }
::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.15); border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.25); }

.wrap { max-width: 1240px; margin: 0 auto; padding: 0 20px; }

/* Top Header */
header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 24px 0 20px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 28px;
  flex-wrap: wrap;
  gap: 16px;
}

.brand {
  display: flex;
  align-items: center;
  gap: 14px;
}

.brand-icon {
  width: 42px;
  height: 42px;
  background: var(--primary-gradient);
  border-radius: var(--radius-md);
  display: flex;
  align-items: center;
  justify-content: center;
  box-shadow: 0 0 20px rgba(99, 102, 241, 0.35);
  color: #fff;
  font-size: 20px;
  font-weight: 800;
}

.brand-info h1 {
  font-size: 20px;
  font-weight: 800;
  letter-spacing: -0.5px;
  background: linear-gradient(135deg, #fff 40%, #cbd5e1 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

.brand-info .tagline {
  font-size: 12px;
  color: var(--text-muted);
  font-weight: 500;
  display: flex;
  align-items: center;
  gap: 6px;
}

.header-center {
  display: flex;
  align-items: center;
  gap: 12px;
}

/* System Status Pill */
.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 14px;
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: var(--radius-full);
  font-size: 13px;
  font-weight: 600;
  color: var(--text-muted);
  backdrop-filter: blur(8px);
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--text-dim);
}

.status-pill.run {
  border-color: rgba(16, 185, 129, 0.4);
  color: #34d399;
}
.status-pill.run .status-dot {
  background: var(--success);
  box-shadow: 0 0 10px var(--success);
  animation: pulse 1.5s infinite;
}

.status-pill.stop {
  border-color: rgba(245, 158, 11, 0.4);
  color: #fbbf24;
}
.status-pill.stop .status-dot {
  background: var(--accent-gold);
}

@keyframes pulse {
  0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
  70% { transform: scale(1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
  100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
}

/* Nav Tabs */
nav {
  display: flex;
  background: var(--bg-card);
  padding: 5px;
  border-radius: var(--radius-full);
  border: 1px solid var(--border);
  gap: 4px;
}

nav button {
  border: 0;
  background: transparent;
  color: var(--text-muted);
  font-family: inherit;
  font-size: 13.5px;
  font-weight: 600;
  padding: 8px 18px;
  border-radius: var(--radius-full);
  cursor: pointer;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  gap: 8px;
}

nav button:hover {
  color: #fff;
}

nav button.on {
  background: var(--primary-gradient);
  color: #fff;
  box-shadow: 0 2px 12px rgba(99, 102, 241, 0.35);
}

.badge {
  background: rgba(255, 255, 255, 0.2);
  padding: 1px 7px;
  border-radius: var(--radius-full);
  font-size: 11px;
  font-weight: 700;
}

/* Cards & Layout */
.card {
  background: var(--bg-card);
  backdrop-filter: blur(16px);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  padding: 24px;
  margin-bottom: 24px;
  box-shadow: var(--shadow-card);
  position: relative;
  overflow: hidden;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.card-title {
  font-size: 17px;
  font-weight: 700;
  color: #fff;
  display: flex;
  align-items: center;
  gap: 10px;
}

.grid {
  display: grid;
  gap: 16px;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
}

.grid-3 {
  display: grid;
  gap: 16px;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
}

/* Form Elements */
label {
  display: block;
  font-size: 13px;
  font-weight: 600;
  color: var(--text-muted);
  margin-bottom: 8px;
}

input, select, textarea {
  width: 100%;
  font-family: inherit;
  font-size: 14px;
  background: var(--bg-input);
  color: #fff;
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: 12px 14px;
  outline: none;
  transition: all 0.2s ease;
}

input:focus, select:focus, textarea:focus {
  border-color: var(--border-focus);
  box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.15);
}

input::placeholder { color: var(--text-dim); }

/* Source Switcher Buttons */
.source-pills {
  display: flex;
  gap: 10px;
  margin-bottom: 20px;
  flex-wrap: wrap;
}

.source-pill {
  flex: 1;
  min-width: 140px;
  background: var(--bg-input);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: 14px 16px;
  color: var(--text-muted);
  font-size: 13.5px;
  font-weight: 600;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 10px;
  transition: all 0.2s ease;
  user-select: none;
}

.source-pill:hover {
  background: var(--bg-card-hover);
  border-color: rgba(255,255,255,0.15);
  color: #fff;
}

.source-pill.active {
  background: rgba(99, 102, 241, 0.12);
  border-color: var(--primary);
  color: #fff;
  box-shadow: 0 0 15px rgba(99, 102, 241, 0.2);
}

.source-icon {
  font-size: 18px;
}

/* URL Input Area */
.url-box {
  position: relative;
  margin-bottom: 20px;
}

.url-input-wrap {
  position: relative;
  display: flex;
  align-items: center;
}

.url-input-wrap input {
  padding-left: 42px;
  font-size: 15px;
  padding-top: 14px;
  padding-bottom: 14px;
}

.url-icon {
  position: absolute;
  left: 14px;
  color: var(--text-muted);
  font-size: 18px;
}

/* Preset Badges */
.presets-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 10px;
  margin-bottom: 20px;
  flex-wrap: wrap;
}

.preset-chip {
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid var(--border);
  border-radius: var(--radius-full);
  padding: 5px 12px;
  font-size: 12px;
  font-weight: 600;
  color: var(--text-muted);
  cursor: pointer;
  transition: all 0.15s ease;
}

.preset-chip:hover {
  background: rgba(99, 102, 241, 0.2);
  border-color: var(--primary);
  color: #fff;
}

/* Buttons */
.btn {
  font-family: inherit;
  font-size: 14px;
  font-weight: 600;
  padding: 12px 20px;
  border-radius: var(--radius-md);
  border: 1px solid var(--border);
  background: var(--bg-card-hover);
  color: #fff;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  transition: all 0.2s ease;
}

.btn:hover:not(:disabled) {
  background: rgba(255, 255, 255, 0.12);
  border-color: rgba(255, 255, 255, 0.2);
}

.btn.primary {
  background: var(--primary-gradient);
  border: none;
  box-shadow: 0 4px 18px rgba(99, 102, 241, 0.35);
  font-size: 15px;
  font-weight: 700;
  padding: 14px 26px;
}

.btn.primary:hover:not(:disabled) {
  box-shadow: 0 6px 24px rgba(99, 102, 241, 0.5);
  transform: translateY(-1px);
}

.btn.danger {
  background: rgba(239, 68, 68, 0.12);
  color: #f87171;
  border-color: rgba(239, 68, 68, 0.3);
}

.btn.danger:hover:not(:disabled) {
  background: rgba(239, 68, 68, 0.22);
  border-color: var(--danger);
  color: #fff;
}

.btn:disabled {
  opacity: 0.45;
  cursor: not-allowed;
  transform: none !important;
}

/* More Options Collapsible */
details {
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: 14px 18px;
  background: rgba(14, 18, 28, 0.4);
  margin-top: 14px;
}

details summary {
  font-size: 13.5px;
  font-weight: 700;
  color: var(--text-muted);
  cursor: pointer;
  outline: none;
  user-select: none;
}

details summary:hover {
  color: #fff;
}

/* Live Studio Terminal */
.terminal-card {
  background: var(--log-bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  overflow: hidden;
  box-shadow: var(--shadow-card);
}

.terminal-header {
  background: #0d1017;
  padding: 12px 18px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--border);
}

.mac-dots {
  display: flex;
  gap: 7px;
}

.mac-dot {
  width: 11px;
  height: 11px;
  border-radius: 50%;
}
.mac-dot.red { background: #ff5f56; }
.mac-dot.yellow { background: #ffbd2e; }
.mac-dot.green { background: #27c93f; }

.terminal-title {
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  font-weight: 600;
  color: var(--text-dim);
  letter-spacing: 0.5px;
}

#log {
  background: var(--log-bg);
  color: #e2e8f0;
  font-family: 'JetBrains Mono', monospace;
  font-size: 12.5px;
  line-height: 1.6;
  padding: 16px 20px;
  height: 380px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-word;
}

#log .clip {
  color: #34d399;
  font-weight: 700;
  background: rgba(16, 185, 129, 0.1);
  padding: 2px 6px;
  border-radius: 4px;
  display: inline-block;
  margin: 2px 0;
}

#log .title {
  color: #fbbf24;
  font-weight: 600;
}

#log .err {
  color: #f87171;
  background: rgba(239, 68, 68, 0.1);
  padding: 2px 6px;
  border-radius: 4px;
}

#log .sys {
  color: #38bdf8;
}

/* Clips Gallery Section */
.gallery-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 24px;
  flex-wrap: wrap;
}

.filter-group {
  display: flex;
  background: var(--bg-card);
  padding: 4px;
  border-radius: var(--radius-full);
  border: 1px solid var(--border);
  gap: 4px;
}

.filter-btn {
  background: transparent;
  border: none;
  color: var(--text-muted);
  font-family: inherit;
  font-size: 13px;
  font-weight: 600;
  padding: 6px 14px;
  border-radius: var(--radius-full);
  cursor: pointer;
  transition: all 0.15s ease;
}

.filter-btn.active {
  background: rgba(99, 102, 241, 0.2);
  color: #fff;
  border: 1px solid var(--primary);
}

#grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 20px;
}

.clip-card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  overflow: hidden;
  display: flex;
  flex-direction: column;
  transition: all 0.25s cubic-bezier(0.16, 1, 0.3, 1);
  position: relative;
}

.clip-card:hover {
  transform: translateY(-4px);
  border-color: rgba(99, 102, 241, 0.4);
  box-shadow: 0 12px 30px rgba(0, 0, 0, 0.5);
}

.clip-card.kept {
  border-color: rgba(16, 185, 129, 0.5);
  box-shadow: 0 0 20px rgba(16, 185, 129, 0.15);
}

.video-wrap {
  position: relative;
  width: 100%;
  aspect-ratio: 9/16;
  background: #000;
}

.video-wrap video {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}

.score-badge {
  position: absolute;
  top: 12px;
  left: 12px;
  background: rgba(16, 185, 129, 0.9);
  backdrop-filter: blur(8px);
  color: #fff;
  font-size: 11.5px;
  font-weight: 800;
  padding: 4px 10px;
  border-radius: var(--radius-full);
  display: flex;
  align-items: center;
  gap: 4px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.4);
}

.duration-badge {
  position: absolute;
  top: 12px;
  right: 12px;
  background: rgba(0, 0, 0, 0.7);
  backdrop-filter: blur(8px);
  color: #e2e8f0;
  font-size: 11.5px;
  font-weight: 700;
  padding: 4px 8px;
  border-radius: var(--radius-full);
}

.clip-content {
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 10px;
  flex: 1;
}

.clip-title {
  font-size: 15px;
  font-weight: 700;
  color: #fff;
  line-height: 1.35;
}

.clip-meta {
  font-size: 12px;
  color: var(--text-dim);
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.signal-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.signal-chip {
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid var(--border);
  padding: 2px 8px;
  border-radius: var(--radius-full);
  font-size: 11px;
  font-weight: 600;
  color: var(--text-muted);
}

.clip-actions {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
  margin-top: auto;
  padding-top: 8px;
}

.clip-actions button {
  width: 100%;
  font-size: 13px;
  padding: 8px;
}

.copy-post-btn {
  grid-column: span 2;
  font-size: 12.5px;
  padding: 7px;
  background: rgba(255, 255, 255, 0.04);
}

/* System Setup Doctor Box */
pre.doc {
  background: var(--log-bg);
  border: 1px solid var(--border);
  color: #cbd5e1;
  padding: 16px;
  border-radius: var(--radius-md);
  font-family: 'JetBrains Mono', monospace;
  font-size: 13px;
  line-height: 1.5;
  white-space: pre-wrap;
  margin-top: 14px;
}

.hide { display: none !important; }
.empty {
  grid-column: 1 / -1;
  text-align: center;
  padding: 60px 20px;
  color: var(--text-dim);
  font-size: 15px;
}
</style>
</head>
<body>
<div class="wrap">

<header>
  <div class="brand">
    <div class="brand-icon">⚡</div>
    <div class="brand-info">
      <h1>CLIPWATCHER PRO</h1>
      <div class="tagline">
        <span>AI Viral Moments & 2K Shorts Studio</span>
        <span>•</span>
        <span style="color:var(--accent-cyan); font-weight:700;">Intel QSV Accelerated</span>
      </div>
    </div>
  </div>

  <div class="header-center">
    <div id="pill" class="status-pill">
      <span class="status-dot"></span>
      <span id="pilltxt">Engine Idle</span>
    </div>
  </div>

  <nav>
    <button data-t="run" class="on"><span>⚡</span> Create & Clip</button>
    <button data-t="clips"><span>🎬</span> Clips Library <span id="clipcountbadge" class="badge">0</span></button>
    <button data-t="setup"><span>⚙️</span> Setup & AI</button>
  </nav>
</header>

<!-- TAB 1: RUN / CREATE -->
<section id="t-run">
  <div class="card">
    <div class="card-header">
      <div class="card-title"><span>🎯</span> Stream / Video Target</div>
      <div id="live" style="font-size:13px; color:var(--accent-cyan); font-weight:600;"></div>
    </div>

    <!-- Interactive Source Switcher -->
    <div class="source-pills">
      <div class="source-pill active" data-src="live">
        <span class="source-icon">🔴</span>
        <div>
          <div>Live Stream</div>
          <div style="font-size:11px; color:var(--text-dim)">Twitch / YouTube Live</div>
        </div>
      </div>
      <div class="source-pill" data-src="vod">
        <span class="source-icon">🎬</span>
        <div>
          <div>Past Video / VOD</div>
          <div style="font-size:11px; color:var(--text-dim)">YouTube / Twitch Past Stream</div>
        </div>
      </div>
      <div class="source-pill" data-src="file">
        <span class="source-icon">📁</span>
        <div>
          <div>Local Video File</div>
          <div style="font-size:11px; color:var(--text-dim)">OBS Recording / MP4 / MKV</div>
        </div>
      </div>
    </div>
    <!-- Hidden native select synced with pills -->
    <select id="source" class="hide">
      <option value="live">Live</option>
      <option value="vod">VOD</option>
      <option value="file">File</option>
    </select>

    <!-- Target URL / Path Input (Channel 1) -->
    <div class="url-box">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
        <label for="url" id="urllabel" style="margin-bottom:0;">Channel / Stream 1 URL</label>
        <span style="font-size:11.5px; color:var(--text-dim);">Paste link, @handle, or comma-separated list</span>
      </div>
      <div class="url-input-wrap">
        <span class="url-icon">🔗</span>
        <input id="url" placeholder="https://www.youtube.com/watch?v=... or Twitch channel like shroud" autocomplete="off">
      </div>
    </div>

    <!-- Extra Channels (Multi-Stream) -->
    <div id="channel2_box" class="url-box hide" style="margin-top:14px; background:rgba(99,102,241,0.04); border:1px solid rgba(99,102,241,0.25); border-radius:10px; padding:12px;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
        <label for="url2" style="margin-bottom:0; color:#818cf8; font-weight:700;">Channel / Stream 2 URL (Simultaneous)</label>
        <button type="button" class="btn" style="padding:2px 8px; font-size:11px;" onclick="removeChannel(2)">✕ Remove</button>
      </div>
      <div class="url-input-wrap">
        <span class="url-icon">🔗</span>
        <input id="url2" placeholder="https://www.youtube.com/watch?v=... or Twitch" autocomplete="off">
      </div>
      <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(140px, 1fr)); gap:10px; margin-top:10px;">
        <div>
          <label for="profile2" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Stream 2 Profile</label>
          <select id="profile2" style="padding:5px 8px; font-size:12px;">
            <option value="">(Inherit from Main)</option>
            <option value="podcast">Podcast / 2-Person</option>
            <option value="gaming">Gaming</option>
            <option value="cricket">Cricket</option>
            <option value="movies">Movies</option>
            <option value="highlights">Highlights</option>
          </select>
        </div>
        <div>
          <label for="layout2" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Stream 2 Layout</label>
          <select id="layout2" style="padding:5px 8px; font-size:12px;">
            <option value="">(Inherit from Main)</option>
            <option value="podcast">🎙️ Dual-Box Podcast Stack</option>
            <option value="split">Split Facecam</option>
            <option value="crop">Full Vertical 9:16</option>
            <option value="blur">Blur</option>
          </select>
        </div>
        <div>
          <label for="category2" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Drive Subfolder</label>
          <select id="category2" style="padding:5px 8px; font-size:12px;">
            <option value="">(Inherit / Auto)</option>
            <option value="Cricket">Cricket</option>
            <option value="Gaming">Gaming</option>
            <option value="Movies">Movies</option>
          </select>
        </div>
        <div>
          <label for="streamer2" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Streamer / Guest Name</label>
          <input id="streamer2" placeholder="e.g. Kai, Rogan, Vinay" style="padding:5px 8px; font-size:12px;">
        </div>
      </div>
    </div>

    <div id="channel3_box" class="url-box hide" style="margin-top:14px; background:rgba(99,102,241,0.04); border:1px solid rgba(99,102,241,0.25); border-radius:10px; padding:12px;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
        <label for="url3" style="margin-bottom:0; color:#818cf8; font-weight:700;">Channel / Stream 3 URL</label>
        <button type="button" class="btn" style="padding:2px 8px; font-size:11px;" onclick="removeChannel(3)">✕ Remove</button>
      </div>
      <div class="url-input-wrap">
        <span class="url-icon">🔗</span>
        <input id="url3" placeholder="https://twitch.tv/... or YouTube live link" autocomplete="off">
      </div>
      <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(140px, 1fr)); gap:10px; margin-top:10px;">
        <div>
          <label for="profile3" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Stream 3 Profile</label>
          <select id="profile3" style="padding:5px 8px; font-size:12px;">
            <option value="">(Inherit from Main)</option>
            <option value="podcast">Podcast / 2-Person</option>
            <option value="gaming">Gaming</option>
            <option value="cricket">Cricket</option>
            <option value="movies">Movies</option>
            <option value="highlights">Highlights</option>
          </select>
        </div>
        <div>
          <label for="layout3" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Stream 3 Layout</label>
          <select id="layout3" style="padding:5px 8px; font-size:12px;">
            <option value="">(Inherit from Main)</option>
            <option value="podcast">🎙️ Dual-Box Podcast Stack</option>
            <option value="split">Split Facecam</option>
            <option value="crop">Full Vertical 9:16</option>
            <option value="blur">Blur</option>
          </select>
        </div>
        <div>
          <label for="category3" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Drive Subfolder</label>
          <select id="category3" style="padding:5px 8px; font-size:12px;">
            <option value="">(Inherit / Auto)</option>
            <option value="Cricket">Cricket</option>
            <option value="Gaming">Gaming</option>
            <option value="Movies">Movies</option>
          </select>
        </div>
        <div>
          <label for="streamer3" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Streamer / Guest Name</label>
          <input id="streamer3" placeholder="e.g. Kai, Rogan, Vinay" style="padding:5px 8px; font-size:12px;">
        </div>
      </div>
    </div>

    <div id="channel4_box" class="url-box hide" style="margin-top:14px; background:rgba(99,102,241,0.04); border:1px solid rgba(99,102,241,0.25); border-radius:10px; padding:12px;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
        <label for="url4" style="margin-bottom:0; color:#818cf8; font-weight:700;">Channel / Stream 4 URL</label>
        <button type="button" class="btn" style="padding:2px 8px; font-size:11px;" onclick="removeChannel(4)">✕ Remove</button>
      </div>
      <div class="url-input-wrap">
        <span class="url-icon">🔗</span>
        <input id="url4" placeholder="https://twitch.tv/... or YouTube live link" autocomplete="off">
      </div>
      <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(140px, 1fr)); gap:10px; margin-top:10px;">
        <div>
          <label for="profile4" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Stream 4 Profile</label>
          <select id="profile4" style="padding:5px 8px; font-size:12px;">
            <option value="">(Inherit from Main)</option>
            <option value="podcast">Podcast / 2-Person</option>
            <option value="gaming">Gaming</option>
            <option value="cricket">Cricket</option>
            <option value="movies">Movies</option>
            <option value="highlights">Highlights</option>
          </select>
        </div>
        <div>
          <label for="layout4" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Stream 4 Layout</label>
          <select id="layout4" style="padding:5px 8px; font-size:12px;">
            <option value="">(Inherit from Main)</option>
            <option value="podcast">🎙️ Dual-Box Podcast Stack</option>
            <option value="split">Split Facecam</option>
            <option value="crop">Full Vertical 9:16</option>
            <option value="blur">Blur</option>
          </select>
        </div>
        <div>
          <label for="category4" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Drive Subfolder</label>
          <select id="category4" style="padding:5px 8px; font-size:12px;">
            <option value="">(Inherit / Auto)</option>
            <option value="Cricket">Cricket</option>
            <option value="Gaming">Gaming</option>
            <option value="Movies">Movies</option>
          </select>
        </div>
        <div>
          <label for="streamer4" style="font-size:11px; margin-bottom:3px; color:var(--text-muted);">Streamer / Guest Name</label>
          <input id="streamer4" placeholder="e.g. Kai, Rogan, Vinay" style="padding:5px 8px; font-size:12px;">
        </div>
      </div>
    </div>

    <div style="margin-bottom:18px;">
      <button type="button" class="btn" id="add_channel_btn" onclick="addNextChannel()" style="font-size:12.5px; padding:7px 15px; background:rgba(99,102,241,0.12); border-color:rgba(99,102,241,0.35); color:#818cf8;">
        <span>＋</span> Add Another Channel (Clip 2+ Streams Simultaneously)
      </button>
    </div>

    <!-- Quick Presets -->
    <div class="presets-bar">
      <span style="font-size:12px; font-weight:700; color:var(--text-dim);">QUICK PRESETS:</span>
      <div class="preset-chip" onclick="applyPreset('cricket')">🏏 Cricket / Sports Highlights</div>
      <div class="preset-chip" onclick="applyPreset('podcast')">🎙️ Dual Podcast Stack (2-Person)</div>
      <div class="preset-chip" onclick="applyPreset('gaming_split')">🎮 Gaming Split (2K 60fps)</div>
      <div class="preset-chip" onclick="applyPreset('just_chatting')">🎙️ Just Chatting Solo</div>
      <div class="preset-chip" onclick="applyPreset('irl_walk')">🚶 IRL Full-Frame</div>
      <div class="preset-chip" onclick="applyPreset('viral_strict')">🔥 Viral Climax Only</div>
    </div>

    <!-- Dynamic Options (Top clips & Range) -->
    <div class="grid" style="margin-bottom:16px;">
      <div id="topbox" class="hide">
        <label for="top">How many clips (best moments first)</label>
        <input id="top" inputmode="numeric" placeholder="10">
      </div>
      <div id="rangebox" class="hide">
        <label for="range">Time window (optional)</label>
        <input id="range" placeholder="e.g. 1:00:00-3:30:00">
      </div>
    </div>

    <!-- Primary Studio Settings -->
    <div class="grid" style="grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));">
      <div>
        <label for="profile">Kind of Stream Profile</label>
        <select id="profile">
          <option value="podcast">Podcast / Interview (Two-Person Talk)</option>
          <option value="gaming">Gaming (face cam + gameplay)</option>
          <option value="cricket">Cricket / Sports (wickets, sixes, crowd roar)</option>
          <option value="movies">Movies & Cinema (Action scenes, dialogue, fight)</option>
          <option value="just-chatting">Just chatting</option>
          <option value="irl">IRL / walking</option>
          <option value="highlights">Only the best (strict)</option>
          <option value="">No preset</option>
        </select>
      </div>

      <div>
        <label for="category">Drive Subfolder / Category</label>
        <select id="category">
          <option value="auto">⚡ Auto (Match Profile: Cricket, Gaming, Movies)</option>
          <option value="cricket">🏏 Cricket (Drive /cricket)</option>
          <option value="gaming">🎮 Gaming (Drive /gaming)</option>
          <option value="movies">🎬 Movies (Drive /movies)</option>
        </select>
      </div>

      <div>
        <label for="layout">Layout Style</label>
        <select id="layout">
          <option value="crop">Full Vertical 9:16 (Fills Entire Screen · Best for Shorts & Reels)</option>
          <option value="podcast">🎙️ Dual-Box Podcast Stack (Speaker A Top + Speaker B Bottom)</option>
          <option value="split">Split: Facecam on Top, Action Below (Shorts / Streamers)</option>
          <option value="blur">Blurred Background Frame (16:9 Center - Not Full Screen)</option>
        </select>
      </div>

      <div>
        <label for="quality">Output Clarity & Resolution</label>
        <select id="quality">
          <option value="2k">2K Ultra QHD (1440x2560 - Studio Quality)</option>
          <option value="1080p">1080p Full HD (1080x1920 - High Bitrate)</option>
          <option value="">Standard (1080p)</option>
        </select>
      </div>
    </div>

    <!-- Advanced Collapsible Settings -->
    <details>
      <summary>⚙️ Studio Details (Camera Position, Captions, AI Scoring)</summary>
      <div class="grid-3" style="margin-top:16px;">
        <div>
          <label for="cam">Camera Position (for Split)</label>
          <select id="cam">
            <option value="bottom-left-wide">Bottom Left - Wide (2 Streamers / MrBeast)</option>
            <option value="bottom-left">Bottom Left (Single Facecam)</option>
            <option value="mid-left">Middle Left (Shroud / Standard Gaming)</option>
            <option value="bottom-right-wide">Bottom Right - Wide (2 Streamers)</option>
            <option value="bottom-right">Bottom Right</option>
            <option value="top-left">Top Left</option>
            <option value="top-right">Top Right</option>
            <option value="auto">Auto-detect facecam</option>
          </select>
        </div>

        <div>
          <label for="style">Captions Look & Feel</label>
          <select id="style">
            <option value="bold">Bold (Neon yellow glow, high impact)</option>
            <option value="classic">Classic (Yellow highlight)</option>
            <option value="neon">Cyberpunk Neon (Pink hook, cyan glow)</option>
            <option value="boxed">Boxed (TikTok style black badge)</option>
            <option value="minimal">Minimalist Clean</option>
          </select>
        </div>

        <div>
          <label for="caption_style">Caption Animation</label>
          <select id="caption_style">
            <option value="words">Word-by-word active glow (Karaoke)</option>
            <option value="lines">Plain lines</option>
          </select>
        </div>

        <div>
          <label for="streamer">Streamer Name (for AI Titles)</label>
          <input id="streamer" placeholder="e.g. Speed, Shroud, Vinay">
        </div>

        <div>
          <label for="min_conf">Detection Confidence</label>
          <select id="min_conf">
            <option value="">Default (Medium)</option>
            <option value="low">Low (Captures more moments)</option>
            <option value="med">Medium (Balanced)</option>
            <option value="high">High (Only clearest spikes)</option>
          </select>
        </div>

        <div>
          <label for="min_rating">AI Rating Filter (0-10)</label>
          <input id="min_rating" inputmode="numeric" placeholder="5">
        </div>
      </div>

      <div style="margin-top:14px;">
        <label for="extra">Advanced CLI Flags</label>
        <input id="extra" placeholder="e.g. --av-offset -200 --audio-db 8">
      </div>
    </details>

    <!-- Action Buttons -->
    <div style="display:flex; align-items:center; gap:14px; margin-top:24px; flex-wrap:wrap;">
      <button class="btn primary" id="start">
        <span>⚡</span> Start Clipping Moments
      </button>
      <button class="btn danger" id="stop" disabled>
        <span>⏹</span> Stop Engine
      </button>
      <span id="msg" style="font-size:13.5px; font-weight:600; color:#f87171;"></span>
    </div>
  </div>

  <!-- Terminal Card -->
  <div class="terminal-card">
    <div class="terminal-header">
      <div class="mac-dots">
        <span class="mac-dot red"></span>
        <span class="mac-dot yellow"></span>
        <span class="mac-dot green"></span>
      </div>
      <div class="terminal-title">LIVE STUDIO CONSOLE</div>
      <div id="stream_filter_bar" style="display:flex; gap:6px; margin-left:auto; margin-right:12px;"></div>
      <div style="display:flex; gap:8px;">
        <button class="btn" style="padding:4px 10px; font-size:11px;" onclick="$('#log').textContent=''">Clear</button>
      </div>
    </div>
    <div id="log" aria-live="polite"></div>
  </div>
</section>

<!-- TAB 2: CLIPS GALLERY -->
<section id="t-clips" class="hide">
  <div class="gallery-bar">
    <div id="bar" style="display:flex; align-items:center; gap:12px; flex-wrap:wrap;"></div>
  </div>
  <div id="tips"></div>
  <div id="grid"></div>
</section>

<!-- TAB 3: SYSTEM SETUP & AI -->
<section id="t-setup" class="hide">
  <div class="card">
    <div class="card-title" style="margin-bottom:12px;"><span>🤖</span> AI Hooks & Title Keys</div>
    <p style="font-size:13.5px; color:var(--text-muted); margin-bottom:18px;">
      Free Google Gemini API keys enable automated hook titles, virality rating, and smart segment pruning. 
      Keys are saved locally in your <code>.env</code> file only.
    </p>

    <div class="grid">
      <div>
        <label for="k_gemini">
          Google Gemini API Key 
          <span id="s_gemini" style="color:var(--success); font-weight:700;"></span>
          <a href="https://aistudio.google.com/apikey" target="_blank" style="color:var(--accent-cyan); float:right; text-decoration:none;">Get Free Key ↗</a>
        </label>
        <input id="k_gemini" type="password" autocomplete="off" placeholder="AIzaSy...">
      </div>

      <div>
        <label for="k_anthropic">
          Anthropic Claude Key (Optional) 
          <span id="s_anthropic" style="color:var(--success); font-weight:700;"></span>
        </label>
        <input id="k_anthropic" type="password" autocomplete="off" placeholder="sk-ant-...">
      </div>
    </div>

    <div style="display:flex; gap:10px; margin-top:16px;">
      <button class="btn primary" id="savekeys">Save API Keys</button>
      <button class="btn" id="clearkeys">Remove Keys</button>
      <span id="kmsg" style="font-size:13px; color:var(--accent-cyan); align-self:center;"></span>
    </div>
  </div>

  <div class="card">
    <div class="card-title" style="margin-bottom:12px;"><span>☁️</span> Google Drive & Cloud Auto-Sync</div>
    <p style="font-size:13.5px; color:var(--text-muted); margin-bottom:14px;">
      Automatically mirrors your highest-resolution vertical clips, thumbnails, subtitles, and viral social post texts directly to your shared Google Drive or OneDrive subfolders (<code>/cricket</code>, <code>/gaming</code>, <code>/movies</code>).
    </p>
    <div style="background:rgba(59,130,246,0.08); border:1px solid rgba(59,130,246,0.25); border-radius:8px; padding:12px 14px; margin-bottom:14px; font-size:13px; color:#cbd5e1; line-height:1.5;">
      💡 <b>Important:</b> Windows saves files to a <b>local folder path</b> (e.g. <code>G:\My Drive\clip</code> or <code>C:\Users\vinay\OneDrive\clip</code>), which cloud software automatically uploads to Google Drive. Do not paste <code>https://drive.google.com/...</code> browser URLs.
      <br><span style="margin-top:4px; display:inline-block;">• <b>Google Drive:</b> Install <a href="https://www.google.com/drive/download/" target="_blank" style="color:var(--accent-cyan); font-weight:600;">Google Drive for Desktop (Free) ↗</a> so it appears as <b>G:\My Drive</b> on your PC.</span>
      <br>• <b>OneDrive:</b> Already active on your PC at <code>C:\Users\vinay\OneDrive</code>.
    </div>
    <div>
      <label for="sync_dir">
        Local Cloud Folder Path:
        <span style="font-size:12px; color:var(--text-muted); font-weight:normal; margin-left:8px;">
          (e.g. <code>G:\My Drive\clip</code> or <code>C:\Users\vinay\OneDrive\clip</code>)
        </span>
      </label>
      <div style="display:flex; gap:10px; align-items:center;">
        <input id="sync_dir" type="text" placeholder="G:\My Drive\clip" style="flex:1;">
        <button class="btn" style="white-space:nowrap;" onclick="testDriveSync()">Test & Sync Now</button>
      </div>
    </div>
    <div style="display:flex; gap:8px; margin-top:10px; flex-wrap:wrap;">
      <button class="btn" style="padding:4px 10px; font-size:12px;" onclick="setSyncPreset('gdrive')">Use Google Drive (G:\My Drive\clip)</button>
      <button class="btn" style="padding:4px 10px; font-size:12px;" onclick="setSyncPreset('onedrive')">Use OneDrive (Active on PC)</button>
      <button class="btn" style="padding:4px 10px; font-size:12px;" onclick="setSyncPreset('desktop')">Use Desktop Folder</button>
    </div>
    <span id="sync_status_msg" style="display:block; margin-top:10px; font-size:13px; font-weight:600;"></span>
  </div>

  <div class="card">
    <div class="card-header" style="margin-bottom:12px;">
      <div class="card-title"><span>🩺</span> System Engine Diagnostic</div>
      <button class="btn" id="doctor">Run Health Check</button>
    </div>
    <p style="font-size:13px; color:var(--text-muted); margin-bottom:12px;">
      Verifies FFmpeg, Intel QuickSync/NVIDIA GPU acceleration, yt-dlp, Streamlink, and Whisper speech models.
    </p>
    <pre class="doc" id="doc">Press "Run Health Check" to inspect installed tools and hardware encoders.</pre>
  </div>
</section>

</div>

<script>
const TOKEN = "__TOKEN__";
const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));

async function api(path, body) {
  const res = await fetch(path, body !== undefined ? {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Token": TOKEN },
    body: JSON.stringify(body)
  } : {
    headers: { "X-Token": TOKEN }
  });
  return res.json();
}

const F = ["source","top","range","url","url2","url3","url4","profile","category","layout","cam","quality","streamer","min_conf","min_rating","caption_style","style","extra","sync_dir",
           "profile2","layout2","category2","streamer2","profile3","layout3","category3","streamer3","profile4","layout4","category4","streamer4"];

function tab(t) {
  $$("section").forEach(s => s.classList.add("hide"));
  $("#t-" + t).classList.remove("hide");
  $$("nav button").forEach(b => b.classList.toggle("on", b.dataset.t == t));
  location.hash = t;
  if (t == "clips") loadClips();
  if (t == "setup") loadKeys();
}

$$("nav button").forEach(b => b.onclick = () => tab(b.dataset.t));

let since = 0, running = false, activeStreamFilter = "ALL";

function addNextChannel() {
  for (let i = 2; i <= 4; i++) {
    const box = $("#channel" + i + "_box");
    if (box && box.classList.contains("hide")) {
      box.classList.remove("hide");
      $("#url" + i).focus();
      break;
    }
  }
}

function removeChannel(n) {
  const box = $("#channel" + n + "_box");
  if (box) {
    box.classList.add("hide");
    $("#url" + n).value = "";
    if ($("#profile" + n)) $("#profile" + n).value = "";
    if ($("#layout" + n)) $("#layout" + n).value = "";
    if ($("#category" + n)) $("#category" + n).value = "";
    if ($("#streamer" + n)) $("#streamer" + n).value = "";
    api("/api/settings", form());
  }
}

function syncChannelVisibility() {
  for (let i = 2; i <= 4; i++) {
    const el = $("#url" + i);
    if (el && el.value.trim()) {
      $("#channel" + i + "_box").classList.remove("hide");
    }
  }
}

function filterConsole() {
  const lg = $("#log");
  const children = Array.from(lg.children);
  children.forEach(c => {
    if (activeStreamFilter === "ALL") {
      c.style.display = "";
    } else {
      const txt = c.textContent || "";
      const matches = txt.includes("[" + activeStreamFilter + "]");
      c.style.display = matches || txt.startsWith("---") ? "" : "none";
    }
  });
}


// Source selection UI synchronization
function setSource(v) {
  $("#source").value = v;
  $$(".source-pill").forEach(p => p.classList.toggle("active", p.dataset.src == v));
  srcUI();
  api("/api/settings", form());
}

$$(".source-pill").forEach(p => {
  p.onclick = () => setSource(p.dataset.src);
});

function srcUI() {
  const v = $("#source").value;
  const u = $("#url");
  $("#topbox").classList.toggle("hide", v == "live");
  $("#rangebox").classList.toggle("hide", v == "live");
  
  if (v == "live") {
    $("#urllabel").textContent = "Stream Link or Twitch Channel";
    u.placeholder = "https://www.youtube.com/watch?v=... or Twitch channel like shroud";
    $("#start").innerHTML = "<span>⚡</span> Start Live Clipping";
  } else if (v == "vod") {
    $("#urllabel").textContent = "YouTube Video or Past Stream Link (VOD)";
    u.placeholder = "https://www.youtube.com/watch?v=... or twitch.tv/videos/...";
    $("#start").innerHTML = "<span>⚡</span> Find Viral Moments";
  } else {
    $("#urllabel").textContent = "Local Video File Path";
    u.placeholder = "C:\\Videos\\recording.mp4 (Windows: Shift + right-click the file, 'Copy as path')";
    $("#start").innerHTML = "<span>⚡</span> Process Video File";
  }
}

$("#source").addEventListener("change", () => {
  setSource($("#source").value);
});

// Quick Presets
function applyPreset(name) {
  if (name === "cricket") {
    $("#profile").value = "cricket";
    $("#layout").value = "crop";
    $("#quality").value = "2k";
    $("#caption_style").value = "words";
    $("#style").value = "bold";
  } else if (name === "podcast") {
    $("#profile").value = "podcast";
    $("#layout").value = "podcast";
    $("#quality").value = "2k";
    $("#caption_style").value = "words";
    $("#style").value = "bold";
  } else if (name === "gaming_split") {
    $("#profile").value = "gaming";
    $("#layout").value = "split";
    $("#cam").value = "mid-left";
    $("#quality").value = "2k";
    $("#caption_style").value = "words";
    $("#style").value = "bold";
  } else if (name === "just_chatting") {
    $("#profile").value = "just-chatting";
    $("#layout").value = "crop";
    $("#quality").value = "2k";
    $("#caption_style").value = "words";
    $("#style").value = "classic";
  } else if (name === "irl_walk") {
    $("#profile").value = "irl";
    $("#layout").value = "crop";
    $("#quality").value = "2k";
    $("#caption_style").value = "words";
    $("#style").value = "boxed";
  } else if (name === "viral_strict") {
    $("#profile").value = "highlights";
    $("#layout").value = "crop";
    $("#min_conf").value = "high";
    $("#min_rating").value = "7";
    $("#quality").value = "2k";
  }
  api("/api/settings", form());
}

function form() {
  const o = {};
  F.forEach(k => o[k] = $("#" + k).value);
  return o;
}

async function init() {
  const s = await api("/api/state");
  F.forEach(k => {
    if (s.settings[k] !== undefined && $("#" + k)) {
      $("#" + k).value = s.settings[k];
    }
  });
  setSource($("#source").value || "live"); syncChannelVisibility();
  poll();
  setInterval(poll, 1000);
  const h = location.hash.slice(1);
  if (["clips", "setup"].includes(h)) tab(h);
}

function cls(t) {
  if (/ CLIP /.test(t)) return "clip";
  if (/ title: /.test(t)) return "title";
  if (/error|failed|Traceback|Missing/i.test(t)) return "err";
  if (t.startsWith("---") || /clip-watcher \d/i.test(t)) return "sys";
  return "";
}

async function poll() {
  const d = await api("/api/log?since=" + since);
  const lg = $("#log");
  const stick = lg.scrollHeight - lg.scrollTop - lg.clientHeight < 70;
  d.lines.forEach(l => {
    const e = document.createElement("div");
    e.className = cls(l.t);
    e.textContent = l.t;
    lg.appendChild(e);
    since = l.n;
  });
  while (lg.children.length > 2000) lg.removeChild(lg.firstChild);
  if (stick) lg.scrollTop = lg.scrollHeight;

  const st = d.state;
  running = st.running;
  $("#start").disabled = running;
  $("#stop").disabled = !running || st.stopping;

  const p = $("#pill");
  p.className = "status-pill " + (st.stopping ? "stop" : running ? "run" : "");
  $("#pilltxt").textContent = st.stopping ? "Finishing Final Clips..." : running ? "Clipping Live Stream" : "Engine Idle";
  
  $("#live").textContent = running 
    ? (st.status || "processing...") + " · " + (st.clips || 0) + " clips generated"
    : (st.exit_code !== null && st.exit_code !== undefined ? "Last session finished" : "");

  // Update dynamic channel filter pills in terminal header
  const bar = $("#stream_filter_bar");
  if (bar && st.active_streams && st.active_streams.length > 1) {
    bar.innerHTML = `<button class="filter-btn ${activeStreamFilter=='ALL'?'active':''}" style="padding:2px 8px; font-size:11px;" onclick="activeStreamFilter='ALL';filterConsole()">All Streams (${st.active_streams.length})</button>` +
      st.active_streams.map(s => `<button class="filter-btn ${activeStreamFilter==s?'active':''}" style="padding:2px 8px; font-size:11px;" onclick="activeStreamFilter='${s}';filterConsole()">${s}</button>`).join("");
  } else if (bar) {
    bar.innerHTML = "";
  }
}

$("#start").onclick = async () => {
  $("#msg").textContent = "";
  const r = await api("/api/start", form());
  if (!r.ok) {
    $("#msg").textContent = r.error || "Could not start";
  } else {
    $("#log").textContent = "";
    since = 0;
    poll();
  }
};

$("#stop").onclick = async () => {
  await api("/api/stop", {});
  poll();
};

// Clips Management
let CLIPS = [], GONE = [], FILTER = "all", SORT = "score";

async function loadClips() {
  const d = await api("/api/clips");
  CLIPS = d.clips || [];
  GONE = d.deleted || [];
  $("#clipcountbadge").textContent = CLIPS.length;
  $("#tips").innerHTML = (d.tips || []).map(t => `<div style="background:rgba(99,102,241,0.1); border:1px solid rgba(99,102,241,0.3); border-radius:12px; padding:12px 18px; margin-bottom:18px; color:#cbd5e1; font-size:13.5px;">💡 ${esc(t)}</div>`).join("");
  renderClips();
}

function renderClips() {
  const kept = CLIPS.filter(c => c.decision == "keep").length;
  const todo = CLIPS.filter(c => !c.decision).length;
  
  $("#bar").innerHTML = `
    <div style="font-size:14px; font-weight:700; color:#fff;">${CLIPS.length} Clips</div>
    <div class="filter-group">
      <button class="filter-btn ${FILTER == 'all' ? 'active' : ''}" onclick="FILTER='all';renderClips()">All (${CLIPS.length})</button>
      <button class="filter-btn ${FILTER == 'to review' ? 'active' : ''}" onclick="FILTER='to review';renderClips()">To Review (${todo})</button>
      <button class="filter-btn ${FILTER == 'kept' ? 'active' : ''}" onclick="FILTER='kept';renderClips()">Kept (${kept})</button>
    </div>
    <select style="width:auto; padding:6px 12px; font-size:13px;" onchange="SORT=this.value;renderClips()">
      <option value="score" ${SORT == 'score' ? 'selected' : ''}>🔥 Highest Score First</option>
      <option value="new" ${SORT == 'new' ? 'selected' : ''}>⏱ Newest First</option>
    </select>
    <a href="/api/export-zip?token=${encodeURIComponent(TOKEN)}" class="btn" style="padding:7px 14px; font-size:13px; text-decoration:none; display:inline-flex; align-items:center; gap:6px; background:linear-gradient(135deg, #10b981, #059669); color:#fff; border:none; font-weight:600; border-radius:8px;" download>📦 Download All (ZIP)</a>
    <button id="btn-sync-drive" class="btn" style="padding:7px 14px; font-size:13px; display:inline-flex; align-items:center; gap:6px; background:linear-gradient(135deg, #3b82f6, #1d4ed8); color:#fff; border:none; font-weight:600; border-radius:8px;" onclick="syncToDrive()">☁️ Sync to Drive</button>
    <button class="btn" style="padding:7px 14px; font-size:13px;" onclick="api('/api/open-folder',{})">📁 Open Folder</button>
    ${GONE.length ? `<button class="btn" style="padding:7px 14px; font-size:13px;" onclick="showGone()">Deleted (${GONE.length})</button>` : ''}
  `;

  let list = CLIPS.filter(c => FILTER == "all" || (FILTER == "kept" ? c.decision == "keep" : !c.decision));
  if (SORT == "new") {
    list = [...list].sort((a, b) => (b.time || "").localeCompare(a.time || ""));
  }
  
  $("#grid").innerHTML = list.length 
    ? list.map(card).join("") 
    : `<div class="empty">No clips matching filter. Start clipping to generate viral moments!</div>`;
}

function card(c) {
  const src = (c.sources || []).map(s => `<span class="signal-chip">${esc(s)}</span>`).join("");
  const tags = (c.hashtags || []).join(" ");
  const post = [c.title, c.description, tags].filter(Boolean).join("\n\n");
  const virality = c.virality_score || Math.min(98, Math.max(50, Math.round(50 + (c.score || 1) * 12.5)));
  const hook = c.hook_strength || (5.0 + (c.score || 1) * 1.1).toFixed(1);
  const pacing = c.pacing_score || (6.0 + (c.score || 1) * 0.8).toFixed(1);
  const tier = c.retention_tier || (virality >= 90 ? "Top 5% (Viral Outlier)" : (virality >= 80 ? "Top 15% (Strong)" : "Standard"));
  const reason = c.virality_reason || c.ai_reason || "";

  let badgeGrad = "background:linear-gradient(135deg, #f59e0b, #d97706);";
  let badgeIcon = "📊";
  if (virality >= 90) {
    badgeGrad = "background:linear-gradient(135deg, #10b981, #059669);";
    badgeIcon = "🔥";
  } else if (virality >= 78) {
    badgeGrad = "background:linear-gradient(135deg, #06b6d4, #0284c7);";
    badgeIcon = "⚡";
  }

  return `
    <div class="clip-card ${c.decision == 'keep' ? 'kept' : ''}">
      <div class="video-wrap">
        <video controls preload="none" ${c.thumb ? `poster="/thumb/${encodeURIComponent(c.thumb)}"` : ""} src="/clips/${encodeURIComponent(c.video)}"></video>
        <div class="score-badge" style="${badgeGrad} color:#fff; font-weight:700; box-shadow:0 4px 12px rgba(0,0,0,0.4);">${badgeIcon} ${virality}/100</div>
        <div class="duration-badge">${Math.round(c.length_s || 0)}s</div>
      </div>
      <div class="clip-content">
        <div class="clip-title">${esc(c.title || "(Untitled Moment)")}</div>
        
        <div style="display:flex; gap:6px; flex-wrap:wrap; margin:8px 0;">
          <span style="background:rgba(56,189,248,0.12); color:#38bdf8; border:1px solid rgba(56,189,248,0.3); padding:2px 8px; border-radius:6px; font-size:11.5px; font-weight:700;">🎣 Hook: ${hook}/10</span>
          <span style="background:rgba(167,139,250,0.12); color:#a78bfa; border:1px solid rgba(167,139,250,0.3); padding:2px 8px; border-radius:6px; font-size:11.5px; font-weight:700;">⚡ Pacing: ${pacing}/10</span>
          <span style="background:rgba(52,211,153,0.12); color:#34d399; border:1px solid rgba(52,211,153,0.3); padding:2px 8px; border-radius:6px; font-size:11.5px; font-weight:700;">⏱️ ${esc(tier)}</span>
        </div>

        <div class="signal-chips">${src}</div>

        ${reason ? `
        <div style="background:rgba(99,102,241,0.08); border-left:3px solid #6366f1; border-radius:4px; padding:7px 10px; font-size:12px; color:#cbd5e1; margin:8px 0; line-height:1.4;">
          <span style="color:#818cf8; font-weight:700;">💡 Virality Insight:</span> ${esc(reason)}
        </div>` : ''}

        ${c.description ? `<div style="font-size:12px; color:var(--text-muted); line-height:1.4; margin-bottom:8px;">${esc(c.description)} ${esc(tags)}</div>` : ''}
        
        <div class="clip-actions">
          <button class="btn" style="${c.decision == 'keep' ? 'background:rgba(16,185,129,0.2); border-color:var(--success); color:#34d399;' : ''}" onclick="act('${esc(c.clip)}','keep')">
            ${c.decision == 'keep' ? '✓ Kept' : 'Keep'}
          </button>
          <button class="btn danger" onclick="act('${esc(c.clip)}','delete')">Delete</button>
          ${post ? `<button class="btn copy-post-btn" style="background:rgba(99,102,241,0.15); border-color:rgba(99,102,241,0.4); color:#a5b4fc; font-weight:600;" onclick="navigator.clipboard.writeText(${esc(JSON.stringify(post))});this.textContent='✓ Copied Post Copy!';setTimeout(()=>this.textContent='📋 Copy Post & Tags',2000)">📋 Copy Post & Tags</button>` : ''}
        </div>
      </div>
    </div>
  `;
}

async function act(n, d) {
  await api("/api/decision", { clip: n, decision: d });
  loadClips();
}

function showGone() {
  $("#grid").innerHTML = GONE.map(g => `
    <div class="clip-card" style="opacity:0.75;">
      <div class="clip-content">
        <div class="clip-title">${esc(g.title || g.clip)}</div>
        <div style="font-size:12px; color:var(--text-dim); margin:8px 0;">${esc(g.clip)}</div>
        <button class="btn" onclick="act('${esc(g.clip)}','undo')">Restore Clip</button>
      </div>
    </div>
  `).join("");
}

// Setup Tab
async function loadKeys() {
  const k = (await api("/api/state")).keys || {};
  ["gemini", "anthropic"].forEach(n => {
    $("#s_" + n).textContent = k[n] ? "(Configured & Saved)" : "(Not Set)";
  });
}

$("#savekeys").onclick = async () => {
  const r = await api("/api/keys", {
    gemini: $("#k_gemini").value.trim(),
    anthropic: $("#k_anthropic").value.trim()
  });
  $("#kmsg").textContent = r.ok ? "Keys saved securely!" : "Error saving key format.";
  $("#k_gemini").value = "";
  $("#k_anthropic").value = "";
  loadKeys();
  setTimeout(() => $("#kmsg").textContent = "", 3000);
};

$("#clearkeys").onclick = async () => {
  await api("/api/keys", { gemini: "", anthropic: "", clear: true });
  $("#kmsg").textContent = "Keys cleared.";
  loadKeys();
  setTimeout(() => $("#kmsg").textContent = "", 3000);
};

$("#doctor").onclick = async () => {
  $("#doc").textContent = "Running diagnostic inspection...";
  $("#doc").textContent = (await api("/api/doctor")).text;
};

function setSyncPreset(type) {
  const input = $("#sync_dir");
  if (!input) return;
  if (type === 'gdrive') input.value = "G:\\My Drive\\Clips";
  else if (type === 'onedrive') input.value = "C:\\Users\\vinay\\OneDrive\\Clips";
  else if (type === 'desktop') input.value = "C:\\Users\\vinay\\Desktop\\Clips";
  api("/api/settings", form());
  const msg = $("#sync_status_msg");
  if (msg) { msg.style.color = "var(--accent-cyan)"; msg.textContent = `Preset set: ${input.value}`; }
}

async function testDriveSync() {
  const msg = $("#sync_status_msg");
  const val = ($("#sync_dir").value || "").trim();
  if (val.startsWith("http://") || val.startsWith("https://")) {
    if (msg) {
      msg.style.color = "var(--danger)";
      msg.innerHTML = "⚠️ You pasted a web browser link! Auto-sync needs your local folder on Windows (e.g. <code>G:\\My Drive\\clip</code>).<br>Install <b>Google Drive for Desktop</b> to get a G:\\ drive, or click <b>Use OneDrive</b> which is already active on your PC.";
    }
    return;
  }
  if (msg) { msg.style.color = "var(--text-muted)"; msg.textContent = "Testing & syncing..."; }
  await api("/api/settings", form());
  const res = await api("/api/sync-drive", { sync_dir: val });
  if (msg) {
    if (res.ok) {
      msg.style.color = "var(--success)";
      msg.textContent = `✅ Successfully synced ${res.count} files (${res.summary || ''}) to: ${res.path}`;
    } else {
      msg.style.color = "var(--danger)";
      msg.textContent = `❌ Error: ${res.error}`;
    }
  }
}

async function syncToDrive() {
  const btn = $("#btn-sync-drive");
  const orig = btn ? btn.textContent : "";
  if (btn) btn.textContent = "⏳ Syncing...";
  try {
    const res = await api("/api/sync-drive", {});
    if (res.ok) {
      alert(`✅ Successfully synced ${res.count} items into respective category folders!\nFolders: ${res.summary || 'Synced'}\nPath: ${res.path}`);
    } else {
      alert(`⚠️ Sync failed: ${res.error}\n\nTip: Enter your Google Drive folder in the 'Setup' tab (e.g. G:\\My Drive\\Viral_Clips).`);
    }
  } catch (e) {
    alert(`⚠️ Sync error: ${e}`);
  } finally {
    if (btn) btn.textContent = orig;
  }
}

// Form Auto-save
F.forEach(k => {
  const el = $("#" + k);
  if (el) el.addEventListener("change", () => api("/api/settings", form()));
});

init();
</script>
</body>
</html>"""


def export_clips_zip(clip_dir: Path) -> bytes:
    """Creates a complete production ZIP archive containing clips, thumbnails, SRTs, and social captions."""
    import zipfile, io
    buf = io.BytesIO()
    clips = review.load_clips(clip_dir)
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        caption_lines = [
            "==================================================================",
            "CLIPWATCHER AI PRO STUDIO - VIRAL EXPORT BUNDLE",
            "Ready-to-Post Captions, Hook Titles & Ranked Hashtags",
            "==================================================================\n"
        ]
        for idx, c in enumerate(clips, 1):
            v_name = c.get("video") or c.get("clip")
            if not v_name:
                continue
            v_path = clip_dir / v_name
            if not v_path.exists():
                continue
            clean_title = re.sub(r'[^a-zA-Z0-9_-]', '_', c.get("title") or f"Clip_{idx}")[:40]
            arc_video_name = f"{idx:02d}_{clean_title}.mp4"
            zf.write(v_path, arc_video_name)

            stem = v_path.stem
            t_path = clip_dir / "info" / f"{stem}.jpg"
            if t_path.exists():
                zf.write(t_path, f"{idx:02d}_{clean_title}_thumb.jpg")

            srt_path = clip_dir / "info" / f"{stem}.srt"
            if srt_path.exists():
                zf.write(srt_path, f"{idx:02d}_{clean_title}.srt")

            title_txt = c.get("title") or f"Viral Moment #{idx}"
            desc_txt = c.get("description") or "Wait until the end to see what happens next! Like & Follow for more."
            tags_txt = " ".join(c.get("hashtags") or ["#shorts", "#trending", "#viral", "#fyp"])
            virality = c.get("virality_score", 85)
            hook = c.get("hook_strength", 8.5)
            pacing = c.get("pacing_score", 8.2)

            caption_lines.append(f"--- CLIP {idx:02d} [Virality Score: {virality}/100 | Hook: {hook}/10 | Pacing: {pacing}/10] ---")
            caption_lines.append(f"FILE: {arc_video_name}")
            caption_lines.append(f"TITLE: {title_txt}")
            caption_lines.append(f"CAPTION: {desc_txt}")
            caption_lines.append(f"HASHTAGS: {tags_txt}")
            if c.get("virality_reason"):
                caption_lines.append(f"INSIGHT: {c['virality_reason']}")
            caption_lines.append("\n" + "-" * 60 + "\n")

        zf.writestr("social_post_captions.txt", "\n".join(caption_lines).encode("utf-8"))
    return buf.getvalue()


# --------------------------------------------------------------------------- #
# server
# --------------------------------------------------------------------------- #
def make_handler(here: Path, clip_dir: Path, token: str, runner: Runner):
    class H(BaseHTTPRequestHandler):
        server_version = "ClipWatcherUI"

        def log_message(self, *a):
            pass

        def _json(self, obj, code=200):
            data = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _host_ok(self) -> bool:
            return self.headers.get("Host", "").split(":")[0] in ("127.0.0.1", "localhost")

        def _ok(self) -> bool:
            u = urlparse(self.path)
            q_tok = parse_qs(u.query).get("token", [""])[0]
            return self._host_ok() and (self.headers.get("X-Token") == token or q_tok == token)

        def do_GET(self):
            u = urlparse(self.path)
            path = unquote(u.path)
            if not self._host_ok():
                return self._json({"error": "bad host"}, 403)
            if path == "/":
                data = PAGE.replace("__TOKEN__", token).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            if path.startswith("/clips/"):
                return self._file(path[len("/clips/"):])
            if path.startswith("/thumb/"):
                return self._file(path[len("/thumb/"):], thumb=True)
            if not self._ok():
                return self._json({"error": "forbidden"}, 403)
            if path == "/api/export-zip":
                zip_data = export_clips_zip(clip_dir)
                date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.send_response(200)
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Disposition", f'attachment; filename="clipwatcher_viral_clips_{date_str}.zip"')
                self.send_header("Content-Length", str(len(zip_data)))
                self.end_headers()
                self.wfile.write(zip_data)
                return
            if path == "/api/state":
                return self._json({"settings": load_settings(here), "keys": key_state(here), **runner.state()})
            if path == "/api/log":
                try:
                    since = int(parse_qs(u.query).get("since", ["0"])[0])
                except ValueError:
                    since = 0
                d = runner.log_since(since)
                d["state"] = runner.state()
                return self._json(d)
            if path == "/api/clips":
                return self._json({"clips": review.load_clips(clip_dir), "deleted": review.deleted_clips(clip_dir),
                                   "tips": review.advice(review.stats(clip_dir))})
            if path == "/api/doctor":
                return self._json({"text": run_doctor(here)})
            self._json({"error": "not found"}, 404)

        def _file(self, name: str, thumb: bool = False):
            ext = ".jpg" if thumb else ".mp4"
            base_dir = clip_dir / "info" if thumb else clip_dir
            if name != Path(name).name or not name.endswith(ext) or not (base_dir / name).is_file():
                return self._json({"error": "not found"}, 404)
            f = base_dir / name
            size = f.stat().st_size
            start, end, code = 0, size - 1, 200
            m = re.match(r"bytes=(\d*)-(\d*)", self.headers.get("Range", ""))
            if m and (m.group(1) or m.group(2)):
                if m.group(1):
                    start = int(m.group(1))
                    end = int(m.group(2)) if m.group(2) else size - 1
                else:
                    start = max(0, size - int(m.group(2)))
                end = min(end, size - 1)
                if start > end:
                    self.send_response(416)
                    self.end_headers()
                    return
                code = 206
            self.send_response(code)
            self.send_header("Content-Type", mimetypes.guess_type(name)[0] or "video/mp4")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(end - start + 1))
            if code == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            try:
                with open(f, "rb") as fh:
                    fh.seek(start)
                    left = end - start + 1
                    while left > 0:
                        buf = fh.read(min(65536, left))
                        if not buf:
                            break
                        self.wfile.write(buf)
                        left -= len(buf)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_POST(self):
            if not self._ok():
                return self._json({"error": "forbidden"}, 403)
            path = urlparse(self.path).path
            try:
                n = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(min(n, 20000)).decode() or "{}")
                if not isinstance(body, dict):
                    raise ValueError
            except ValueError:
                return self._json({"error": "bad request"}, 400)
            if path == "/api/start":
                jobs, err = build_jobs(body, here)
                if not jobs:
                    return self._json({"ok": False, "error": err})
                save_settings(here, body)
                ok, err = runner.start_multi(jobs)
                return self._json({"ok": ok, "error": err})
            if path == "/api/stop":
                runner.stop()
                return self._json({"ok": True})
            if path == "/api/settings":
                save_settings(here, body)
                return self._json({"ok": True})
            if path == "/api/keys":
                ok = True
                for name in KEY_NAMES:
                    val = str(body.get(name, ""))
                    if val or body.get("clear"):
                        ok = save_key(here, name, val) and ok
                return self._json({"ok": ok})
            if path == "/api/decision":
                ok = review.decide(clip_dir, str(body.get("clip", "")), str(body.get("decision", "")))
                return self._json({"ok": ok}, 200 if ok else 400)
            if path == "/api/sync-drive":
                sync_target = (body.get("sync_dir") or load_settings(here).get("sync_dir", "")).strip()
                if not sync_target:
                    return self._json({"ok": False, "error": "No cloud sync folder configured in Setup tab."})
                if sync_target.startswith("http://") or sync_target.startswith("https://"):
                    return self._json({
                        "ok": False,
                        "error": "You entered a web browser link! Google Drive auto-sync requires the local folder path on your computer (e.g. 'G:\\My Drive\\clip'). Install Google Drive for Desktop to get a G:\\ drive, or click 'Use OneDrive' which is already active."
                    })
                try:
                    sync_p = Path(sync_target)
                    sync_p.mkdir(parents=True, exist_ok=True)
                    clips = review.load_clips(clip_dir)
                    count = 0
                    cat_counts = collections.defaultdict(int)

                    def get_clip_cat(c: dict) -> str:
                        cat = (c.get("category") or "").strip().lower()
                        if cat and cat != "auto":
                            return cat
                        prof = (c.get("profile") or "").strip().lower()
                        text = f"{c.get('title','')} {c.get('description','')} {c.get('clip','')} {c.get('channel','')} {c.get('stream_title','')}".lower()
                        if prof == "cricket" or any(w in text for w in ("cricket", "wicket", "six", "ipl", "t20", "bowled", "catch")):
                            return "cricket"
                        if prof == "gaming" or any(w in text for w in ("game", "gaming", "valorant", "minecraft", "gta", "speed", "streamer")):
                            return "gaming"
                        if prof == "movies" or any(w in text for w in ("movie", "film", "cinema", "scene", "trailer", "dialogue", "actor", "hero")):
                            return "movies"
                        return prof if prof in ("cricket", "gaming", "movies") else ""

                    for c in clips:
                        vname = c.get("video") or c.get("clip")
                        if not vname:
                            continue
                        src_vid = clip_dir / vname
                        if not src_vid.exists():
                            continue
                        cat = get_clip_cat(c)
                        sub_name = cat
                        if cat and sync_p.exists():
                            for sf in sync_p.iterdir():
                                if sf.is_dir() and sf.name.lower() == cat.lower():
                                    sub_name = sf.name
                                    break
                        dest_d = sync_p / sub_name if sub_name else sync_p
                        dest_d.mkdir(parents=True, exist_ok=True)

                        stem = Path(vname).stem
                        shutil.copy2(str(src_vid), str(dest_d / src_vid.name))
                        count += 1
                        if cat:
                            cat_counts[cat] += 1
                        for cand_t in (clip_dir / ".info" / f"{stem}.jpg", clip_dir / f"{stem}.jpg"):
                            if cand_t.exists():
                                shutil.copy2(str(cand_t), str(dest_d / cand_t.name))
                                count += 1
                        for cand_s in (clip_dir / ".info" / f"{stem}.srt", clip_dir / f"{stem}.srt"):
                            if cand_s.exists():
                                shutil.copy2(str(cand_s), str(dest_d / cand_s.name))
                                count += 1
                        for cand_p in (clip_dir / ".info" / f"{stem}_post.txt", clip_dir / f"{stem}_post.txt"):
                            if cand_p.exists():
                                shutil.copy2(str(cand_p), str(dest_d / cand_p.name))
                                count += 1

                    if not clips:
                        for f in clip_dir.glob("*"):
                            if f.is_file() and f.suffix.lower() in (".mp4", ".jpg", ".srt", ".txt", ".json"):
                                shutil.copy2(str(f), str(sync_p / f.name))
                                count += 1

                    summary = ", ".join(f"{k}: {v} reels" for k, v in cat_counts.items()) if cat_counts else f"{count} files"
                    return self._json({"ok": True, "count": count, "summary": summary, "path": str(sync_p)})
                except Exception as e:
                    return self._json({"ok": False, "error": str(e)})
            if path == "/api/open-folder":
                return self._json({"ok": open_folder(clip_dir)})
            self._json({"error": "not found"}, 404)

    return H


def serve(here: Path, clip_dir: Path, port: int = 8765, tab: str = "run", open_browser: bool = True) -> int:
    here, clip_dir = Path(here), Path(clip_dir)
    clip_dir.mkdir(exist_ok=True)
    token = secrets.token_urlsafe(16)
    runner = Runner(here)
    try:
        srv = ThreadingHTTPServer(("127.0.0.1", port), make_handler(here, clip_dir, token, runner))
    except OSError as e:
        print(f"can't start on port {port}: {e}. Try --port 8800")
        return 1
    url = f"http://127.0.0.1:{port}/#{tab}"
    print(f"Clip Watcher is open at {url}   (Ctrl-C here to quit)")
    if open_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    if runner.running:
        print("stopping the run (finishing pending clips)...")
        runner.stop()
        for _ in range(180):
            if not runner.running:
                break
            time.sleep(1)
    return 0
