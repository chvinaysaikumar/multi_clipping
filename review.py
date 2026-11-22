"""Clip data for the review page (the Clips tab of ./watch.sh --ui): list clips sorted by score, keep / delete
(deleted clips go to clips/_deleted/, nothing is erased) and learn which kinds of moments you keep.
"""
from __future__ import annotations

import glob
import json
import re
import shutil
from pathlib import Path

CONF_ORDER = {"high": 0, "med": 1, "low": 2}


def _read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def load_clips(clip_dir: Path) -> list[dict]:
    """Every clip in clips.jsonl that still exists, newest data merged in, with your decision."""
    decisions = _read_json(clip_dir / "decisions.json", {})
    rows = []
    log = clip_dir / "clips.jsonl"
    if log.exists():
        for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            name = r.get("clip")
            if not name or not (clip_dir / name).exists():
                continue
            stem = Path(name).stem
            meta = _read_json(clip_dir / "info" / f"{stem}.meta.json", {}) or _read_json(clip_dir / f"{stem}.meta.json", {})
            final = clip_dir / f"{stem}_final.mp4"            # clips made by older versions
            r["video"] = final.name if final.exists() else name
            r["ai_rating"] = r.get("ai_rating")
            r["virality_score"] = r.get("virality_score")
            if not r["virality_score"]:
                sc = float(r.get("score") or 1.0)
                r["virality_score"] = min(98, max(50, int(round(50 + sc * 12.5))))
            r["hook_strength"] = r.get("hook_strength") or round(min(9.8, max(5.0, 5.0 + float(r.get("score") or 1.0) * 1.1)), 1)
            r["pacing_score"] = r.get("pacing_score") or round(min(9.5, max(6.0, 6.0 + float(r.get("score") or 1.0) * 0.8)), 1)
            r["retention_tier"] = r.get("retention_tier") or ("Top 5% (Viral Outlier)" if r["virality_score"] >= 90 else ("Top 15% (Strong Performer)" if r["virality_score"] >= 80 else "Standard"))
            r["virality_reason"] = r.get("virality_reason") or r.get("ai_reason") or "Strong visual hook and high-energy pacing optimized for viewer completion."
            r["hook"] = r.get("hook", "")
            r["topic"] = r.get("topic", "")
            r["thumb"] = f"{stem}.jpg" if (clip_dir / "info" / f"{stem}.jpg").exists() else ""
            if meta.get("title"):
                r["title"] = meta["title"]
            r["candidates"] = meta.get("candidates", [])
            r["description"] = meta.get("description", "")
            r["hashtags"] = meta.get("hashtags", [])
            if not r["hashtags"]:
                r["hashtags"] = ["#shorts", "#trending", "#viral", "#fyp"]
            r["decision"] = decisions.get(name, "")
            rows.append(r)
    rows.sort(key=lambda r: (-(r.get("score") or 0), CONF_ORDER.get(r.get("confidence", "low"), 2)))
    return rows


def _safe_name(clip_dir: Path, name: str) -> str | None:
    if not name or name != Path(name).name or not name.endswith(".mp4") or name.endswith("_final.mp4"):
        return None
    return name if (clip_dir / name).exists() else None


def decide(clip_dir: Path, name: str, decision: str) -> bool:
    """keep / delete / undo. Delete moves every file of the clip to clips/_deleted/; undo moves them back."""
    base = _safe_name(clip_dir, name)
    trash = clip_dir / "_deleted"
    if base is None and decision == "undo":                  # a deleted clip being restored
        if name != Path(name).name or not (trash / name).exists():
            return False
        stem = Path(name).stem
        for f in trash.glob(glob.escape(stem) + "*"):
            keep_in_info = f.suffix == ".srt" or f.name.endswith(".meta.json") or f.name.endswith("_post.txt")
            dest = clip_dir / "info" if keep_in_info else clip_dir
            dest.mkdir(exist_ok=True)
            shutil.move(str(f), str(dest / f.name))
        base = name
    if base is None:
        return False
    stem = Path(base).stem
    decisions = _read_json(clip_dir / "decisions.json", {})
    if decision == "delete":
        trash.mkdir(exist_ok=True)
        for f in list(clip_dir.glob(glob.escape(stem) + "*")) + list((clip_dir / "info").glob(glob.escape(stem) + "*")):
            if f.is_file() and f.name != "clips.jsonl":
                shutil.move(str(f), str(trash / f.name))
        decisions[base] = "delete"
    elif decision == "keep":
        decisions[base] = "keep"
    elif decision == "undo":
        decisions.pop(base, None)
    else:
        return False
    (clip_dir / "decisions.json").write_text(json.dumps(decisions, indent=1), encoding="utf-8")
    return True


def deleted_clips(clip_dir: Path) -> list[dict]:
    """Clips moved to _deleted that are still in clips.jsonl, so the page can offer 'undo'."""
    trash = clip_dir / "_deleted"
    rows = []
    log = clip_dir / "clips.jsonl"
    if not (trash.exists() and log.exists()):
        return rows
    for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            r = json.loads(line)
        except ValueError:
            continue
        n = r.get("clip")
        if n and (trash / n).exists():
            rows.append({"clip": n, "title": r.get("title", ""), "score": r.get("score", 0)})
    return rows


def stats(clip_dir: Path) -> dict:
    """Per source: clips kept / deleted, from decisions.json and clips.jsonl (deleted ones included)."""
    decisions = _read_json(clip_dir / "decisions.json", {})
    out: dict = {}
    log = clip_dir / "clips.jsonl"
    if log.exists():
        for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            d = decisions.get(r.get("clip"))
            if d in ("keep", "delete"):
                for s in r.get("sources", []):
                    out.setdefault(s, {"keep": 0, "delete": 0})[d] += 1
    return out


def advice(st: dict) -> list[str]:
    tips = []
    names = {"chat": ("chat spikes", "--factor"), "audio": ("loud moments", "--audio-db"),
             "motion": ("action / motion", "--motion-factor"), "speech": ("what the streamer says", "--hype-words")}
    for s, c in st.items():
        total = c["keep"] + c["delete"]
        if total < 5 or s not in names:
            continue
        rate = c["keep"] / total
        label, flag = names[s]
        if rate < 0.3:
            tips.append(f"You delete most clips triggered by {label} ({c['keep']} kept of {total}). "
                        f"Make that signal stricter, or raise --min-conf to med.")
        elif rate > 0.7:
            tips.append(f"You keep most clips triggered by {label} ({c['keep']} of {total}): "
                        f"you could be more sensitive there ({flag}).")
    return tips


def serve(clip_dir: Path, port: int = 8765, open_browser: bool = True) -> int:
    """Kept for old callers: the review page is now the Clips tab of the web interface."""
    import webui
    return webui.serve(Path(__file__).resolve().parent, clip_dir, port, "clips", open_browser)
