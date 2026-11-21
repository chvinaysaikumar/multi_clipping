"""
viral_engine.py - Professional AI Video-First Clipping Engine
Turns long pre-recorded videos into viral 9:16 vertical clips with:
  1. Semantic Storyline & Hook Scoring (Whisper + Gemini 1.5/2.0 Flash)
  2. Word-by-word Animated Karaoke Subtitles (TikTok / Reels style)
  3. Dynamic Virtual Cameraman (Face-tracking 9:16 auto-framing)
  4. GPU-Accelerated Video Rendering (NVENC / QSV / AMF / libx264)
"""
from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


# --------------------------------------------------------------------------- #
# 1. GPU Acceleration & Hardware Encoder Detection
# --------------------------------------------------------------------------- #
_CACHED_ENCODER = None


def detect_gpu_encoder() -> tuple[str, list[str]]:
    """
    Detects hardware-accelerated H.264 encoders:
      - NVIDIA: h264_nvenc
      - Intel:  h264_qsv
      - AMD:    h264_amf
      - CPU:    libx264 (fallback)
    Returns: (encoder_name, extra_ffmpeg_args)
    """
    global _CACHED_ENCODER
    if _CACHED_ENCODER is not None:
        return _CACHED_ENCODER

    try:
        r = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True, text=True, timeout=5
        )
        out = r.stdout.lower()
        if "h264_nvenc" in out:
            # Test if NVENC actually initialises on this hardware
            test = subprocess.run(
                ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi",
                 "-i", "color=c=black:s=64x64", "-frames:v", "1", "-c:v", "h264_nvenc", "-f", "null", "-"],
                capture_output=True, timeout=5
            )
            if test.returncode == 0:
                _CACHED_ENCODER = ("h264_nvenc", ["-preset", "p6", "-cq", "16", "-b:v", "18M", "-maxrate", "30M", "-bufsize", "40M"])
                return _CACHED_ENCODER

        if "h264_qsv" in out:
            test = subprocess.run(
                ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi",
                 "-i", "color=c=black:s=64x64", "-frames:v", "1", "-c:v", "h264_qsv", "-f", "null", "-"],
                capture_output=True, timeout=5
            )
            if test.returncode == 0:
                _CACHED_ENCODER = ("h264_qsv", ["-preset", "medium", "-global_quality", "16", "-b:v", "18M", "-maxrate", "30M"])
                return _CACHED_ENCODER

        if "h264_amf" in out:
            test = subprocess.run(
                ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi",
                 "-i", "color=c=black:s=64x64", "-frames:v", "1", "-c:v", "h264_amf", "-f", "null", "-"],
                capture_output=True, timeout=5
            )
            if test.returncode == 0:
                _CACHED_ENCODER = ("h264_amf", ["-quality", "quality", "-rc", "cbr", "-b:v", "18M", "-maxrate", "30M"])
                return _CACHED_ENCODER
    except Exception:
        pass

    _CACHED_ENCODER = ("libx264", ["-preset", "medium", "-crf", "16"])
    return _CACHED_ENCODER


# --------------------------------------------------------------------------- #
# 2. Modern Word-Level Animated Karaoke Subtitles
# --------------------------------------------------------------------------- #
ASS_HEADER = """[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: ViralActive,Montserrat,72,&H0000FFFF,&H00FFFFFF,&H00000000,&H90000000,-1,0,0,0,100,100,2,0,1,6,2,2,40,40,460,1
Style: ViralTitle,Montserrat,64,&H00FFFFFF,&H0000FFFF,&H00000000,&HB0000000,-1,0,0,0,100,100,3,0,1,7,3,8,40,40,160,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""


def _sec_to_ass(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:d}:{m:02d}:{s:05.2f}"


def generate_karaoke_ass(
    words: list[dict],
    dst_path: Path,
    hook_title: str = "",
    clip_start_s: float = 0.0,
    max_words_per_chunk: int = 3,
) -> None:
    """
    Generates high-retention karaoke subtitles:
      - 2-3 words per on-screen burst
      - The active spoken word pops in Neon Yellow (&H0000FFFF) with slight scale pop
      - Previous/next words in crisp white with bold black outline
    """
    events = []

    # Optional top hook header (shown during the first 4 seconds)
    if hook_title:
        t_end = _sec_to_ass(min(5.0, (words[-1]["end"] - clip_start_s) if words else 5.0))
        clean_title = re.sub(r'[\r\n]+', ' ', hook_title).upper()[:50]
        events.append(f"Dialogue: 1,0:00:00.00,{t_end},ViralTitle,,0,0,0,,{clean_title}")

    # Re-zero timestamps relative to the clip start
    rel_words = []
    for w in words:
        s0 = max(0.0, w.get("start", 0.0) - clip_start_s)
        e0 = max(s0 + 0.1, w.get("end", 0.0) - clip_start_s)
        text = str(w.get("word", "")).strip()
        if text:
            rel_words.append({"word": text, "start": s0, "end": e0})

    if not rel_words:
        dst_path.write_text(ASS_HEADER, encoding="utf-8")
        return

    # Group into punchy 2-3 word visual chunks
    chunks = []
    curr = []
    for w in rel_words:
        curr.append(w)
        # End chunk if max words reached or ending in punctuation
        if len(curr) >= max_words_per_chunk or curr[-1]["word"].endswith((".", "!", "?", ",")):
            chunks.append(curr)
            curr = []
    if curr:
        chunks.append(curr)

    for chunk in chunks:
        if not chunk:
            continue
        # Display each word in the chunk with word-by-word active highlighting
        for active_idx, active_word in enumerate(chunk):
            w_start = _sec_to_ass(active_word["start"])
            w_end = _sec_to_ass(active_word["end"])

            rendered_tokens = []
            for j, token in enumerate(chunk):
                raw_word = token["word"].upper().replace("{", "").replace("}", "")
                if j == active_idx:
                    # Highlight active word in Neon Yellow with subtle scale bump
                    rendered_tokens.append(
                        f"{{\\c&H0000FFFF\\fscx112\\fscy112}}{raw_word}{{\\fscx100\\fscy100\\c&H00FFFFFF}}"
                    )
                else:
                    rendered_tokens.append(f"{{\\c&H00FFFFFF}}{raw_word}")

            line_text = " ".join(rendered_tokens)
            events.append(f"Dialogue: 0,{w_start},{w_end},ViralActive,,0,0,0,,{line_text}")

    content = ASS_HEADER + "\n".join(events) + "\n"
    dst_path.write_text(content, encoding="utf-8")


# --------------------------------------------------------------------------- #
# 3. Dynamic Virtual Cameraman (Face-Tracking 9:16 Auto-Reframing)
# --------------------------------------------------------------------------- #
class DynamicAutoFramer:
    """
    Samples frames from 16:9 video, detects face coordinates, and computes a smoothly
    moving 9:16 crop box using exponential smoothing.
    """

    def __init__(self, target_w: int = 1080, target_h: int = 1920, smoothing: float = 0.2):
        self.target_w, self.target_h = target_w, target_h
        self.smoothing = smoothing
        self.cascade = None
        self._init_cascade()

    def _init_cascade(self):
        try:
            import cv2
            xml = Path(cv2.data.haarcascades) / "haarcascade_frontalface_default.xml"
            if xml.exists():
                self.cascade = cv2.CascadeClassifier(str(xml))
        except Exception:
            self.cascade = None

    def compute_crop_box(self, video_path: Path, start_s: float, length_s: float) -> str:
        """
        Returns an FFmpeg crop expression, e.g. 'crop=607:1080:120:0,scale=1080:1920'
        """
        # Probe video dimensions
        try:
            r = subprocess.run(
                ["ffprobe", "-v", "error", "-select_streams", "v:0",
                 "-show_entries", "stream=width,height", "-of", "csv=p=0", str(video_path)],
                capture_output=True, text=True, timeout=5
            )
            w_str, h_str = r.stdout.strip().splitlines()[0].split(",")[:2]
            src_w, src_h = int(w_str), int(h_str)
        except Exception:
            src_w, src_h = 1920, 1080

        crop_w = int(src_h * (9 / 16))
        if crop_w % 2 != 0:
            crop_w -= 1
        max_x = max(0, src_w - crop_w)
        default_x = max(0, (src_w - crop_w) // 2)

        if not self.cascade:
            return f"crop={crop_w}:{src_h}:{default_x}:0,scale={self.target_w}:{self.target_h}"

        # Sample 10 frames across the clip to locate the primary speaker's face
        sample_n = 8
        step = max(1.0, length_s / sample_n)
        detected_x = []

        with tempfile.TemporaryDirectory() as d:
            pattern = Path(d) / "sample_%02d.jpg"
            subprocess.run(
                ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                 "-ss", f"{max(0.0, start_s):.2f}", "-t", f"{length_s:.2f}", "-i", str(video_path),
                 "-vf", f"fps={sample_n / max(1.0, length_s):.4f},scale=640:-2",
                 "-frames:v", str(sample_n), "-q:v", "4", str(pattern)],
                capture_output=True, timeout=15
            )
            import cv2
            for f in sorted(Path(d).glob("sample_*.jpg")):
                img = cv2.imread(str(f), cv2.IMREAD_GRAYSCALE)
                if img is None:
                    continue
                h_img, w_img = img.shape
                faces = self.cascade.detectMultiScale(img, scaleFactor=1.2, minNeighbors=4, minSize=(30, 30))
                if len(faces) > 0:
                    # Pick largest face
                    largest = max(faces, key=lambda b: b[2] * b[3])
                    # Scale back to original video dimensions
                    fx = largest[0] * (src_w / w_img)
                    fw = largest[2] * (src_w / w_img)
                    center_x = fx + fw / 2.0
                    crop_left = center_x - (crop_w / 2.0)
                    clamped_x = max(0, min(max_x, int(crop_left)))
                    detected_x.append(clamped_x)

        if detected_x:
            # Use median to avoid glitchy outlier frames
            chosen_x = int(sorted(detected_x)[len(detected_x) // 2])
        else:
            chosen_x = default_x

        if chosen_x % 2 != 0:
            chosen_x -= 1

        return f"crop={crop_w}:{src_h}:{chosen_x}:0,scale={self.target_w}:{self.target_h}:flags=lanczos,format=yuv420p,setsar=1"


# --------------------------------------------------------------------------- #
# 4. Semantic Storyline & Virality Engine (Full Transcript Segmentation)
# --------------------------------------------------------------------------- #
VIRAL_PROMPT = """You are an elite short-form content director and viral video editor for TikTok, YouTube Shorts, and Instagram Reels.
Below is the full transcript of a video, formatted with timestamps: [start_s - end_s] text.

YOUR OBJECTIVE:
Identify the top {top_k} most viral, engaging, and standalone clips from this video (each between 20 to 60 seconds long).

CRITICAL VIRALITY CRITERIA:
1. THE HOOK (0-5s): Must start with an immediate curiosity hook, shocking claim, surprising question, funny reaction, or high-stakes statement.
2. STANDALONE NARRATIVE: The clip must be 100% understandable and satisfying on its own without needing the full video. It must have: Hook -> Setup/Tension -> Climax/Payoff.
3. CLEAN SENTENCE BOUNDARIES: start_s MUST be the exact start of a sentence. end_s MUST be the exact conclusion of a thought or punchline. NEVER cut someone off mid-sentence!
4. HIGH RETENTION & VIRALITY: Prioritize humor, emotional stories, hot takes, intense clutch/fail moments, counter-intuitive insights, or relatable truths.

Respond ONLY with valid JSON with this exact schema:
{{
  "clips": [
    {{
      "start_s": 12.0,
      "end_s": 48.5,
      "hook": "Opening hook line spoken in the first 3-5 seconds",
      "topic": "Concise 4-word summary of the clip",
      "virality_score": 94,
      "hook_strength": 9,
      "reason": "Why viewers will watch until the end and share this clip",
      "suggested_title": "PUNCHY VIRAL TITLE IN ALL CAPS",
      "hashtags": ["#viral", "#shorts", "#trending"]
    }}
  ]
}}

Transcript:
{transcript}
"""


class SemanticSegmenter:
    """
    Analyzes full speech-to-text transcripts with Gemini 1.5/2.0 Flash to locate
    complete viral story segments with hook and virality scoring.
    """

    def __init__(self, api_key: str, model: str = "gemini-1.5-flash"):
        self.key = api_key
        self.model = model or "gemini-1.5-flash"

    def is_available(self) -> bool:
        return bool(self.key and (self.key.startswith("AIzaSy") or not "your-key" in self.key))

    def extract_viral_segments(self, word_segments: list[dict], top_k: int = 5) -> list[dict]:
        """
        word_segments: list of dicts [{'word': str, 'start': float, 'end': float}, ...]
        Returns a list of scored clip segments.
        """
        if not self.is_available() or not word_segments:
            return []

        # Format transcript into compact timestamped lines (~6 words per line)
        lines = []
        chunk = []
        for w in word_segments:
            chunk.append(w.get("word", ""))
            if len(chunk) >= 6 or w.get("word", "").endswith((".", "?", "!")):
                t0 = w.get("start", 0.0)
                lines.append(f"[{t0:.1f}s] {' '.join(chunk)}")
                chunk = []
        if chunk:
            lines.append(f"[{word_segments[-1].get('start', 0.0):.1f}s] {' '.join(chunk)}")

        formatted_transcript = "\n".join(lines)
        if len(formatted_transcript) < 30:
            return []

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
        headers = {"x-goog-api-key": self.key, "Content-Type": "application/json"}
        prompt_text = VIRAL_PROMPT.format(top_k=top_k, transcript=formatted_transcript[:120000])

        body = {
            "contents": [{"parts": [{"text": prompt_text}]}],
            "generationConfig": {
                "temperature": 0.2,
                "response_mime_type": "application/json"
            }
        }

        try:
            req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers)
            with urllib.request.urlopen(req, timeout=45) as r:
                res = json.loads(r.read().decode("utf-8", "replace"))

            cands = res.get("candidates") or []
            if not cands:
                return []
            parts = cands[0].get("content", {}).get("parts") or []
            text_out = "".join(p.get("text", "") for p in parts)

            data = json.loads(text_out)
            raw_clips = data.get("clips") or []

            # Snap clip boundaries to nearest Whisper word boundaries
            snapped_clips = []
            for c in raw_clips:
                s_raw = float(c.get("start_s", 0.0))
                e_raw = float(c.get("end_s", 0.0))
                if e_raw - s_raw < 10.0:
                    continue

                # Snap start to the word immediately preceding or matching
                s_snap = min(word_segments, key=lambda w: abs(w.get("start", 0.0) - s_raw)).get("start", s_raw)
                # Snap end to the word immediately preceding or matching
                e_snap = min(word_segments, key=lambda w: abs(w.get("end", 0.0) - e_raw)).get("end", e_raw)

                snapped_clips.append({
                    "start_s": round(s_snap, 2),
                    "end_s": round(e_snap, 2),
                    "length_s": round(e_snap - s_snap, 2),
                    "virality_score": int(c.get("virality_score", 75)),
                    "hook_strength": int(c.get("hook_strength", 7)),
                    "hook": c.get("hook", ""),
                    "topic": c.get("topic", ""),
                    "reason": c.get("reason", ""),
                    "suggested_title": c.get("suggested_title", "VIRAL MOMENT"),
                    "hashtags": c.get("hashtags", ["#viral", "#shorts"]),
                })

            snapped_clips.sort(key=lambda x: -x["virality_score"])
            return snapped_clips[:top_k]

        except Exception as e:
            # Safe degradation on API error
            return []


# --------------------------------------------------------------------------- #
# 5. Fast Clip Rendering Pipeline
# --------------------------------------------------------------------------- #
def render_viral_vertical_clip(
    src_video: Path,
    start_s: float,
    length_s: float,
    dst_clip: Path,
    ass_path: Path | None = None,
    crop_filter: str | None = None,
    use_gpu: bool = True,
    target_w: int = 1080,
    target_h: int = 1920,
) -> tuple[bool, str]:
    """
    Cuts and exports a vertical 9:16 clip (1080x1920 or 2K 1440x2560) with:
      - Full-frame 9:16 edge-to-edge crop (Dynamic face-tracked or center)
      - Burned-in karaoke ASS subtitles
      - Hardware-accelerated GPU encoding with universal yuv420p color
    """
    encoder, enc_args = detect_gpu_encoder() if use_gpu else ("libx264", ["-preset", "fast", "-crf", "22"])

    filter_parts = []
    if crop_filter:
        filter_parts.append(crop_filter)
    else:
        filter_parts.append(
            f"crop=trunc(ih*9/16/2)*2:ih:(iw-trunc(ih*9/16/2)*2)/2:0,scale={target_w}:{target_h}:flags=lanczos,format=yuv420p,setsar=1"
        )

    if ass_path and ass_path.exists():
        # Clean path for FFmpeg subtitles filter on Windows
        clean_ass = str(ass_path).replace("\\", "/").replace(":", "\\:")
        filter_parts.append(f"ass='{clean_ass}'")

    vf_chain = ",".join(filter_parts)

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-ss", f"{start_s:.3f}",
        "-t", f"{length_s:.3f}",
        "-i", str(src_video),
        "-vf", vf_chain,
        "-c:v", encoder,
        *enc_args,
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        str(dst_clip)
    ]

    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if r.returncode == 0 and dst_clip.exists() and dst_clip.stat().st_size > 1000:
            return True, ""
        return False, (r.stderr or "ffmpeg exited with non-zero status").strip()
    except subprocess.TimeoutExpired:
        return False, "ffmpeg encoding timed out after 180s"
    except Exception as e:
        return False, str(e)
