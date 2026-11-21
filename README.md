# clip-watcher

Records a live **Twitch or YouTube** stream, spots hype moments from **chat, loudness, on-screen motion and speech**,
and cuts vertical 9:16 clips with subtitles and an optional AI-written hook title.

## Quick start (Windows Git Bash, macOS, Linux)

1. Install **Python 3.9+** and **ffmpeg** (Windows: `winget install ffmpeg`, then open a new Git Bash; macOS: `brew install ffmpeg`; Ubuntu: `sudo apt install ffmpeg`).
2. `git clone <this repo> && cd clip-watcher`
3. `bash setup.sh` (makes `.venv`, installs everything, creates `.env`)
4. `./watch.sh --doctor` then `./watch.sh --selftest` (both should end clean)
5. **Easiest: the web interface.** `./watch.sh --ui` opens a page on your own computer where you paste the stream link,
   pick the kind of stream, press **Start**, watch the live log, review the clips (**Clips** tab) and save your AI key and run the setup check (**Setup** tab).
   Or use the command line:

```bash
./watch.sh https://www.youtube.com/watch?v=VIDEO_ID --profile gaming     # YouTube live
./watch.sh shroud --profile just-chatting                                # Twitch channel
./watch.sh @LofiGirl                                                     # YouTube channel: waits until live
```
Clips land in `clips/`. Stop with Ctrl-C. Optional AI titles: put `GEMINI_API_KEY=...` in `.env`.

### Profiles

| `--profile` | For | Sets |
|---|---|---|
| `gaming` | face cam + gameplay | split layout, a bit more sensitive to loud/fast moments |
| `just-chatting` | talk streams | no motion signal, needs 2 signals agreeing |
| `irl` | walking streams | calmer motion threshold |
| `highlights` | only the best | high confidence only, 60 s between clips |

Any flag you add overrides the profile, e.g. `--profile gaming --layout crop`.

### License
MIT, see `LICENSE`. Issues and PRs welcome: see `CONTRIBUTING.md`.

### Please be a good clipper
Clips use someone else's stream. Check the streamer's clip/reposting policy (and the game publisher's rules), credit the streamer, and don't repost where they've asked you not to.

## Very long videos (v1.7)

You never need to sit through a 24-hour video. In video mode Clip Watcher scans the whole thing first, ranks the moments, then
cuts only the best ones, so you can start it and walk away.

- **Fast scan (default).** Only the sound is scanned (quick, mostly disk speed). The picture is checked later, only around the best
  moments. `--scan full` also scans every frame: more complete, but on a long 1080p video it can take several hours.
- **Saved scan + resume.** The scan is saved in `rec/<video>.analysis.json`. Run it again (or after a crash) and it skips the scan;
  clips already made from that video are counted and never cut twice. `--fresh` forces a new scan.
- **Part of a video.** `--range 2:00:00-5:30:00` scans only that part (also a box on the Run tab).
- **Progress and time left** are printed every 20 s while scanning.
- A very long video also has to be on your disk first: a 24 h 1080p VOD can be 10-20 GB, and `--vod` downloads it all.
- Fixed: runs **without an AI key** used to crash right after encoding a clip (`unexpected error: 'title'`), so the clip existed
  but was not logged or shown in the Clips tab. That is fixed.

## Editing polish (v1.6)

- **Text styles.** `--style classic | bold | neon | minimal | boxed` changes the title and caption look (colours, size, highlight pop,
  title on a black box). Also a dropdown on the Run tab. Same style for the whole run.
- **Even loudness.** Every clip is normalised to -16 LUFS (typical for shorts apps), so loud and quiet streams sound alike. `--no-normalize` turns it off.
  Tested: it does not shift the sound against the picture.
- **Thumbnails.** Each clip gets `clips/info/<clip>.jpg`, a frame from the peak of the moment, shown as the preview in the Clips tab.
- `--top N` is exact now: parallel workers can no longer overshoot.

## Past streams and video files, complete stories, learning (v1.5)

- **Video files and VODs.** No need to wait for a live stream:
  ```bash
  ./watch.sh --file "C:\Videos\stream.mp4" --top 10         # a file on your computer
  ./watch.sh --vod "https://www.youtube.com/watch?v=ID"     # downloads it first (yt-dlp), then the same
  ```
  It scans the whole video for loud and fast moments, ranks them, and makes the best `--top` clips (default 10; `--top 0` = all).
  The web page has the same under **What do you want to clip?**. Live chat is not available for past videos, so it uses sound, motion,
  speech and the AI. Only use videos you have the right to clip.
- **Complete stories and jokes.** Speech-to-text runs once over the area around each moment. Clips start and end at sentence
  boundaries (never mid-sentence). With an AI key, the AI reads the timed transcript and the frames of a wider window (up to
  `--max-pre 30` s of setup) and picks the real start and end: setup, punchline and reaction. Clips can reach `--max-len 75` s.
- **Learns from you.** Every Keep/Delete in the Clips tab is saved. Once you have judged 8+ clips for a signal (chat, audio, motion,
  speech), signals you keep count up to x1.4 and signals you delete count down to x0.6. The run prints
  `learning from your Keep/Delete choices: ...`. `--no-learn` turns it off. This is a simple adjustment, not a trained model.

## Quality, speed and fewer junk clips (v1.3)

- **One file per clip.** The clip is cut, laid out, titled and captioned in a single encode. There is no second plain copy and no
  second re-encode (the old double encode made clips softer). Extras live in `clips/info/` (`.srt`, `*_post.txt`, `.meta.json`).
- **Faster.** Speech-to-text runs before encoding and uses faster-whisper. Slow PC? `--preset veryfast` or `--whisper-model tiny`.
- **Sharper.** Default `--crf 18 --preset fast`, a light sharpen after scaling, and a log line telling you the source resolution.
  A 720p stream cannot look HD in 1080x1920: record a 1080p stream (`--quality best`). The face cam is small in the stream, so
  it is always softer than gameplay; `--cam-share 0.30` makes it smaller and sharper.
- **Fewer, better clips.** Defaults are stricter (`--min-conf med`, `--cooldown 45`). With an AI key, the AI also rates every moment
  1 to 10 from the frames and transcript; anything under `--min-rating 5` is skipped (use `--min-rating 0` to keep everything).
- **Fights are not cut in half.** The clip follows the action until the sound settles (up to `--max-post 45` s) and a moment that falls
  inside a clip just made is skipped, so you don't get the "rest of the fight" as a second clip.
- **Titles that differ.** Each request asks for five different angles, bans overused words (INSANE, CRAZY, EPIC...), shows the AI
  the last 8 titles to avoid, and forces a concrete detail from the clip.
- **Lip-sync.** The cut keeps sound locked to the picture (tested with a flash + beep test video: within 12 ms). If your clips still
  have the voice AFTER the lips, the recording itself is probably out of sync: use `--av-offset -300` (sound 300 ms earlier) and adjust.

## Better clips (v9)

- **Automatic face cam.** In the split layout `--cam` now defaults to `auto`: Clip Watcher looks for a face that stays in one
  place (needs `opencv-python-headless`, installed by `setup.sh`), prefers faces near the left/right edge, and crops around it.
  It prints `face cam found automatically: --cam ...`. If it picks the wrong spot, run `./watch.sh --preview rec/file.ts --layout split`
  and pass `--cam x,y,w,h` yourself. With no face found it falls back to the old default box.
- **Word-by-word captions.** Each spoken word lights up yellow as it is said (`--caption-style words`, the default; `lines` = plain lines).
- **Smart clip length.** The clip ends when the loudness has settled back to normal (`--min-post 6`, `--max-post 20`). `--no-smart-length` keeps a fixed `--post`.
- **Better titles.** With an AI key, the model sees the frames, the **full transcript** and chat, writes 5 candidate titles, picks the best,
  and also writes a description and hashtags. They are saved next to each clip as `*_post.txt` (ready to paste) and `*.meta.json`.
  The title is written after Whisper finishes, so it can use what was said.
- **Review page.** `./watch.sh --review` opens the web interface on the Clips tab (127.0.0.1 only, nothing is uploaded): clips sorted by score,
  playback, Keep / Delete (deleted clips move to `clips/_deleted/` and can be restored), a Copy-text button for the post, and tips
  once you have judged 5+ clips ("you delete most chat-triggered clips: make chat stricter"). Choices are saved in `clips/decisions.json`.

## What it does

1. **Records** the live stream with `streamlink` to `rec/*.ts` (MPEG-TS can be read while it is still being written).
2. **Reads chat** with no login and no API key:
   - **Twitch:** anonymous IRC (`irc.chat.twitch.tv:6667`, `justinfan` nick). Answers `PING`, reconnects automatically.
   - **YouTube:** reads live chat directly from YouTube (falls back automatically if `chat-downloader` breaks). Replayed messages after a reconnect are never counted twice.
3. **Detects spikes** in 10 s buckets. A spike is >= 2.5x the median of the last 3 minutes **and** >= `--min` messages.
4. **Cuts clips**: 20 s before + 10 s after the moment, center-cropped to 9:16 (1080x1920), into `clips/`.
5. **Subtitles**: Whisper writes a `.srt` next to every clip, on a background thread so detection never stalls.

Every clip is also logged to `clips/clips.jsonl` (time, message count, how many times normal chat, top emotes, sample messages) so you can sort the best ones.

## AI hook titles (what is happening on screen, not what the streamer says)

For each clip a vision AI looks at 6 frames plus some context (stream title, why the moment was picked, what the
streamer said, what chat wrote) and writes a hook line such as `SPEED ROBS A BANK IN THE FUNNIEST WAY`. It is burned
onto the seam between face and game (other layouts: near the top) for the whole clip, and word captions go lower down.

```bash
export GEMINI_API_KEY="..."        # or ANTHROPIC_API_KEY (needs paid API credit); Git Bash: put it in ~/.bashrc to keep it
./watch.sh "<url>" --layout split --streamer Speed
./watch.sh "<url>" --layout split --title "WAIT FOR IT"     # no AI: the same text on every clip
./watch.sh "<url>" --title-ai off                            # never use the AI
```
`--streamer` lets the AI use the name. `--title-model` overrides the model (defaults: `gemini-flash-latest`,
`claude-haiku-5-5`); if the API says the model is not found, copy a current name from the provider and pass it.
The title and text are burned into the clip itself (one file per clip); the title is also in `clips/clips.jsonl`. The clip frames are sent to the provider you choose.
Emoji can't be burned reliably, so titles are plain capitals. If the AI call fails the clip is still made, without a title.

## Split layout: face cam on top, gameplay below (shorts style)

```bash
./watch.sh --preview rec/yourfile.ts --layout split      # saves preview_boxes.jpg + preview_result.jpg, records nothing
./watch.sh "<url>" --layout split                         # real run; captions are burned in on the seam
```
- `--cam x,y,w,h` = where the face cam is in the stream, as fractions of the frame (default `0,0.43,0.2,0.205` fits a
  cam in the left-middle). In `preview_boxes.jpg` the **red** box is the cam and the **green** box is the gameplay part that is kept.
- `--cam-share 0.36` = how much of the height the face cam gets. `--game-x 0.5` = which part of the gameplay is kept (0 left, 1 right).
- Captions (needs Whisper, so don't pass `--no-subs`) are big capitals on the seam between face and game, burned into the clip.
  `--no-burn-subs` skips that. The cam is small in the stream, so it is enlarged about 3x and looks softer than the gameplay.

## Scoring: signals that agree make a better clip

Chat, audio, motion and speech no longer cut separate clips. Signals that happen within ~15 s of each
other are merged into one **moment**, which waits `--fuse-wait` (20 s) so slow chat can join, then gets a score:

| Signal | What it adds |
|---|---|
| chat | spike size; extra if the messages are hype (W, KEKW, LUL, fire/skull emoji) and if people write "clip it" |
| audio | how far above normal loudness it jumped |
| motion | how far above normal on-screen action it was (counts a bit less, it is the noisiest) |
| speech | Whisper (`tiny`) transcribes ~14 s around the moment; reactions like "no way", "let's go", "oh my god" score, filler like "bro" barely does |

More independent signals = more confidence: **HIGH** (3+ signals, or 2 strong ones), **MED**, **LOW** (one weak signal).
The label is in the file name (`..._HIGH_chat+audio+speech.mp4`), `clips/clips.jsonl` has the score, the signals and what
was said, and the end-of-run summary lists the best clips. Review HIGH first.

```bash
./watch.sh "<url>" --min-conf med        # only keep MED and HIGH clips
./watch.sh "<url>" --no-speech           # skip the Whisper reaction check (faster)
./watch.sh "<url>" --hype-words "sus,cooked,aura"   # your own reaction phrases
```
Clip start and end are also moved to the nearest pause in the audio, so clips don't begin or end mid-word (`--no-snap` turns it off).

## Audio + motion clips (not just chat)

Chat alone misses a streamer shouting or a fight when chat is slow or steady, so the recording itself
is also analysed while it is written. All three triggers share one cooldown, so one moment = one clip.

| Trigger | Fires when | Tune with |
|---|---|---|
| **audio** | loudness jumps well above the stream's normal level (shouting, screams, explosions) | `--audio-db 9` (lower = more clips; try 6) |
| **motion** | the picture suddenly changes a lot (fights, action, scene cuts) | `--motion-factor 2.5` (try 1.8), `--motion-min 6` |
| **chat** | message rate spikes | `--factor`, `--sigma`, `--min` |

```bash
./watch.sh "https://www.youtube.com/watch?v=VIDEO_ID" --layout blur --audio-db 7 --motion-factor 2 --factor 1.5 --sigma 3
./watch.sh "<url>" --no-chat          # only audio + motion
./watch.sh "<url>" --no-motion        # chat + audio only
```
Clip names show why they were made (`..._audio22.mp4` = 22 dB louder than normal, `..._motion14.mp4`,
`..._x3.1.mp4` = 3.1x chat). `clips/clips.jsonl` records the `source` of each clip.
The first minute (`--warmup`) is used to learn what "normal" sounds and looks like.

## Beyond the original spec

| Addition | Why |
|---|---|
| Anti-spam: `--min-users` (3) and `--user-cap` (5) | One person pasting 100 messages can't trigger a clip |
| Noise guard `--sigma` (5) | Random wobble in a medium chat doesn't fire clips (checked on 4 h of simulated chat: ~0 false clips) |
| Warm-up (`--warmup 60`) | No clips from the first minute, when there's no baseline |
| Peak finding | A rising spike waits for its top bucket, so the clip centers on the peak, not the start of the climb |
| Cooldown (`--cooldown 30`) | One big moment = one clip, not five overlapping ones |
| Smart sync | Clip position comes from the recording's real duration, so stream delay cancels out. `--lag` is the only knob |
| Waits for footage | Cutting waits until the "after" part is actually on disk instead of a fixed sleep |
| `--layout blur` | Full frame over a blurred background (use for games where the center crop loses the action) |
| `--no-burn-subs` | Don't burn captions into the video (a `.srt` is still written to `clips/info/`) |
| `--keep-wide` | Also saves the uncropped 16:9 clip (lossless copy) for editing |
| `--chat-only` | Skip recording, just print spikes: perfect for tuning `--min` and `--factor` |
| `--webhook URL` | Discord ping when a clip is ready |
| `--max-rec-gb N` | Disk cap: rotates and prunes old recordings (never ones in use) |
| Offline handling | Waits for the stream to go live, resumes after a stream restart |
| Clean Ctrl-C | Finishes pending clips and subtitle jobs before exiting |
| `--selftest` | Offline checks of the detector, captions, and filters |

## YouTube notes

- **Accepted inputs:** watch URL, `youtu.be/...`, `/live/ID`, `@handle`, `/channel/ID`, or a bare 11-character video ID with `--platform youtube`.
- **Channel mode** (`@handle`, `/channel/ID`) keeps waiting until the channel goes live, then starts. A watch URL for a stream that hasn't started yet also waits.
- **Timing:** YouTube chat arrives in small batches, so the default `--lag` is 8 s (Twitch: 5 s). If clips start too late, raise it.
- **Reliability:** `chat-downloader` reads the same chat feed the YouTube web player uses. YouTube sometimes changes that feed, which can break it until the package is updated. If chat stops working, run `pip install -U chat-downloader`.
- **Members-only, age-restricted or private streams** may need cookies for streamlink (`--streamlink-args '--http-cookies ...'`) and may not allow chat reading at all.
- Titles of the stream are saved in `clips/clips.jsonl` (`stream_title`) for later use.

## Usage

```bash
./watch.sh shroud                          # defaults
./watch.sh shroud --min 5                  # quiet chat
./watch.sh shroud --layout blur --burn-subs
./watch.sh shroud --chat-only              # tune thresholds without recording
./watch.sh shroud --pre 25 --post 15 --lag 7
./watch.sh shroud --quality 720p60,720p,best --max-rec-gb 20
./watch.sh shroud --no-subs                # skip Whisper entirely
./watch.sh @LofiGirl --chat-only           # try YouTube chat detection without recording
./watch.sh --help                          # everything
```

## Tuning

- **Too many clips?** Raise `--min` and/or `--factor`. **Too few?** Lower `--min` (quiet chats: 5).
- **Clip starts too late (missed the setup)?** Raise `--lag` (chat reacts later than 5 s, YouTube 8 s) or `--pre`.
- **Clip starts too early?** Lower `--lag`.
- **Captions slow?** Speech-to-text now uses `faster-whisper` (about 4x faster, no PyTorch). Use `--whisper-model tiny` for even faster, or `small` for better accuracy.
- **Ads on Twitch?** Streamlink filters ad breaks, but the recording can pause during them. If you're subscribed to the channel, pass your token:
  `--streamlink-args '--twitch-api-header "Authorization=OAuth <token>"'`
- **Disk:** source-quality Twitch is about 2-3 GB/hour. Use `--max-rec-gb` for long sessions.

## Output

```
clips/
  <name>_20261119-153012_x4.2.mp4     # 9:16 clip (YouTube names start with yt_), "x4.2" = 4.2x normal chat rate
  <name>_20261119-153012_x4.2.srt     # subtitles
  <name>_20261119-153012_x4.2_captions.mp4   # only with --burn-subs
  clips.jsonl                         # one JSON line per clip
rec/
  <name>_20261119-150000.ts           # raw recording
  streamlink.log
```

## Notes

- It only works while the stream is **live**. If offline, it waits and starts when the stream begins. Past videos are not supported yet.
- Only clip and repost streamers who allow it. Many have clip or VOD reuse rules; check before publishing.
- Chat reading is read-only. It never posts and needs no account, token or API key.
- Windows: use WSL2.

## Web interface (`./watch.sh --ui`)

- **Run tab:** stream link, kind of stream (gaming / just chatting / IRL / only the best), optional streamer name and advanced options.
  **Start** launches the same program as the command line; **Stop** asks it to finish pending clips and exit cleanly. The live log highlights clips and titles.
- **Clips tab:** every clip sorted by score with playback, Keep / Delete / Restore, Copy text and "Open clips folder".
- **Setup tab:** saves your Gemini or Claude key into `.env` (never shown again, never sent anywhere except the AI you chose) and runs the setup check.
- It only listens on `127.0.0.1` and every request needs a random token, so other websites and other computers cannot use it.
  `--port 8800` changes the port. Closing the browser tab does not stop a run; press Ctrl-C in the terminal running `--ui` to quit (it stops the run first).

## v1.8 - quiet moments and short videos

- Short videos no longer waste time on the 60 s warm-up (it shrinks to ~10 % of the video).
- **AI picture sweep**: with an AI key, the video is looked at in ~8 minute chunks and the AI picks moments that are
  interesting to *see* (a reveal, a surprise) even when nothing is loud. It runs automatically for videos up to 10 minutes,
  or whenever the sound scan finds nothing. Force it on any length with `--scan ai`.
- If no sound jump is big enough, the log says how big the biggest one was and which setting (`--audio-db`) to lower.
- `--min-conf high` is treated as `med` in video mode (no chat means high is out of reach).
