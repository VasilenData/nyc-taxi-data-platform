"""Render a captioned video replay from recorded evidence, not invented output.

Optional renderer dependencies: Pillow and imageio-ffmpeg. This creates a visual
replay, not footage of a person operating an editor. The evidence remains the
original process output and row comparisons.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import textwrap

import imageio_ffmpeg
from PIL import Image, ImageDraw, ImageFont


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--font-dir", type=Path, default=Path("C:/Windows/Fonts"))
    args = parser.parse_args()
    data = json.loads(args.evidence.read_text(encoding="utf-8"))
    output = args.evidence.parent.parent
    frame_dir = output / "frames"
    frame_dir.mkdir(exist_ok=True)
    sans = args.font_dir / "arial.ttf"
    bold = args.font_dir / "arialbd.ttf"
    mono = args.font_dir / "consola.ttf"
    fonts = {"title": ImageFont.truetype(str(bold), 43), "body": ImageFont.truetype(str(sans), 27),
             "label": ImageFont.truetype(str(bold), 20), "mono": ImageFont.truetype(str(mono), 24)}
    patch = "\n".join(line for line in data["steps"][1]["output"].splitlines()
                      if line.startswith(("- ", "+ ")))
    slides = [
        (8, "A failed refresh must not replace good data", "NYC TAXI / RECOVERY WALKTHROUGH",
         "Eight consumer tables. One transaction.\n\nStart with 4 valid rows; build a 7-row candidate.\nStop the process after the fact-table write.\nReopen the database and compare every consumer row.",
         "Captioned replay of commands executed on 13 November 2026.\nAI-assisted synthetic exercise; not the owner's unaided performance."),
        (17, "1 / Reproduce a partial publication", "DELIBERATELY BROKEN TEACHING COPY",
         data["steps"][0]["output"], "The teaching copy removes the transaction boundary.\nThe real repository publisher is not modified."),
        (16, "2 / Restore the transaction boundary", "ACTUAL EDIT TO THE ISOLATED COPY",
         patch, "Diff excerpt: BEGIN wraps validation and all table copies;\nCOMMIT publishes the complete version. Full diff is downloadable."),
        (17, "3 / Repeat the crash, then recover", "SAME PROCESS-EXIT BOUNDARY",
         data["steps"][2]["output"], "After the process exits, DuckDB restores the old committed version.\nA successful retry publishes the 7-row candidate."),
        (8, "4 / Check the existing regressions", "CURRENT REPOSITORY PUBLISHER",
         "python -m pytest -q tests/test_publication.py\n\n" + data["steps"][3]["output"],
         "Checks include invalid candidates, mid-copy failures,\npre-commit failures, abrupt exit and legacy-view replacement."),
        (8, "What this proves - and what it does not", "ONE DATABASE / SINGLE WRITER",
         "Observed: the same abrupt exit failed without a transaction\nand preserved all 9 consumer relations after the repair.\n\nTrade-off: staged copies add storage and publication time.\nReaders spanning multiple queries need a read transaction.\n\nNot tested here: power loss, cloud service operation,\nmulti-writer concurrency or real customer traffic.",
         "Inspect the captured transcript, evidence JSON and source.\nReplay timing is edited for readability; no human narration."),
    ]
    concat = []
    cues = ["WEBVTT", ""]
    elapsed = 0
    for i, (seconds, title, label, terminal, caption) in enumerate(slides):
        canvas = Image.new("RGB", (1440, 900), "#101817")
        draw = ImageDraw.Draw(canvas)
        draw.text((64, 37), "VASILEN KANCHEV  /  ENGINEERING EVIDENCE", fill="#8de2bd", font=fonts["label"])
        draw.text((64, 98), title, fill="#f1f5f2", font=fonts["title"])
        draw.text((64, 165), label, fill="#a9bcb3", font=fonts["label"])
        draw.rounded_rectangle((62, 215, 1378, 700), 14, fill="#07100e", outline="#315347", width=2)
        lines = []
        for line in terminal.splitlines():
            lines.extend(textwrap.wrap(line, width=88, replace_whitespace=False, drop_whitespace=False) or [""])
        if len(lines) > 15:
            raise ValueError(f"Slide {i} has {len(lines)} lines; increase room instead of clipping")
        for j, line in enumerate(lines):
            color = "#f3a9a1" if line.startswith(("FAIL", "- ")) else "#d5e8de"
            if line.startswith(("PASS", "+ ")):
                color = "#8de2bd"
            draw.text((87, 238 + j * 29), line, fill=color, font=fonts["mono"])
        for j, line in enumerate(caption.splitlines()):
            draw.text((64, 737 + j * 36), line, fill="#dce7e0", font=fonts["body"])
        draw.text((64, 851), "SYNTHETIC LOCAL EXERCISE  |  AI-ASSISTED CAPTURED-OUTPUT REPLAY", fill="#a9bcb3", font=fonts["label"])
        draw.text((1280, 851), f"{i + 1} / {len(slides)}", fill="#a9bcb3", font=fonts["label"])
        filename = f"frame-{i:02d}.png"
        canvas.save(frame_dir / filename)
        if i == 0:
            canvas.save(output / "poster.png")
        concat.extend([f"file '{filename}'", f"duration {seconds}"])
        start, end = elapsed, elapsed + seconds
        def timestamp(t):
            return f"00:{t // 60:02}:{t % 60:02}.000"
        cues.extend([f"{timestamp(start)} --> {timestamp(end)}", title, caption, ""])
        elapsed = end
    concat.append(f"file '{filename}'")
    (frame_dir / "frames.txt").write_text("\n".join(concat) + "\n", encoding="utf-8")
    subprocess.run([imageio_ffmpeg.get_ffmpeg_exe(), "-y", "-f", "concat", "-safe", "0", "-i",
                    str(frame_dir / "frames.txt"), "-vf", "fps=24,format=yuv420p", "-c:v", "libx264",
                    "-preset", "medium", "-crf", "22", "-movflags", "+faststart", "-t", str(elapsed),
                    str(output / "recovery-walkthrough.mp4")], check=True, capture_output=True)
    (output / "captions.vtt").write_text("\n".join(cues), encoding="utf-8")
    (output / "replay-data.js").write_text("window.RECOVERY_EVIDENCE = " + json.dumps(data) + ";\n", encoding="utf-8")
    media = {"duration_seconds": elapsed, "format": "captioned rendered replay from actual command output; no human screen recording or narration",
             "evidence_sha256": hashlib.sha256(args.evidence.read_bytes()).hexdigest(),
             "files": {p.name: {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                       for p in [output / "recovery-walkthrough.mp4", output / "poster.png", output / "captions.vtt"]}}
    (output / "media-manifest.json").write_text(json.dumps(media, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(media, indent=2))


if __name__ == "__main__":
    main()
