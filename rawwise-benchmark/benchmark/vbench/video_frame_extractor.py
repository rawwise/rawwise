#!/usr/bin/env python3
"""
ffmpeg 视频抽帧工具：将 .mp4 → N 张关键帧图片。

用法：
  python video_frame_extractor.py --video <path.mp4> --output-dir <dir> [--num-frames 8]

输出：
  {output_dir}/frame_0000.jpg
  {output_dir}/frame_0001.jpg
  ...
  {output_dir}/frames.json   ← {"frame_count": N, "frame_rate": fps, "duration": sec}
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

try:
    import imageio_ffmpeg
    FFMPEG_BIN = imageio_ffmpeg.get_ffmpeg_exe()
    # ffprobe 也尝试取 imageio_ffmpeg 同目录
    _ff_dir = Path(FFMPEG_BIN).parent
    _candidate = _ff_dir / "ffprobe"
    FFPROBE_BIN = str(_candidate) if _candidate.exists() else "ffprobe"
except ImportError:
    FFMPEG_BIN = "ffmpeg"
    FFPROBE_BIN = "ffprobe"


def probe_video(video_path: str) -> dict:
    """用 ffmpeg 自身（输出到 /dev/null）获取视频元数据，避免依赖 ffprobe。"""
    cmd = [FFMPEG_BIN, "-i", video_path, "-f", "null", "-"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    # ffmpeg 会把元信息打到 stderr
    stderr = result.stderr
    duration = 0.0
    fps = 30.0
    width = 0
    height = 0
    # 解析 "Duration: 00:00:03.00, ..."
    import re
    m = re.search(r"Duration:\s*(\d+):(\d+):(\d+\.\d+)", stderr)
    if m:
        h, mi, s = m.groups()
        duration = int(h) * 3600 + int(mi) * 60 + float(s)
    # 解析 "Stream #0:0: Video: ... 224x224, ... 8 fps"
    m = re.search(r"(\d+)x(\d+)[^,]*", stderr)
    if m:
        width, height = int(m.group(1)), int(m.group(2))
    m = re.search(r"(\d+(?:\.\d+)?)\s*fps", stderr)
    if m:
        fps = float(m.group(1))
    return {
        "duration": duration,
        "frame_rate": fps,
        "nb_frames": int(duration * fps) if duration > 0 else None,
        "width": width,
        "height": height,
    }


def extract_frames(video_path: str, output_dir: str, num_frames: int = 8) -> dict:
    """均匀采样 num_frames 帧到 output_dir/frame_NNNN.jpg。"""
    out = Path(output_dir)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    info = probe_video(video_path)
    duration = info["duration"]
    if duration <= 0:
        raise RuntimeError(f"Invalid video duration: {duration}")

    if num_frames <= 1:
        timestamps = [duration / 2.0]
    else:
        step = duration / num_frames
        timestamps = [step * (i + 0.5) for i in range(num_frames)]

    frame_paths = []
    for i, ts in enumerate(timestamps):
        frame_path = out / f"frame_{i:04d}.jpg"
        cmd = [
            FFMPEG_BIN, "-y", "-loglevel", "error",
            "-ss", f"{ts:.3f}",
            "-i", video_path,
            "-frames:v", "1",
            "-q:v", "2",
            str(frame_path),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            raise RuntimeError(f"ffmpeg failed at ts={ts:.3f}: {result.stderr}")
        if not frame_path.exists():
            raise RuntimeError(f"Frame not produced at ts={ts:.3f}")
        frame_paths.append(str(frame_path))

    metadata = {
        "video_path": str(Path(video_path).resolve()),
        "output_dir": str(out.resolve()),
        "frame_count": len(frame_paths),
        "frame_rate": info["frame_rate"],
        "duration": info["duration"],
        "width": info["width"],
        "height": info["height"],
        "timestamps": [round(t, 3) for t in timestamps],
        "frame_paths": frame_paths,
    }
    with open(out / "frames.json", "w") as fp:
        json.dump(metadata, fp, indent=2)
    return metadata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--video", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--num-frames", type=int, default=8)
    args = parser.parse_args()

    if not os.path.exists(args.video):
        print(f"Video not found: {args.video}", file=sys.stderr)
        sys.exit(1)

    meta = extract_frames(args.video, args.output_dir, args.num_frames)
    print(json.dumps(meta))


if __name__ == "__main__":
    main()
