#!/usr/bin/env python3
"""生成测试用 MP4 视频（无外部模型依赖）。"""
import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from PIL import Image, ImageDraw

try:
    import imageio_ffmpeg
    FFMPEG_BIN = imageio_ffmpeg.get_ffmpeg_exe()
except ImportError:
    FFMPEG_BIN = "ffmpeg"


def make_jacket_frame(color: str, frame_idx: int) -> Image.Image:
    """生成一张 224x224 的"穿外套的人"测试帧。"""
    color_map = {"red": (220, 50, 50), "blue": (50, 80, 220), "green": (50, 200, 80)}
    bg = (240, 240, 240)
    img = Image.new("RGB", (224, 224), bg)
    draw = ImageDraw.Draw(img)
    rgb = color_map.get(color, (200, 200, 200))
    # 加一些 frame_idx 相关的微动作（位置偏移）
    offset_x = (frame_idx % 4) - 2
    draw.rectangle([60 + offset_x, 80, 164 + offset_x, 180], fill=rgb)
    draw.ellipse([85 + offset_x, 30, 139 + offset_x, 84], fill=(220, 180, 140))
    draw.line([(115 + offset_x, 80), (115 + offset_x, 180)], fill=(0, 0, 0), width=2)
    return img


def generate_video(scenario: str, output: str, duration_sec: float = 3.0, fps: int = 8):
    total_frames = int(duration_sec * fps)
    # 用项目内目录而非系统 tmp（避免容器 tmpfs 隔离问题）
    work_dir = Path(output).parent / f".{scenario}_frames"
    work_dir.mkdir(parents=True, exist_ok=True)
    try:
        for i in range(total_frames):
            if scenario == "red_consistent":
                color = "red"
            elif scenario == "red_to_blue":
                color = "red" if i < int(total_frames * 0.66) else "blue"
            elif scenario == "red_corrected":
                color = "red"
            elif scenario == "blue_consistent":
                color = "blue"
            else:
                color = "red"
            frame = make_jacket_frame(color, i)
            frame.save(work_dir / f"f_{i:04d}.png")

        cmd = [
            FFMPEG_BIN, "-y", "-loglevel", "error",
            "-framerate", str(fps),
            "-i", str(work_dir / "f_%04d.png"),
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            "-crf", "23",
            output,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"ffmpeg failed: {result.stderr}", file=sys.stderr)
            sys.exit(1)
    finally:
        # 清理工作目录
        for f in work_dir.glob("*.png"):
            f.unlink()
        work_dir.rmdir()
    print(f"Generated: {output} ({total_frames} frames, {duration_sec}s @ {fps}fps)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True,
                        choices=["red_consistent", "red_to_blue", "red_corrected", "blue_consistent"])
    parser.add_argument("--output", required=True)
    parser.add_argument("--duration", type=float, default=3.0)
    parser.add_argument("--fps", type=int, default=8)
    args = parser.parse_args()
    generate_video(args.scenario, args.output, args.duration, args.fps)


if __name__ == "__main__":
    main()
