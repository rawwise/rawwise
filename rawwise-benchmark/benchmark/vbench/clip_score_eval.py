#!/usr/bin/env python3
"""
真实 CLIP score 评价脚本（支持图像和视频输入）。

新增能力：
  - --video <path.mp4>: 抽帧 N 张 → 逐帧 CLIP 评价 → 聚合分数
  - --num-frames N: 抽帧数量（默认 8）
  - --reference-video <path>: 参考视频（subject_consistency 用第一帧）

输出新增字段：
  - temporal_consistency: 相邻帧 CLIP image embedding 余弦相似度均值
  - frame_count: 实际评价的帧数

调用示例：
  python clip_score_eval.py --image synth:red_jacket --prompt "..."
  python clip_score_eval.py --video benchmark/vbench/videos/baseline/p1_run_0.mp4 --prompt "..."
"""

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path

os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")

import torch
from PIL import Image, ImageDraw
from transformers import CLIPModel, CLIPProcessor

# Import frame extractor
sys.path.insert(0, str(Path(__file__).parent))
from video_frame_extractor import extract_frames

MODEL_ID = "openai/clip-vit-base-patch32"
AESTHETIC_REFERENCES = [
    "a beautiful photograph",
    "a high quality image",
    "a professionally taken picture",
    "a sharp, well-composed photo",
]
LOW_QUALITY_REFERENCES = [
    "a blurry image",
    "a low quality photo",
    "a poorly composed picture",
    "a distorted image",
]


def load_model(device: str):
    model = CLIPModel.from_pretrained(MODEL_ID)
    processor = CLIPProcessor.from_pretrained(MODEL_ID)
    model = model.to(device).eval()
    return model, processor


def compute_image_text_similarity(model, processor, image: Image.Image, text: str, device: str) -> float:
    inputs = processor(text=[text], images=image, return_tensors="pt", padding=True)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = model(**inputs)
    img_emb = outputs.image_embeds / outputs.image_embeds.norm(dim=-1, keepdim=True)
    txt_emb = outputs.text_embeds / outputs.text_embeds.norm(dim=-1, keepdim=True)
    return (img_emb @ txt_emb.T).squeeze().item()


def compute_aesthetic_score(model, processor, image: Image.Image, device: str) -> float:
    all_refs = AESTHETIC_REFERENCES + LOW_QUALITY_REFERENCES
    inputs = processor(text=all_refs, images=image, return_tensors="pt", padding=True)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = model(**inputs)
    img_emb = outputs.image_embeds / outputs.image_embeds.norm(dim=-1, keepdim=True)
    txt_emb = outputs.text_embeds / outputs.text_embeds.norm(dim=-1, keepdim=True)
    sims = (img_emb @ txt_emb.T).squeeze()
    pos = sims[: len(AESTHETIC_REFERENCES)].mean().item()
    neg = sims[len(AESTHETIC_REFERENCES):].mean().item()
    return max(0.0, min(1.0, (pos - neg + 0.3) / 0.6))


def get_image_embedding(model, processor, image: Image.Image, device: str):
    inputs = processor(images=image, return_tensors="pt")
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = model.vision_model(**inputs)
        pooled = outputs.pooler_output
        embed = model.visual_projection(pooled)
    return embed / embed.norm(dim=-1, keepdim=True)


def compute_pair_similarity(emb1, emb2) -> float:
    sim = (emb1 @ emb2.T).item()
    return max(0.0, min(1.0, (sim + 1.0) / 2.0))


def evaluate_video(model, processor, frames, prompt: str, reference_image: Image.Image, device: str) -> dict:
    """对视频帧逐帧评价并聚合。"""
    text_sims = []
    aesthetics = []
    embeddings = []

    for frame in frames:
        text_sims.append(compute_image_text_similarity(model, processor, frame, prompt, device))
        aesthetics.append(compute_aesthetic_score(model, processor, frame, device))
        embeddings.append(get_image_embedding(model, processor, frame, device))

    # Temporal consistency = avg cosine similarity of adjacent frames
    if len(embeddings) >= 2:
        temporal_sims = []
        for i in range(len(embeddings) - 1):
            temporal_sims.append(compute_pair_similarity(embeddings[i], embeddings[i + 1]))
        temporal_consistency = sum(temporal_sims) / len(temporal_sims)
    else:
        temporal_consistency = 1.0

    # Subject consistency = avg sim(each frame, reference)
    subject_consistency = None
    if reference_image is not None:
        ref_emb = get_image_embedding(model, processor, reference_image, device)
        subj_sims = [compute_pair_similarity(emb, ref_emb) for emb in embeddings]
        subject_consistency = sum(subj_sims) / len(subj_sims)

    return {
        "clip_text_image_similarity": round(sum(text_sims) / len(text_sims), 4),
        "clip_aesthetic_score": round(sum(aesthetics) / len(aesthetics), 4),
        "temporal_consistency": round(temporal_consistency, 4),
        "subject_consistency_score": round(subject_consistency, 4) if subject_consistency is not None else None,
        "frame_count": len(frames),
        "per_frame_text_sim": [round(s, 4) for s in text_sims],
    }


def evaluate_image(model, processor, image: Image.Image, prompt: str,
                    reference: Image.Image, device: str) -> dict:
    text_sim = compute_image_text_similarity(model, processor, image, prompt, device)
    aesthetic = compute_aesthetic_score(model, processor, image, device)
    subject = None
    if reference is not None:
        emb1 = get_image_embedding(model, processor, image, device)
        emb2 = get_image_embedding(model, processor, reference, device)
        subject = compute_pair_similarity(emb1, emb2)
    return {
        "clip_text_image_similarity": round(text_sim, 4),
        "clip_aesthetic_score": round(aesthetic, 4),
        "subject_consistency_score": round(subject, 4) if subject is not None else None,
        "frame_count": 1,
    }


def create_sample_image(color: str) -> Image.Image:
    color_map = {
        "red_jacket": (220, 50, 50), "blue_jacket": (50, 80, 220),
        "horse": (120, 80, 50), "noise": (128, 128, 128),
    }
    rgb = color_map.get(color, (200, 200, 200))
    img = Image.new("RGB", (224, 224), rgb)
    draw = ImageDraw.Draw(img)
    if color == "red_jacket":
        draw.rectangle([60, 80, 164, 180], fill=(180, 30, 30))
        draw.ellipse([85, 30, 139, 84], fill=(220, 180, 140))
    elif color == "blue_jacket":
        draw.rectangle([60, 80, 164, 180], fill=(30, 50, 180))
        draw.ellipse([85, 30, 139, 84], fill=(220, 180, 140))
    elif color == "horse":
        draw.ellipse([40, 80, 184, 160], fill=(90, 60, 30))
        draw.rectangle([150, 60, 180, 100], fill=(80, 50, 25))
    return img


def load_image_spec(spec: str) -> Image.Image:
    if spec is None:
        return None
    if spec.startswith("synth:"):
        return create_sample_image(spec.split(":", 1)[1])
    return Image.open(spec).convert("RGB")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", help="Image path or 'synth:<keyword>'")
    parser.add_argument("--video", help="Video path (.mp4)")
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--reference", help="Reference image path or 'synth:<keyword>'")
    parser.add_argument("--reference-video", help="Reference video (uses first frame)")
    parser.add_argument("--num-frames", type=int, default=8)
    parser.add_argument("--device", default="auto", choices=["auto", "mps", "cpu"])
    args = parser.parse_args()

    if not args.image and not args.video:
        print("Error: must provide --image or --video", file=sys.stderr)
        sys.exit(1)

    device = args.device
    if device == "auto":
        device = "mps" if torch.backends.mps.is_available() else "cpu"

    model, processor = load_model(device)

    reference = None
    if args.reference:
        reference = load_image_spec(args.reference)
    elif args.reference_video:
        ref_dir = Path(args.reference_video).parent / f".{Path(args.reference_video).stem}_refframes"
        ref_dir.mkdir(parents=True, exist_ok=True)
        ref_meta = extract_frames(args.reference_video, str(ref_dir), num_frames=1)
        reference = Image.open(ref_meta["frame_paths"][0]).convert("RGB")

    start = time.time()
    if args.video:
        frames_dir = Path(args.video).parent / f".{Path(args.video).stem}_frames"
        frames_dir.mkdir(parents=True, exist_ok=True)
        meta = extract_frames(args.video, str(frames_dir), num_frames=args.num_frames)
        frames = [Image.open(p).convert("RGB") for p in meta["frame_paths"]]
        result = evaluate_video(model, processor, frames, args.prompt, reference, device)
        result["video_path"] = args.video
        result["video_duration_sec"] = meta["duration"]
        result["video_frame_rate"] = meta["frame_rate"]
    else:
        image = load_image_spec(args.image)
        result = evaluate_image(model, processor, image, args.prompt, reference, device)
        result["image_path"] = args.image

    result["eval_latency_ms"] = int((time.time() - start) * 1000)
    result["model_id"] = MODEL_ID
    result["device"] = device
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
