import os
import sys
import io
import base64
import json
import math
import unicodedata
import subprocess
import time
from dataclasses import dataclass
from typing import List, Optional, Dict

import cv2
import numpy as np
from PIL import Image
import click
import pandas as pd
from rich.console import Console
from rich.progress import track

# Optional imports guarded at use

console = Console()

# (Captioning/labeling functionality removed)

# Cache for CLIP embedding model với size limit để tránh memory leak
_CLIP_CACHE: Optional[Dict[str, object]] = None
_MAX_CACHE_SIZE = 3  # Giới hạn số lượng models trong cache (thường chỉ cần 1 model)


@dataclass
class CaptionResult:
    second: int
    image_path: str
    caption: str


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def seconds_floor(duration_seconds: float) -> int:
    if duration_seconds is None or math.isnan(duration_seconds):
        return 0
    return max(0, int(math.floor(duration_seconds)))


def extract_frames_1fps(video_path: str, output_dir: str) -> List[str]:
    """Extract one frame per second from the video and save as PNG files.

    Returns the list of saved image paths ordered by second (0,1,2,...).
    """
    ensure_dir(output_dir)

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
    frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0.0
    duration = frame_count / fps if fps > 0 else 0.0
    total_seconds = seconds_floor(duration)

    saved_paths: List[str] = []

    for sec in track(range(total_seconds), description="Extracting frames (1 fps)"):
        # Position by milliseconds to pick a representative frame at each whole second
        cap.set(cv2.CAP_PROP_POS_MSEC, sec * 1000)
        success, frame = cap.read()
        if not success or frame is None:
            # Fallback: try to set by frame index if ms seek failed
            if fps > 0:
                cap.set(cv2.CAP_PROP_POS_FRAMES, int(sec * fps))
                success, frame = cap.read()
        if not success or frame is None:
            console.print(f"[yellow]Warning: could not read frame at {sec}s[/yellow]")
            continue
        # Convert BGR to RGB and save
        rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        img = Image.fromarray(rgb)
        out_path = os.path.join(output_dir, f"frame_{sec:04d}.png")
        img.save(out_path)
        saved_paths.append(out_path)

    cap.release()
    return saved_paths


# (Captioning, labeling, translation removed)


def _get_clip_model(model_id: str = "openai/clip-vit-base-patch32"):
    """Load and cache CLIP processor/model on CPU/GPU với memory management."""
    global _CLIP_CACHE
    if _CLIP_CACHE and _CLIP_CACHE.get("id") == model_id:
        return _CLIP_CACHE["processor"], _CLIP_CACHE["model"], _CLIP_CACHE["device"]
    try:
        from transformers import AutoProcessor, CLIPModel
        import torch
        import gc
        
        # Clear cache nếu đã đạt limit (thường không cần vì chỉ dùng 1 model)
        if _CLIP_CACHE and _CLIP_CACHE.get("id") != model_id:
            # Clear old model from memory
            if "model" in _CLIP_CACHE:
                del _CLIP_CACHE["model"]
            if "processor" in _CLIP_CACHE:
                del _CLIP_CACHE["processor"]
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            gc.collect()
        
        has_cuda = torch.cuda.is_available()
        device = torch.device("cuda" if has_cuda else "cpu")
        processor = AutoProcessor.from_pretrained(model_id)
        model = CLIPModel.from_pretrained(model_id)
        model.to(device)
        model.eval()
        _CLIP_CACHE = {"id": model_id, "processor": processor, "model": model, "device": device}
        return processor, model, device
    except Exception as e:
        raise RuntimeError(f"Cannot load CLIP model {model_id}: {e}")


def embed_image_clip(image_path: str, model_id: str = "openai/clip-vit-base-patch32") -> List[float]:
    """Compute CLIP image embedding for the given image and return as list of floats."""
    from PIL import Image as PILImage
    import torch
    import numpy as np
    processor, model, device = _get_clip_model(model_id)
    with torch.no_grad():
        image = PILImage.open(image_path).convert("RGB")
        inputs = processor(images=image, return_tensors="pt")
        pixel_values = inputs["pixel_values"].to(device)
        image_features = model.get_image_features(pixel_values=pixel_values)
        image_features = image_features.detach().cpu().numpy()[0]
        # Optionally L2-normalize for cosine usage
        norm = np.linalg.norm(image_features) or 1.0
        image_features = (image_features / norm).astype(np.float32)
        return image_features.tolist()


def embed_image_clip_to_npy(image_path: str, out_path: str, model_id: str = "openai/clip-vit-base-patch32") -> None:
    """Compute CLIP embedding and save to .npy file."""
    import numpy as np
    vec = embed_image_clip(image_path, model_id=model_id)
    arr = np.asarray(vec, dtype=np.float32)
    ensure_dir(os.path.dirname(out_path))
    np.save(out_path, arr)


# ------------------------ CLI ------------------------

@click.command()
@click.option("--input", "input_video", required=False, type=click.Path(exists=True, dir_okay=False), help="Path to input video")
@click.option("--out_dir", default="output", type=click.Path(dir_okay=True, file_okay=False), help="Directory to write frames and captions")
@click.option("--overwrite", is_flag=True, default=False, help="Overwrite existing frame files")
@click.option("--serve/--no-serve", "serve_web", default=True, help="Also start the web UI server (default: on)")
def main(input_video: str, out_dir: str, overwrite: bool, serve_web: bool) -> None:
    console.rule("Video to Step Images + Captions (1 fps)")

    web_proc = None
    if serve_web:
        try:
            web_proc = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(__file__), "web_app.py")])
            console.print("[cyan]Web UI đang chạy tại[/cyan] http://localhost:5000")
            time.sleep(1.0)
        except Exception as e:
            console.print(f"[yellow]Không thể khởi động web UI:[/yellow] {e}")
            web_proc = None

    # Nếu có video đầu vào thì chỉ tách khung hình 1fps; bỏ mô tả và gán nhãn
    if input_video:
        frames_dir = os.path.join(out_dir, "frames")
        ensure_dir(out_dir)
        if overwrite and os.path.isdir(frames_dir):
            for name in os.listdir(frames_dir):
                try:
                    os.remove(os.path.join(frames_dir, name))
                except Exception:
                    pass
        ensure_dir(frames_dir)
        # Extract frames at 1 fps
        frame_paths = extract_frames_1fps(input_video, frames_dir)
        console.print(f"[green]Extracted {len(frame_paths)} frame(s) into {frames_dir}[/green]")
        console.print(f"[bold green]Done[/bold green]. Chỉ tách khung hình, không tạo mô tả hay gán nhãn.")
        if web_proc is not None:
            try:
                web_proc.wait()
            except KeyboardInterrupt:
                pass
    else:
        if web_proc is not None:
            try:
                web_proc.wait()
            except KeyboardInterrupt:
                pass
        else:
            console.print("[yellow]Không có --input và không khởi động được web. Thoát.[/yellow]")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)
