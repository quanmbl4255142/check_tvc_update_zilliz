"""
Video Service: Kiểm tra duplicate trong Milvus và thêm video nếu chưa có
Sử dụng CLIP embedding để tìm duplicates
"""

import os
import sys
import json
import tempfile
import time
import cv2
import numpy as np
from typing import Dict, Optional, Tuple
from PIL import Image
from datetime import datetime

from pymilvus import (
    connections,
    Collection,
    utility,
)

from milvus_config import (
    get_connection_params,
    COLLECTION_NAME,
    EMBEDDING_DIM,
    DEFAULT_SIMILARITY_THRESHOLD,
    DEFAULT_TOP_K,
    print_config,
)

# Import timeout config
try:
    from timeout_config import (
        VIDEO_EXTRACT_FRAME_TIMEOUT,
        VIDEO_EXTRACT_FRAME_RETRIES,
        MILVUS_INSERT_RETRIES,
        MILVUS_INSERT_RETRY_DELAY,
    )
except ImportError:
    # Fallback nếu không có timeout_config
    VIDEO_EXTRACT_FRAME_TIMEOUT = int(os.getenv("VIDEO_EXTRACT_FRAME_TIMEOUT", "30"))
    VIDEO_EXTRACT_FRAME_RETRIES = int(os.getenv("VIDEO_EXTRACT_FRAME_RETRIES", "2"))
    MILVUS_INSERT_RETRIES = int(os.getenv("MILVUS_INSERT_RETRIES", "3"))
    MILVUS_INSERT_RETRY_DELAY = int(os.getenv("MILVUS_INSERT_RETRY_DELAY", "1"))

# Use COLLECTION_NAME from milvus_config (default: product_embeddings)
# Can be overridden via MILVUS_COLLECTION environment variable
DEFAULT_COLLECTION = COLLECTION_NAME

from app import embed_image_clip, ensure_dir


def is_hls_manifest(url: str) -> bool:
    """Check if URL is an HLS manifest (.m3u8 or /manifest/hls)"""
    url_lower = url.lower()
    return (
        '.m3u8' in url_lower or 
        '/manifest/hls' in url_lower or
        'hls_variant' in url_lower or
        'hls' in url_lower and 'manifest' in url_lower
    )


def extract_frame_from_hls(url: str, output_path: str, timeout: int = 60) -> Optional[Image.Image]:
    """
    Extract first frame from HLS manifest using FFmpeg
    
    Args:
        url: HLS manifest URL
        output_path: Path to save the extracted frame image
        timeout: Timeout in seconds
    
    Returns:
        PIL Image if successful, None otherwise
    """
    try:
        import subprocess
        import shutil
        
        # Check if ffmpeg is available
        if not shutil.which('ffmpeg'):
            return None
        
        # Use FFmpeg to extract first frame from HLS stream
        # -protocol_whitelist: allow https/http/file protocols (fixes "Protocol 'https' not on whitelist")
        cmd = [
            'ffmpeg',
            '-protocol_whitelist', 'file,http,https,tcp,tls,crypto',
            '-i', url,
            '-vframes', '1',
            '-ss', '0',
            '-y',  # overwrite
            '-loglevel', 'error',  # reduce noise
            output_path
        ]
        
        # Run FFmpeg with timeout
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout + 10,  # Add buffer
            check=False
        )
        
        # Check if output file was created and is valid
        if result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            try:
                return Image.open(output_path)
            except Exception:
                return None
        
        return None
        
    except subprocess.TimeoutExpired:
        return None
    except FileNotFoundError:
        # FFmpeg not found
        return None
    except Exception:
        return None


class VideoService:
    """Service để kiểm tra và thêm video vào Milvus"""
    
    # Class-level connection để reuse connection giữa các instances
    _milvus_connected = False
    _connection_alias = "default"
    
    def __init__(self, collection_name: str = None):
        self.collection_name = collection_name or DEFAULT_COLLECTION
        self.collection = None
        self._connect()
    
    def _connect(self):
        """Kết nối đến Milvus và load collection - reuse connection nếu đã có"""
        try:
            # Chỉ connect nếu chưa connect (reuse connection)
            if not VideoService._milvus_connected:
                params = get_connection_params()
                connections.connect(VideoService._connection_alias, **params)
                VideoService._milvus_connected = True
                print(f"✅ Connected to Milvus (connection reused)")
            else:
                # Kiểm tra connection còn hoạt động không
                try:
                    connections.get_connection_addr(VideoService._connection_alias)
                except:
                    # Connection bị mất, reconnect
                    params = get_connection_params()
                    connections.connect(VideoService._connection_alias, **params)
                    print(f"✅ Reconnected to Milvus")
            
            if not utility.has_collection(self.collection_name):
                raise ValueError(f"Collection '{self.collection_name}' không tồn tại!")
            
            # Load collection (có thể cache collection object)
            self.collection = Collection(self.collection_name)
            if not self.collection.has_index():
                print(f"⚠️  Collection {self.collection_name} chưa có index!")
            self.collection.load()
            
            # Kiểm tra embedding dimension và schema fields
            schema = self.collection.schema
            field_names = [f.name for f in schema.fields]
            
            # Detect schema type với validation rõ ràng
            has_product_schema = "product_name" in field_names and "image_url" in field_names
            has_video_schema = "url" in field_names and "job_id" in field_names
            
            if not has_product_schema and not has_video_schema:
                error_msg = (
                    f"⚠️  WARNING: Unknown schema! Fields: {field_names}\n"
                    f"⚠️  Expected: product_name/image_url OR url/job_id\n"
                    f"⚠️  Collection may not work correctly with this schema!"
                )
                print(error_msg)
                # Raise error để force fix schema issue
                # raise ValueError(f"Unsupported schema. Fields: {field_names}")
            
            # Store schema type for later use
            self.has_product_schema = has_product_schema
            self.has_video_schema = has_video_schema
            self.schema_fields = field_names
            
            # Log schema type để debug
            schema_type = "product_embeddings" if has_product_schema else "video_dedup" if has_video_schema else "unknown"
            print(f"📋 Detected schema type: {schema_type}")
            
            # Check embedding dimension
            embedding_field = None
            for field in schema.fields:
                if field.name == "embedding":
                    embedding_field = field
                    break
            
            if embedding_field:
                collection_dim = embedding_field.params.get('dim', 0)
                if collection_dim != EMBEDDING_DIM:
                    print(f"⚠️  WARNING: Collection embedding dim={collection_dim} but CLIP creates dim={EMBEDDING_DIM}")
                    print(f"⚠️  This will cause errors when inserting! Please fix collection dimension.")
            
            print(f"✅ Loaded Milvus collection: {self.collection_name}")
            print(f"   Schema type: {'product_embeddings' if has_product_schema else 'video_dedup' if has_video_schema else 'unknown'}")
        except Exception as e:
            print(f"❌ Failed to connect to Milvus: {e}")
            VideoService._milvus_connected = False  # Reset connection status
            raise
    
    def get_collection_count(self) -> int:
        """Lấy số lượng videos hiện tại trong collection"""
        try:
            if self.collection is None:
                self._connect()
            return self.collection.num_entities
        except Exception:
            return 0
    
    def calculate_frame_quality(self, frame: np.ndarray) -> float:
        """
        Tính toán chất lượng frame dựa trên độ sáng, contrast và sharpness
        Returns: Quality score (0-1, cao hơn = tốt hơn)
        """
        try:
            # Convert to grayscale nếu cần
            if len(frame.shape) == 3:
                gray = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
            else:
                gray = frame
            
            # 1. Độ sáng (brightness) - tránh quá tối hoặc quá sáng
            mean_brightness = np.mean(gray) / 255.0
            brightness_score = 1.0 - abs(mean_brightness - 0.5) * 2  # Tốt nhất ở 0.5 (50% brightness)
            
            # 2. Contrast - độ tương phản
            std_contrast = np.std(gray) / 255.0
            contrast_score = min(std_contrast * 2, 1.0)  # Contrast cao hơn = tốt hơn (max 1.0)
            
            # 3. Sharpness - độ sắc nét (sử dụng Laplacian variance)
            laplacian = cv2.Laplacian(gray, cv2.CV_64F)
            sharpness = laplacian.var()
            sharpness_score = min(sharpness / 1000.0, 1.0)  # Normalize (max 1.0)
            
            # Weighted average
            quality_score = (
                brightness_score * 0.3 +
                contrast_score * 0.4 +
                sharpness_score * 0.3
            )
            
            return float(quality_score)
        except Exception:
            return 0.5  # Default score nếu lỗi
    
    def extract_best_frame(
        self, 
        video_url: str, 
        num_frames: int = 5,
        frame_interval: int = 10,
        timeout_seconds: int = None, 
        max_retries: int = None
    ) -> Optional[Image.Image]:
        """
        Extract nhiều frame và chọn frame có chất lượng tốt nhất
        Giúp xử lý tốt hơn các video có frame đầu tiên tối/không rõ
        
        Args:
            video_url: URL của video
            num_frames: Số lượng frame để thử (mặc định 5)
            frame_interval: Khoảng cách giữa các frame (mặc định 10 frames)
            timeout_seconds: Timeout cho việc download video
            max_retries: Số lần retry tối đa
        
        Returns:
            PIL Image (frame tốt nhất) hoặc None nếu lỗi
        """
        # Sử dụng config nếu không chỉ định
        if timeout_seconds is None:
            timeout_seconds = VIDEO_EXTRACT_FRAME_TIMEOUT
        if max_retries is None:
            max_retries = VIDEO_EXTRACT_FRAME_RETRIES
        
        # Check if URL is HLS manifest - for HLS, just extract first frame
        if is_hls_manifest(video_url):
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
                tmp_path = tmp_file.name
            try:
                frame = extract_frame_from_hls(video_url, tmp_path, timeout=timeout_seconds)
                if frame:
                    return frame
                else:
                    print(f"⚠️  HLS manifest detected but FFmpeg extraction failed: {video_url}")
                    return None
            finally:
                # Clean up temp file
                try:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception:
                    pass
        
        retry_delay = 1
        
        for attempt in range(max_retries + 1):
            try:
                start_time = time.time()
                
                # Thử mở trực tiếp từ URL
                cap = cv2.VideoCapture(video_url)
                
                if not cap.isOpened():
                    if attempt < max_retries:
                        print(f"⚠️  Cannot open video URL (attempt {attempt + 1}/{max_retries + 1}): {video_url}, retrying...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    print(f"⚠️  Cannot open video URL: {video_url}")
                    return None
                
                # Lấy thông tin video
                fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
                total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
                
                # Extract nhiều frame và đánh giá chất lượng
                frames_with_quality = []
                
                for i in range(num_frames):
                    frame_pos = i * frame_interval
                    
                    # Đảm bảo không vượt quá tổng số frame
                    if total_frames > 0 and frame_pos >= total_frames:
                        break
                    
                    # Seek đến frame
                    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_pos)
                    success, frame = cap.read()
                    
                    if success and frame is not None:
                        # Tính chất lượng frame
                        quality = self.calculate_frame_quality(frame)
                        frames_with_quality.append((frame, quality, frame_pos))
                
                cap.release()
                
                # Check timeout
                elapsed = time.time() - start_time
                if elapsed > timeout_seconds:
                    if attempt < max_retries:
                        print(f"⚠️  Video processing timeout (attempt {attempt + 1}/{max_retries + 1}), retrying...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    print(f"⚠️  Video processing timeout after {elapsed:.1f}s")
                    return None
                
                # Chọn frame tốt nhất
                if frames_with_quality:
                    best_frame, best_quality, best_pos = max(frames_with_quality, key=lambda x: x[1])
                    print(f"📸 Selected best frame at position {best_pos} (quality: {best_quality:.3f})")
                    
                    # Convert to PIL Image
                    rgb = cv2.cvtColor(best_frame, cv2.COLOR_BGR2RGB)
                    return Image.fromarray(rgb)
                else:
                    # Fallback: thử frame đầu tiên
                    print(f"⚠️  No frames extracted, trying first frame as fallback...")
                    cap = cv2.VideoCapture(video_url)
                    if cap.isOpened():
                        cap.set(cv2.CAP_PROP_POS_MSEC, 0)
                        success, frame = cap.read()
                        cap.release()
                        if success and frame is not None:
                            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                            return Image.fromarray(rgb)
                    return None
                
            except Exception as e:
                if attempt < max_retries:
                    print(f"⚠️  Error extracting best frame (attempt {attempt + 1}/{max_retries + 1}): {e}, retrying...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                print(f"⚠️  Error extracting best frame: {e}")
                # Fallback to first frame method
                return self.extract_first_frame(video_url, timeout_seconds, max_retries)
        
        return None
    
    def extract_first_frame(self, video_url: str, timeout_seconds: int = None, max_retries: int = None) -> Optional[Image.Image]:
        """
        Extract frame đầu tiên từ video URL với timeout và retry mechanism
        
        Args:
            video_url: URL của video
            timeout_seconds: Timeout cho việc download video (mặc định từ config)
            max_retries: Số lần retry tối đa (mặc định từ config)
        
        Returns:
            PIL Image hoặc None nếu lỗi
        """
        # Sử dụng config nếu không chỉ định
        if timeout_seconds is None:
            timeout_seconds = VIDEO_EXTRACT_FRAME_TIMEOUT
        if max_retries is None:
            max_retries = VIDEO_EXTRACT_FRAME_RETRIES
        
        # Check if URL is HLS manifest
        if is_hls_manifest(video_url):
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
                tmp_path = tmp_file.name
            try:
                frame = extract_frame_from_hls(video_url, tmp_path, timeout=timeout_seconds)
                if frame:
                    return frame
                else:
                    print(f"⚠️  HLS manifest detected but FFmpeg extraction failed: {video_url}")
                    return None
            finally:
                # Clean up temp file
                try:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception:
                    pass
        
        retry_delay = 1
        
        for attempt in range(max_retries + 1):
            try:
                start_time = time.time()
                
                # Thử mở trực tiếp từ URL
                cap = cv2.VideoCapture(video_url)
                
                if not cap.isOpened():
                    if attempt < max_retries:
                        print(f"⚠️  Cannot open video URL (attempt {attempt + 1}/{max_retries + 1}): {video_url}, retrying...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    print(f"⚠️  Cannot open video URL: {video_url}")
                    return None
                
                # Set timeout cho read operation (OpenCV không hỗ trợ timeout trực tiếp)
                # Sử dụng cách check thời gian thủ công
                cap.set(cv2.CAP_PROP_POS_MSEC, 0)
                
                # Check timeout trước khi read
                elapsed = time.time() - start_time
                if elapsed > timeout_seconds:
                    cap.release()
                    if attempt < max_retries:
                        print(f"⚠️  Video open timeout (attempt {attempt + 1}/{max_retries + 1}), retrying...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    print(f"⚠️  Video open timeout after {elapsed:.1f}s")
                    return None
                
                # Read frame với timeout check
                success, frame = cap.read()
                elapsed = time.time() - start_time
                
                cap.release()
                
                # Check timeout sau khi read
                if elapsed > timeout_seconds:
                    if attempt < max_retries:
                        print(f"⚠️  Video processing timeout (attempt {attempt + 1}/{max_retries + 1}), retrying...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    print(f"⚠️  Video processing timeout after {elapsed:.1f}s")
                    return None
                
                if success and frame is not None:
                    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    return Image.fromarray(rgb)
                
                # Frame read failed, retry if possible
                if attempt < max_retries:
                    print(f"⚠️  Failed to read frame (attempt {attempt + 1}/{max_retries + 1}), retrying...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                
                return None
            except Exception as e:
                if attempt < max_retries:
                    print(f"⚠️  Error extracting frame (attempt {attempt + 1}/{max_retries + 1}): {e}, retrying...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                print(f"⚠️  Error extracting frame: {e}")
                return None
        
        return None
    
    def create_embedding(self, video_url: str, use_best_frame: bool = True) -> Optional[np.ndarray]:
        """
        Tạo CLIP embedding từ video URL
        
        Args:
            video_url: URL của video
            use_best_frame: Nếu True, extract nhiều frame và chọn frame tốt nhất
                          Nếu False, chỉ dùng frame đầu tiên (nhanh hơn nhưng kém chính xác hơn)
        
        Returns:
            Normalized embedding vector (512 dims) hoặc None nếu lỗi
        """
        try:
            # Extract frame (best frame hoặc first frame)
            if use_best_frame:
                frame = self.extract_best_frame(video_url)
            else:
                frame = self.extract_first_frame(video_url)
            
            if frame is None:
                return None
            
            # Save to temp file
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
                tmp_path = tmp_file.name
                frame.save(tmp_path)
            
            try:
                # Create embedding
                embedding = embed_image_clip(tmp_path, model_id="openai/clip-vit-base-patch32")
                
                # Convert to numpy array and normalize
                vec = np.array(embedding, dtype=np.float32)
                norm = np.linalg.norm(vec)
                if norm > 0:
                    vec = vec / norm
                
                return vec
            finally:
                # Clean up temp file
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
            
        except Exception as e:
            print(f"❌ Error creating embedding: {e}")
            return None
    
    def search_duplicates(
        self,
        embedding: np.ndarray,
        top_k: int = None,
        threshold: float = None,
        use_fallback_threshold: bool = True
    ) -> list:
        """
        Tìm duplicates trong Milvus bằng vector similarity search
        Hỗ trợ dynamic threshold fallback để phát hiện duplicate tốt hơn
        khi video khác độ phân giải/kích thước
        
        Args:
            embedding: Embedding vector để search
            top_k: Số lượng kết quả tối đa
            threshold: Similarity threshold (mặc định từ config)
            use_fallback_threshold: Nếu True, thử lại với threshold thấp hơn nếu không tìm thấy
        
        Returns:
            List of results: [{"id": ..., "distance": ..., "url": ..., "job_id": ...}, ...]
        """
        if self.collection is None:
            self._connect()
        
        top_k = top_k or DEFAULT_TOP_K
        primary_threshold = threshold or DEFAULT_SIMILARITY_THRESHOLD
        fallback_threshold = max(0.98, primary_threshold - 0.015)  # Giảm 0.015 nhưng không dưới 0.98
        
        try:
            # Search in Milvus
            search_params = {
                "metric_type": "IP",  # Inner Product (for normalized vectors = cosine similarity)
                "params": {"nprobe": 16}
            }
            
            # Determine output fields based on schema
            if hasattr(self, 'has_product_schema') and self.has_product_schema:
                output_fields = ["product_name", "image_url"]
            elif hasattr(self, 'has_video_schema') and self.has_video_schema:
                output_fields = ["url", "job_id"]
            else:
                # Try to detect from collection schema
                schema = self.collection.schema
                field_names = [f.name for f in schema.fields]
                if "product_name" in field_names and "image_url" in field_names:
                    output_fields = ["product_name", "image_url"]
                elif "url" in field_names and "job_id" in field_names:
                    output_fields = ["url", "job_id"]
                else:
                    # Fallback: try common fields
                    output_fields = ["url", "job_id"] if "url" in field_names else ["product_name", "image_url"]
            
            results = self.collection.search(
                data=[embedding.tolist()],
                anns_field="embedding",
                param=search_params,
                limit=top_k,
                output_fields=output_fields
            )
            
            # Process results
            duplicates = []
            if results and len(results) > 0:
                for hit in results[0]:
                    # IP distance for normalized vectors = cosine similarity
                    # Higher IP = more similar
                    similarity = hit.distance
                    
                    if similarity >= primary_threshold:
                        # Map fields based on schema type
                        if hasattr(self, 'has_product_schema') and self.has_product_schema:
                            # Schema: product_name, image_url
                            duplicates.append({
                                "id": hit.id,
                                "similarity": float(similarity),
                                "url": hit.entity.get("image_url", ""),
                                "job_id": hit.entity.get("product_name", "")
                            })
                        else:
                            # Schema: url, job_id
                            duplicates.append({
                                "id": hit.id,
                                "similarity": float(similarity),
                                "url": hit.entity.get("url", ""),
                                "job_id": hit.entity.get("job_id", "")
                            })
            
            # Dynamic threshold fallback: Nếu không tìm thấy với threshold cao,
            # thử lại với threshold thấp hơn (để phát hiện duplicate khi khác độ phân giải)
            if not duplicates and use_fallback_threshold and fallback_threshold < primary_threshold:
                print(f"🔍 Không tìm thấy duplicate với threshold {primary_threshold:.4f}, thử lại với threshold {fallback_threshold:.4f}...")
                
                # Search lại với threshold thấp hơn
                fallback_results = self.collection.search(
                    data=[embedding.tolist()],
                    anns_field="embedding",
                    param=search_params,
                    limit=top_k,
                    output_fields=output_fields
                )
                
                if fallback_results and len(fallback_results) > 0:
                    for hit in fallback_results[0]:
                        similarity = hit.distance
                        
                        if similarity >= fallback_threshold:
                            # Map fields based on schema type
                            if hasattr(self, 'has_product_schema') and self.has_product_schema:
                                duplicates.append({
                                    "id": hit.id,
                                    "similarity": float(similarity),
                                    "url": hit.entity.get("image_url", ""),
                                    "job_id": hit.entity.get("product_name", ""),
                                    "fallback_match": True  # Đánh dấu là match từ fallback threshold
                                })
                            else:
                                duplicates.append({
                                    "id": hit.id,
                                    "similarity": float(similarity),
                                    "url": hit.entity.get("url", ""),
                                    "job_id": hit.entity.get("job_id", ""),
                                    "fallback_match": True  # Đánh dấu là match từ fallback threshold
                                })
                    
                    if duplicates:
                        print(f"✅ Tìm thấy {len(duplicates)} duplicate(s) với fallback threshold {fallback_threshold:.4f}")
            
            return duplicates
            
        except Exception as e:
            print(f"❌ Error searching duplicates: {e}")
            return []
    
    def add_video_to_milvus(
        self,
        video_url: str,
        embedding: np.ndarray
    ) -> Optional[str]:
        """
        Thêm video vào Milvus collection
        
        Returns:
            unique_id (job_id) hoặc None nếu lỗi
        """
        if self.collection is None:
            self._connect()
        
        try:
            # Generate job_id (unique identifier) - Fix race condition
            # Sử dụng UUID để đảm bảo unique, sau đó format lại nếu cần
            import uuid
            import time
            
            # Tạo unique ID dựa trên timestamp + UUID để tránh collision
            # Format: url_TIMESTAMP_UUID (đảm bảo unique ngay cả khi concurrent)
            timestamp = int(time.time() * 1000)  # milliseconds
            unique_suffix = str(uuid.uuid4())[:8]  # 8 chars từ UUID
            job_id = f"url_{timestamp}_{unique_suffix}"
            
            # Alternative: Nếu muốn giữ format url_XXXX, sử dụng Redis atomic counter
            # Nhưng format mới này đảm bảo unique hơn và không cần external dependency
            
            # Insert into Milvus với retry mechanism
            # Detect schema and insert accordingly
            schema = self.collection.schema
            # Get fields in order (excluding auto-id)
            insert_fields = [f.name for f in schema.fields if f.name != "id"]
            
            # Build insert data based on schema
            insert_data = []
            for field_name in insert_fields:
                if field_name == "product_name":
                    insert_data.append([job_id])
                elif field_name == "image_url":
                    insert_data.append([video_url])
                elif field_name == "url":
                    insert_data.append([video_url])
                elif field_name == "job_id":
                    insert_data.append([job_id])
                elif field_name == "embedding":
                    insert_data.append([embedding.tolist()])
                else:
                    # Unknown field - use empty string or default
                    print(f"⚠️  WARNING: Unknown field '{field_name}' in schema, using empty string")
                    insert_data.append([""])
            
            if not insert_data:
                raise ValueError(f"Cannot determine insert fields for schema: {[f.name for f in schema.fields]}")
            
            # Retry insert với exponential backoff (sử dụng config)
            max_retries = MILVUS_INSERT_RETRIES
            retry_delay = MILVUS_INSERT_RETRY_DELAY
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    self.collection.insert(insert_data)
                    # Flush to ensure data is written
                    self.collection.flush()
                    return job_id
                except Exception as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        print(f"⚠️  Insert attempt {attempt + 1} failed: {e}, retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        raise
            
            # Should not reach here, but just in case
            if last_error:
                raise last_error
            return job_id
            
        except Exception as e:
            print(f"❌ Error adding video to Milvus: {e}")
            return None
    
    def check_and_add_video(
        self,
        video_url: str,
        similarity_threshold: float = None,
        use_best_frame: bool = True
    ) -> Dict:
        """
        Kiểm tra video có duplicate không, nếu không thì thêm vào Milvus
        Cải thiện để xử lý tốt hơn các video khác độ phân giải/kích thước
        
        Args:
            video_url: URL của video
            similarity_threshold: Threshold để phát hiện duplicate (mặc định từ config)
            use_best_frame: Nếu True, extract nhiều frame và chọn frame tốt nhất
        
        Returns:
            {
                "status": "success" | "error",
                "is_new": True | False,
                "unique_id": "url_XXXX" | None,
                "similarity": float,
                "message": str,
                "error": str (if error),
                "fallback_match": bool (nếu dùng fallback threshold)
            }
        """
        similarity_threshold = similarity_threshold or DEFAULT_SIMILARITY_THRESHOLD
        
        try:
            # Bước 1: Tạo embedding từ video (sử dụng best frame để tăng độ chính xác)
            embedding = self.create_embedding(video_url, use_best_frame=use_best_frame)
            if embedding is None:
                return {
                    "status": "error",
                    "is_new": False,
                    "unique_id": None,
                    "similarity": 0.0,
                    "message": "Không thể tạo embedding từ video",
                    "error": "Embedding creation failed"
                }
            
            # Bước 2: Tìm duplicates trong Milvus (với fallback threshold)
            duplicates = self.search_duplicates(
                embedding,
                top_k=DEFAULT_TOP_K,
                threshold=similarity_threshold,
                use_fallback_threshold=True  # Bật fallback để phát hiện duplicate tốt hơn
            )
            
            if duplicates:
                # Video đã tồn tại (TVC CŨ)
                best_match = duplicates[0]  # Highest similarity
                is_fallback = best_match.get("fallback_match", False)
                
                return {
                    "status": "success",
                    "is_new": False,
                    "unique_id": best_match["job_id"],  # product_name
                    "similarity": best_match["similarity"],
                    "message": f"Video đã tồn tại (similarity: {best_match['similarity']:.4f})" + 
                              (" - Phát hiện bằng fallback threshold" if is_fallback else ""),
                    "duplicate_url": best_match["url"],  # image_url
                    "fallback_match": is_fallback
                }
            else:
                # Video mới (TVC MỚI) - Thêm vào Milvus
                unique_id = self.add_video_to_milvus(video_url, embedding)
                
                if unique_id:
                    return {
                        "status": "success",
                        "is_new": True,
                        "unique_id": unique_id,
                        "similarity": 0.0,
                        "message": "Video mới đã được thêm vào Milvus"
                    }
                else:
                    return {
                        "status": "error",
                        "is_new": False,
                        "unique_id": None,
                        "similarity": 0.0,
                        "message": "Không thể thêm video vào Milvus",
                        "error": "Milvus insert failed"
                    }
        
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                "status": "error",
                "is_new": False,
                "unique_id": None,
                "similarity": 0.0,
                "message": f"Lỗi khi xử lý video: {str(e)}",
                "error": str(e)
            }


if __name__ == "__main__":
    # Test service
    print_config()
    
    # Test video URL
    test_url = "https://example.com/video.mp4"
    
    service = VideoService()
    result = service.check_and_add_video(test_url)
    
    print("\n" + "=" * 60)
    print("Test Result:")
    print(json.dumps(result, indent=2))
    print("=" * 60)

