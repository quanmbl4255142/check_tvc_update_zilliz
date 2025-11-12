"""
Video Service: Kiểm tra duplicate trong Milvus và thêm video nếu chưa có
Sử dụng CLIP embedding để tìm duplicates
"""

import os
import sys
import json
import tempfile
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

# Allow override collection name via environment variable
# Default to product_embeddings if not set
DEFAULT_COLLECTION = os.getenv("MILVUS_COLLECTION", "product_embeddings")

from app import embed_image_clip, ensure_dir


class VideoService:
    """Service để kiểm tra và thêm video vào Milvus"""
    
    def __init__(self, collection_name: str = None):
        self.collection_name = collection_name or DEFAULT_COLLECTION
        self.collection = None
        self._connect()
    
    def _connect(self):
        """Kết nối đến Milvus và load collection"""
        try:
            params = get_connection_params()
            connections.connect("default", **params)
            
            if not utility.has_collection(self.collection_name):
                raise ValueError(f"Collection '{self.collection_name}' không tồn tại!")
            
            self.collection = Collection(self.collection_name)
            self.collection.load()
            
            # Kiểm tra embedding dimension
            schema = self.collection.schema
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
            
            print(f"✅ Connected to Milvus collection: {self.collection_name}")
        except Exception as e:
            print(f"❌ Failed to connect to Milvus: {e}")
            raise
    
    def get_collection_count(self) -> int:
        """Lấy số lượng videos hiện tại trong collection"""
        try:
            if self.collection is None:
                self._connect()
            return self.collection.num_entities
        except Exception:
            return 0
    
    def extract_first_frame(self, video_url: str) -> Optional[Image.Image]:
        """
        Extract frame đầu tiên từ video URL
        
        Returns:
            PIL Image hoặc None nếu lỗi
        """
        try:
            # Thử mở trực tiếp từ URL
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
            print(f"⚠️  Error extracting frame: {e}")
            return None
    
    def create_embedding(self, video_url: str) -> Optional[np.ndarray]:
        """
        Tạo CLIP embedding từ video URL (sử dụng frame đầu tiên)
        
        Returns:
            Normalized embedding vector (512 dims) hoặc None nếu lỗi
        """
        try:
            # Extract first frame
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
        threshold: float = None
    ) -> list:
        """
        Tìm duplicates trong Milvus bằng vector similarity search
        
        Returns:
            List of results: [{"id": ..., "distance": ..., "url": ..., "job_id": ...}, ...]
        """
        if self.collection is None:
            self._connect()
        
        top_k = top_k or DEFAULT_TOP_K
        threshold = threshold or DEFAULT_SIMILARITY_THRESHOLD
        
        try:
            # Search in Milvus
            search_params = {
                "metric_type": "IP",  # Inner Product (for normalized vectors = cosine similarity)
                "params": {"nprobe": 16}
            }
            
            results = self.collection.search(
                data=[embedding.tolist()],
                anns_field="embedding",
                param=search_params,
                limit=top_k,
                output_fields=["product_name", "image_url"]  # Schema mới
            )
            
            # Process results
            duplicates = []
            if results and len(results) > 0:
                for hit in results[0]:
                    # IP distance for normalized vectors = cosine similarity
                    # Higher IP = more similar
                    similarity = hit.distance
                    
                    if similarity >= threshold:
                        # Schema: product_name, image_url (không có url, job_id)
                        duplicates.append({
                            "id": hit.id,
                            "similarity": float(similarity),
                            "url": hit.entity.get("image_url", ""),  # Dùng image_url thay vì url
                            "job_id": hit.entity.get("product_name", "")  # Dùng product_name thay vì job_id
                        })
            
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
            # Generate job_id (unique identifier)
            # Format: url_XXXX (4 digits, auto-increment based on collection size)
            current_count = self.collection.num_entities
            job_id = f"url_{current_count:04d}"
            
            # Insert into Milvus
            # Collection schema: id (auto), product_name, image_url, embedding
            # Format: list of lists matching schema field order
            # Extract product name from URL (hoặc dùng job_id làm product_name)
            product_name = job_id  # Hoặc extract từ URL
            
            self.collection.insert([
                [product_name],    # product_name
                [video_url],       # image_url (dùng video_url làm image_url)
                [embedding.tolist()]  # embedding
            ])
            
            # Flush to ensure data is written
            self.collection.flush()
            
            return job_id
            
        except Exception as e:
            print(f"❌ Error adding video to Milvus: {e}")
            return None
    
    def check_and_add_video(
        self,
        video_url: str,
        similarity_threshold: float = None
    ) -> Dict:
        """
        Kiểm tra video có duplicate không, nếu không thì thêm vào Milvus
        
        Returns:
            {
                "status": "success" | "error",
                "is_new": True | False,
                "unique_id": "url_XXXX" | None,
                "similarity": float,
                "message": str,
                "error": str (if error)
            }
        """
        similarity_threshold = similarity_threshold or DEFAULT_SIMILARITY_THRESHOLD
        
        try:
            # Bước 1: Tạo embedding từ video
            embedding = self.create_embedding(video_url)
            if embedding is None:
                return {
                    "status": "error",
                    "is_new": False,
                    "unique_id": None,
                    "similarity": 0.0,
                    "message": "Không thể tạo embedding từ video",
                    "error": "Embedding creation failed"
                }
            
            # Bước 2: Tìm duplicates trong Milvus
            duplicates = self.search_duplicates(
                embedding,
                top_k=DEFAULT_TOP_K,
                threshold=similarity_threshold
            )
            
            if duplicates:
                # Video đã tồn tại (TVC CŨ)
                best_match = duplicates[0]  # Highest similarity
                return {
                    "status": "success",
                    "is_new": False,
                    "unique_id": best_match["job_id"],  # product_name
                    "similarity": best_match["similarity"],
                    "message": f"Video đã tồn tại (similarity: {best_match['similarity']:.4f})",
                    "duplicate_url": best_match["url"]  # image_url
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

