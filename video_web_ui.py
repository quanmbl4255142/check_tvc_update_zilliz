"""
Web UI để gửi video URL và xem kết quả
Flask app với HTML/CSS/JS interface
"""

import os
import json
import requests
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response
from flask_cors import CORS
from rich.console import Console

console = Console()

app = Flask(__name__)
CORS(app)

# API configuration
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:5000")
API_VIDEO_ENDPOINT = f"{API_BASE_URL}/api/video"
API_HEALTH_ENDPOINT = f"{API_BASE_URL}/api/health"

# Redis UI
REDIS_UI_URL = os.getenv("REDIS_UI_URL", "http://localhost:8081")

@app.route('/')
def index():
    """Main page"""
    return render_template('index.html', 
                         api_url=API_BASE_URL,
                         redis_ui_url=REDIS_UI_URL)

@app.route('/api/submit', methods=['POST'])
def submit_video():
    """Proxy endpoint để gửi video đến API"""
    try:
        data = request.get_json()
        video_url = data.get('video_url', '').strip()
        
        if not video_url:
            return jsonify({
                "status": "error",
                "message": "Video URL không được để trống"
            }), 400
        
        # Forward request to video API
        response = requests.post(
            API_VIDEO_ENDPOINT,
            json={"video_url": video_url},
            timeout=30  # Tăng timeout lên 30 giây vì Kafka có thể mất thời gian
        )
        
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            return jsonify({
                "status": "error",
                "message": f"API error: {response.status_code}",
                "details": response.text
            }), response.status_code
            
    except requests.exceptions.ConnectionError:
        return jsonify({
            "status": "error",
            "message": "Không thể kết nối đến API server. Đảm bảo video_api.py đang chạy!"
        }), 503
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi: {str(e)}"
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Check API health"""
    try:
        response = requests.get(API_HEALTH_ENDPOINT, timeout=5)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Không thể kết nối đến API: {str(e)}"
        }), 503

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get statistics (placeholder - có thể kết nối Redis trực tiếp)"""
    try:
        import redis
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True
        )
        
        total_before = redis_client.get("stats:total_before") or "0"
        total_after = redis_client.get("stats:total_after") or "0"
        total_added = redis_client.get("stats:total_added") or "0"
        total_duplicates = redis_client.get("stats:total_duplicates") or "0"
        
        return jsonify({
            "status": "success",
            "stats": {
                "total_before": int(total_before),
                "total_after": int(total_after),
                "total_added": int(total_added),
                "total_duplicates": int(total_duplicates)
            }
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Không thể lấy stats: {str(e)}"
        }), 500

if __name__ == '__main__':
    port = int(os.getenv("WEB_UI_PORT", "5001"))
    host = os.getenv("WEB_UI_HOST", "0.0.0.0")
    
    console.print(f"[bold cyan]🌐 Starting Video Processing Web UI on {host}:{port}[/bold cyan]")
    console.print(f"[cyan]📡 API: {API_BASE_URL}[/cyan]")
    console.print(f"[cyan]🔴 Redis UI: {REDIS_UI_URL}[/cyan]")
    
    app.run(host=host, port=port, debug=True)

