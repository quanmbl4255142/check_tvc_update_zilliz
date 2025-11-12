"""
Test script để kiểm tra Video Processing API
"""

import requests
import time
import json
from rich.console import Console

console = Console()

API_URL = "http://localhost:5000/api/video"
HEALTH_URL = "http://localhost:5000/api/health"

def test_health():
    """Test health endpoint"""
    console.print("[cyan]🔍 Testing health endpoint...[/cyan]")
    try:
        response = requests.get(HEALTH_URL, timeout=5)
        if response.status_code == 200:
            console.print("[green]✅ Health check passed[/green]")
            console.print(json.dumps(response.json(), indent=2))
            return True
        else:
            console.print(f"[red]❌ Health check failed: {response.status_code}[/red]")
            return False
    except Exception as e:
        console.print(f"[red]❌ Health check error: {e}[/red]")
        return False

def test_add_video(video_url: str):
    """Test adding a video"""
    console.print(f"\n[cyan]📹 Testing add video: {video_url[:80]}...[/cyan]")
    try:
        response = requests.post(
            API_URL,
            json={"video_url": video_url},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            console.print("[green]✅ Video sent to Kafka successfully[/green]")
            console.print(json.dumps(result, indent=2))
            return result.get("request_id")
        else:
            console.print(f"[red]❌ Failed to send video: {response.status_code}[/red]")
            console.print(response.text)
            return None
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        return None

def main():
    console.print("[bold cyan]🧪 Video Processing API Test[/bold cyan]")
    console.print("=" * 60)
    
    # Test 1: Health check
    if not test_health():
        console.print("\n[red]❌ Health check failed. Make sure API server is running![/red]")
        console.print("[yellow]💡 Run: python video_api.py[/yellow]")
        return
    
    # Test 2: Add video
    test_videos = [
        "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4",
        # Thêm các video URLs khác để test
    ]
    
    console.print("\n[bold cyan]📤 Testing video submission...[/bold cyan]")
    request_ids = []
    
    for video_url in test_videos:
        request_id = test_add_video(video_url)
        if request_id:
            request_ids.append(request_id)
        time.sleep(1)  # Wait between requests
    
    if request_ids:
        console.print(f"\n[green]✅ Successfully sent {len(request_ids)} video(s) to Kafka[/green]")
        console.print("[yellow]💡 Check video_consumer.py logs to see processing results[/yellow]")
        console.print("[yellow]💡 Wait a few seconds for consumer to process...[/yellow]")
    else:
        console.print("\n[red]❌ Failed to send videos[/red]")
    
    console.print("\n[bold]" + "=" * 60 + "[/bold]")

if __name__ == "__main__":
    main()

