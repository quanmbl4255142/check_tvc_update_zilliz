"""
Script để reset Kafka consumer offset về latest
Sử dụng khi consumer đang đọc từ offset cũ và không nhận được message mới
"""

import os
import subprocess
import sys
from rich.console import Console

console = Console()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "video_processing")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "video_processor_group")

def reset_offset_to_latest():
    """Reset consumer offset về latest"""
    console.print("[bold cyan]🔄 Resetting Kafka Consumer Offset[/bold cyan]")
    console.print("=" * 60)
    console.print(f"[cyan]Bootstrap Server: {KAFKA_BOOTSTRAP_SERVERS}[/cyan]")
    console.print(f"[cyan]Topic: {KAFKA_TOPIC}[/cyan]")
    console.print(f"[cyan]Group ID: {KAFKA_GROUP_ID}[/cyan]")
    console.print("=" * 60)
    
    # Command để reset offset
    cmd = [
        "kafka-consumer-groups",
        "--bootstrap-server", KAFKA_BOOTSTRAP_SERVERS,
        "--group", KAFKA_GROUP_ID,
        "--topic", KAFKA_TOPIC,
        "--reset-offsets",
        "--to-latest",
        "--execute"
    ]
    
    try:
        console.print(f"\n[yellow]Running command: {' '.join(cmd)}[/yellow]\n")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        console.print(f"[green]✅ Success![/green]")
        console.print(result.stdout)
        console.print(f"\n[green]✅ Consumer offset đã được reset về latest[/green]")
        console.print(f"[yellow]💡 Bây giờ consumer sẽ chỉ đọc message mới[/yellow]")
    except subprocess.CalledProcessError as e:
        console.print(f"[red]❌ Error resetting offset:[/red]")
        console.print(f"[red]{e.stderr}[/red]")
        console.print(f"\n[yellow]💡 Có thể Kafka tools chưa được cài đặt hoặc consumer group không tồn tại[/yellow]")
        console.print(f"[yellow]   Thử chạy: docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group {KAFKA_GROUP_ID} --topic {KAFKA_TOPIC} --reset-offsets --to-latest --execute[/yellow]")
        sys.exit(1)
    except FileNotFoundError:
        console.print(f"[red]❌ kafka-consumer-groups không tìm thấy[/red]")
        console.print(f"[yellow]💡 Nếu dùng Docker, chạy lệnh này trong container:[/yellow]")
        console.print(f"[cyan]docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group {KAFKA_GROUP_ID} --topic {KAFKA_TOPIC} --reset-offsets --to-latest --execute[/cyan]")
        sys.exit(1)

if __name__ == "__main__":
    try:
        reset_offset_to_latest()
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️  Cancelled by user[/yellow]")
        sys.exit(0)

