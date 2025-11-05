"""
Query Zilliz Cloud và hiển thị URLs duy nhất (không trùng lặp)
Hiển thị dưới dạng bảng đẹp với thống kê
"""

from pymilvus import connections, Collection, utility
from milvus_config import get_connection_params, COLLECTION_NAME, print_config
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import sys

console = Console()


def get_unique_urls():
    """Query Zilliz và lấy danh sách URLs duy nhất"""
    
    # Print config
    print_config()
    
    # Connect
    console.print("\n🔌 Connecting to Zilliz Cloud...", style="cyan")
    params = get_connection_params()
    connections.connect("default", **params)
    console.print("✅ Connected!", style="green")
    
    # Check collection exists
    if not utility.has_collection(COLLECTION_NAME):
        console.print(f"\n❌ Collection '{COLLECTION_NAME}' not found!", style="red")
        console.print(f"Please run upload_to_milvus.py first.", style="yellow")
        sys.exit(1)
    
    # Load collection
    collection = Collection(COLLECTION_NAME)
    collection.load()
    
    total_vectors = collection.num_entities
    console.print(f"\n📊 Total vectors in collection: {total_vectors}", style="cyan")
    
    # Query all data
    console.print("\n🔍 Fetching all videos from Zilliz...", style="cyan")
    results = collection.query(
        expr="id >= 0",
        output_fields=["job_id", "url", "frame_type"],
        limit=total_vectors
    )
    
    # Group by job_id to get unique videos
    videos = {}
    for item in results:
        job_id = item["job_id"]
        if job_id not in videos:
            videos[job_id] = {
                "url": item["url"],
                "frames": []
            }
        videos[job_id]["frames"].append(item["frame_type"])
    
    console.print(f"✅ Found {len(videos)} unique videos", style="green")
    
    # Disconnect
    connections.disconnect("default")
    
    return videos


def display_table(videos):
    """Hiển thị bảng URLs duy nhất"""
    
    # Create table
    table = Table(
        title="🎬 UNIQUE VIDEOS (Không trùng lặp)",
        show_header=True,
        header_style="bold magenta",
        border_style="cyan",
        show_lines=True
    )
    
    table.add_column("STT", style="cyan", justify="right", width=6)
    table.add_column("Job ID", style="yellow", width=12)
    table.add_column("Frames", style="green", justify="center", width=20)
    table.add_column("URL", style="white", overflow="fold")
    
    # Add rows
    for idx, (job_id, data) in enumerate(sorted(videos.items()), 1):
        frames_str = ", ".join(sorted(data["frames"]))
        url = data["url"]
        
        # Truncate URL if too long
        if len(url) > 80:
            display_url = url[:77] + "..."
        else:
            display_url = url
        
        table.add_row(
            str(idx),
            job_id,
            frames_str,
            display_url
        )
    
    console.print("\n")
    console.print(table)
    
    # Statistics
    stats = Panel(
        f"[cyan]📊 THỐNG KÊ:[/cyan]\n\n"
        f"[green]✅ Tổng số videos duy nhất:[/green] [bold]{len(videos)}[/bold]\n"
        f"[yellow]📹 Tổng số frames:[/yellow] [bold]{sum(len(v['frames']) for v in videos.values())}[/bold]\n"
        f"[blue]📈 Trung bình frames/video:[/blue] [bold]{sum(len(v['frames']) for v in videos.values()) / len(videos):.2f}[/bold]",
        title="📈 Summary",
        border_style="green"
    )
    console.print("\n")
    console.print(stats)


def export_to_csv(videos, output_file="unique_urls_from_zilliz.csv"):
    """Export URLs to CSV"""
    import csv
    
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["job_id", "url", "num_frames", "frames"])
        
        for job_id, data in sorted(videos.items()):
            writer.writerow([
                job_id,
                data["url"],
                len(data["frames"]),
                ",".join(sorted(data["frames"]))
            ])
    
    console.print(f"\n💾 Exported to: [green]{output_file}[/green]")


def main():
    try:
        # Get unique URLs from Zilliz
        videos = get_unique_urls()
        
        # Display table
        display_table(videos)
        
        # Export to CSV
        export_to_csv(videos)
        
        console.print("\n🎉 [bold green]DONE![/bold green]\n")
        
    except Exception as e:
        console.print(f"\n❌ [bold red]ERROR:[/bold red] {e}", style="red")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

