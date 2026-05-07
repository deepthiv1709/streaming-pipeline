import os, time
from datetime import datetime, timedelta
from config.settings import OUTPUT_DIR, EXPECTED_RECORDS_PER_MINUTE

def count_recent_parquet_files(directory: str, minutes: int = 5) -> int:
    """Count Parquet files written in the last N minutes."""
    cutoff = datetime.now() - timedelta(minutes=minutes)
    count  = 0
    for root, _, files in os.walk(directory):
        for f in files:
            if f.endswith(".parquet"):
                path  = os.path.join(root, f)
                mtime = datetime.fromtimestamp(os.path.getmtime(path))
                if mtime > cutoff:
                    count += 1
    return count

def check_completeness():
    """Alert if output volume drops below expected threshold."""
    file_count = count_recent_parquet_files(OUTPUT_DIR, minutes=5)
    print(f"[MONITOR] {datetime.now().isoformat()} — Parquet files in last 5 min: {file_count}")

    if file_count == 0:
        alert("No Parquet files written in the last 5 minutes — pipeline may be stalled.")

def alert(message: str):
    """Print alert — extend this to send email/Slack in production."""
    print(f"\n[ALERT] {datetime.now().isoformat()}")
    print(f"[ALERT] {message}\n")
    # In production: send_slack_message(message) or send_email(message)

def run():
    print("Monitoring started. Checking every 60 seconds...")
    while True:
        check_completeness()
        time.sleep(60)

if __name__ == "__main__":
    run()