#!/usr/bin/env python3
"""Monitor weather import jobs running in parallel."""

import subprocess
import sys
from pathlib import Path

# Process IDs for the 5 parallel jobs (updated 2025-12-02)
JOBS = {
    '2010': {'pid': 1472, 'log': '/tmp/weather_2010.log'},
    '2011': {'pid': 1476, 'log': '/tmp/weather_2011.log'},
    '2012': {'pid': 1480, 'log': '/tmp/weather_2012.log'},
    '2013': {'pid': 1485, 'log': '/tmp/weather_2013.log'},
    '2014': {'pid': 1490, 'log': '/tmp/weather_2014.log'},
}

def check_process(pid):
    """Check if process is running."""
    try:
        result = subprocess.run(['ps', '-p', str(pid)], capture_output=True)
        return result.returncode == 0
    except:
        return False

def get_log_tail(log_file, lines=15):
    """Get last N lines from log file."""
    try:
        with open(log_file, 'r') as f:
            return ''.join(f.readlines()[-lines:])
    except FileNotFoundError:
        return "Log file not found yet"
    except Exception as e:
        return f"Error reading log: {e}"

def main():
    """Monitor all weather import jobs."""

    print("=" * 80)
    print("WEATHER IMPORT JOBS - STATUS MONITOR")
    print("=" * 80)
    print()

    running_count = 0
    stopped_count = 0

    for year, job in JOBS.items():
        pid = job['pid']
        log = job['log']
        is_running = check_process(pid)

        if is_running:
            status = "🟢 RUNNING"
            running_count += 1
        else:
            status = "🔴 STOPPED/COMPLETED"
            stopped_count += 1

        print(f"Year {year} (PID {pid}): {status}")
        print(f"  Log: tail -f {log}")
        print()

        # Show last few lines of log
        if len(sys.argv) > 1 and sys.argv[1] == '--verbose':
            print(f"  Last 10 lines:")
            tail = get_log_tail(log, 10)
            for line in tail.split('\n'):
                if line.strip():
                    print(f"    {line}")
            print()

    print("-" * 80)
    print(f"Summary: {running_count} running, {stopped_count} stopped/completed")
    print("-" * 80)

    print()
    print("Quick Commands:")
    print("  Monitor all:     watch -n 5 'poetry run python scripts/monitor_weather_imports.py'")
    print("  Verbose:         poetry run python scripts/monitor_weather_imports.py --verbose")
    print("  Check 2010 log:  tail -f /tmp/weather_2010.log")
    print("  Check 2011 log:  tail -f /tmp/weather_2011.log")
    print("  Check 2012 log:  tail -f /tmp/weather_2012.log")
    print("  Check 2013 log:  tail -f /tmp/weather_2013.log")
    print("  Check 2014 log:  tail -f /tmp/weather_2014.log")
    print()
    print("Kill jobs (if needed):")
    print("  kill 97594 97595 97596 97597 97598")

if __name__ == "__main__":
    main()
