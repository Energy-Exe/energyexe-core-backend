#!/usr/bin/env python3
"""
Monitor backfill processes every hour for 24 hours.
Automatically detects and restarts stuck processes.
"""

import subprocess
import time
import os
import signal
from datetime import datetime, timedelta
from pathlib import Path

# Task configurations
TASKS = {
    # 2016 COMPLETED - removed from monitoring
    '2010-2012': {
        'output_file': '/tmp/claude/-Users-mdfaisal-Documents-energyexe-energyexe-core-backend/tasks/bbd9b10.output',
        'script': 'backfill_2010_2015.py',
        'total_days': 1864,
    },
    '2012': {
        'output_file': '/tmp/claude/-Users-mdfaisal-Documents-energyexe-energyexe-core-backend/tasks/ba3670d.output',
        'script': 'backfill_2012.py',
        'total_days': 348,
    },
    '2013': {
        'output_file': '/tmp/claude/-Users-mdfaisal-Documents-energyexe-energyexe-core-backend/tasks/b44732f.output',
        'script': 'backfill_2013.py',
        'total_days': 191,
    },
    '2014': {
        'output_file': '/tmp/claude/-Users-mdfaisal-Documents-energyexe-energyexe-core-backend/tasks/b479c3f.output',
        'script': 'backfill_2014.py',
        'total_days': 211,  # 260 - 49 already done before restart
    },
    '2015': {
        'output_file': '/tmp/claude/-Users-mdfaisal-Documents-energyexe-energyexe-core-backend/tasks/b5c68c5.output',
        'script': 'backfill_2015.py',
        'total_days': 181,  # 301 - 120 already done
    },
}

SCRIPT_DIR = Path(__file__).parent
LOG_FILE = SCRIPT_DIR / 'monitor_log.txt'
CHECK_INTERVAL_HOURS = 1
TOTAL_CHECKS = 24
STUCK_THRESHOLD_MINUTES = 45  # Consider stuck if no progress for 45 minutes


def log(message):
    """Log message to file and stdout."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {message}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')


def get_completed_count(output_file):
    """Get count of completed days from output file."""
    try:
        result = subprocess.run(
            ['grep', '-c', '✓ Completed', output_file],
            capture_output=True, text=True
        )
        return int(result.stdout.strip()) if result.returncode == 0 else 0
    except:
        return 0


def get_last_activity(output_file):
    """Get timestamp of last activity from output file."""
    try:
        result = subprocess.run(
            ['tail', '-50', output_file],
            capture_output=True, text=True
        )
        lines = result.stdout.strip().split('\n')
        for line in reversed(lines):
            if line.startswith('202'):
                # Extract timestamp from log line
                parts = line.split(' ')
                if len(parts) >= 2:
                    try:
                        ts = datetime.strptime(f"{parts[0]} {parts[1]}", '%Y-%m-%d %H:%M:%S')
                        return ts
                    except:
                        continue
        return None
    except:
        return None


def is_process_running(script_name):
    """Check if a process is running."""
    try:
        result = subprocess.run(
            ['pgrep', '-f', script_name],
            capture_output=True, text=True
        )
        return result.returncode == 0
    except:
        return False


def get_process_pid(script_name):
    """Get PID of running process."""
    try:
        result = subprocess.run(
            ['pgrep', '-f', script_name],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            pids = result.stdout.strip().split('\n')
            # Return the main python process (usually has highest PID)
            return max(int(p) for p in pids if p)
        return None
    except:
        return None


def kill_process(script_name):
    """Kill a process by script name."""
    pid = get_process_pid(script_name)
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            log(f"  Killed process {pid} ({script_name})")
            return True
        except:
            pass
    return False


def restart_process(script_name):
    """Restart a backfill process."""
    script_path = SCRIPT_DIR / script_name
    log(f"  Restarting {script_name}...")

    # Start process in background
    subprocess.Popen(
        ['poetry', 'run', 'python', str(script_path)],
        cwd=str(SCRIPT_DIR.parent.parent.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True
    )
    log(f"  Restarted {script_name}")


def check_and_fix_stuck(task_name, task_info, prev_counts):
    """Check if a task is stuck and fix it."""
    output_file = task_info['output_file']
    script_name = task_info['script']
    total_days = task_info['total_days']

    completed = get_completed_count(output_file)
    last_activity = get_last_activity(output_file)
    is_running = is_process_running(script_name)

    prev_completed = prev_counts.get(task_name, 0)
    progress_pct = (completed / total_days) * 100

    status = "RUNNING" if is_running else "STOPPED"

    # Check if completed
    if completed >= total_days:
        log(f"  {task_name}: COMPLETE ({completed}/{total_days})")
        return completed, True

    log(f"  {task_name}: {completed}/{total_days} ({progress_pct:.1f}%) - {status}")

    # Check if stuck
    is_stuck = False

    if is_running and last_activity:
        minutes_since_activity = (datetime.now() - last_activity).total_seconds() / 60
        if minutes_since_activity > STUCK_THRESHOLD_MINUTES:
            log(f"    WARNING: No activity for {minutes_since_activity:.0f} minutes - appears STUCK")
            is_stuck = True

    if not is_running and completed < total_days:
        log(f"    WARNING: Process not running but not complete")
        is_stuck = True

    if is_stuck:
        log(f"    Attempting to fix {task_name}...")
        if is_running:
            kill_process(script_name)
            time.sleep(3)
        restart_process(script_name)
        log(f"    Fixed {task_name}")

    return completed, False


def main():
    log("=" * 60)
    log("BACKFILL MONITOR STARTED")
    log(f"Will check every {CHECK_INTERVAL_HOURS} hour(s) for {TOTAL_CHECKS} checks")
    log("=" * 60)

    prev_counts = {}
    completed_tasks = set()

    for check_num in range(1, TOTAL_CHECKS + 1):
        log("")
        log("=" * 60)
        log(f"CHECK {check_num}/{TOTAL_CHECKS} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log("=" * 60)

        all_complete = True

        for task_name, task_info in TASKS.items():
            if task_name in completed_tasks:
                continue

            completed, is_complete = check_and_fix_stuck(task_name, task_info, prev_counts)
            prev_counts[task_name] = completed

            if is_complete:
                completed_tasks.add(task_name)
            else:
                all_complete = False

        if all_complete:
            log("")
            log("=" * 60)
            log("ALL BACKFILLS COMPLETE!")
            log("=" * 60)
            break

        if check_num < TOTAL_CHECKS:
            log(f"\nNext check in {CHECK_INTERVAL_HOURS} hour(s)...")
            time.sleep(CHECK_INTERVAL_HOURS * 3600)

    log("")
    log("=" * 60)
    log("MONITOR FINISHED")
    log("=" * 60)


if __name__ == '__main__':
    main()
