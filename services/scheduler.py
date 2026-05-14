"""
services/scheduler.py

APScheduler integration for the ComputerReseller Portal — Deal Blaster.

How it works:
- A single BackgroundScheduler is created at module level.
- init_scheduler(app) is called once in create_app() after blueprints are registered.
- On init, all active scan_schedules rows are loaded and registered as APScheduler jobs.
- Each job calls _run_schedule_job(schedule_id) which fetches, scores, and upserts deals.
- Routes can call add_job / pause_job / resume_job / remove_job to manage schedules
  dynamically without restarting the server.

DB access:
- Jobs run outside Flask request context so we call get_db() directly (not g.db).
- Every run is logged to scan_runs regardless of success or failure.
"""

import logging
import time
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron         import CronTrigger
from apscheduler.triggers.interval     import IntervalTrigger
from apscheduler.jobstores.memory      import MemoryJobStore

from .bestbuy import run_scan, upsert_deals
from ..db     import get_db

logger = logging.getLogger(__name__)

# ── Scheduler singleton ───────────────────────────────────────────────────────

_scheduler = BackgroundScheduler(
    jobstores={"default": MemoryJobStore()},
    job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 300},
    timezone="America/New_York",
)


# ── Job execution ─────────────────────────────────────────────────────────────

def _run_schedule_job(schedule_id: int):
    """
    Called by APScheduler for both cron and interval jobs.
    1. Loads the schedule config from DB.
    2. Runs the Best Buy scan with the schedule's filters.
    3. Upserts deals into bb_deals.
    4. Logs the result to scan_runs.
    5. For score_alert schedules: checks if any new deal hits alert_threshold.
    """
    start_ms = int(time.time() * 1000)
    logger.info(f"[Scheduler] Running schedule_id={schedule_id}")

    conn = None
    try:
        conn = get_db()

        # Load schedule
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, trigger_type, alert_threshold, filters, mode, is_active
                FROM scan_schedules
                WHERE id = %s
            """, (schedule_id,))
            schedule = cur.fetchone()

        if not schedule:
            logger.warning(f"[Scheduler] schedule_id={schedule_id} not found — removing job")
            remove_job(schedule_id)
            return

        if not schedule["is_active"]:
            logger.info(f"[Scheduler] schedule_id={schedule_id} is paused — skipping")
            return

        filters = schedule["filters"] or {}

        # Run scan
        result      = run_scan(filters)
        products    = result["products"]
        deals_found = len(products)

        # Upsert into bb_deals
        deals_found, new_deals = upsert_deals(conn, products)

        # For score_alert mode — check if threshold was hit
        if schedule["trigger_type"] == "score_alert" and schedule.get("alert_threshold"):
            threshold    = schedule["alert_threshold"]
            hot_deals    = [p for p in products if p.get("fresh_score", 0) >= threshold]
            if hot_deals:
                logger.info(
                    f"[Scheduler] ALERT: {len(hot_deals)} deals hit score >= {threshold} "
                    f"for schedule '{schedule['name']}'"
                )
                # Future: trigger portal notification / badge here

        duration_ms = int(time.time() * 1000) - start_ms

        # Log run
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scan_runs
                    (schedule_id, triggered_by, filters_used, deals_found, new_deals,
                     run_at, duration_ms, status)
                VALUES (%s, 'scheduled', %s, %s, %s, NOW(), %s, 'ok')
            """, (
                schedule_id,
                filters,
                deals_found,
                new_deals,
                duration_ms,
            ))
            # Update last_run_at on the schedule
            cur.execute("""
                UPDATE scan_schedules SET last_run_at = NOW() WHERE id = %s
            """, (schedule_id,))
        conn.commit()

        logger.info(
            f"[Scheduler] schedule_id={schedule_id} '{schedule['name']}' done — "
            f"{deals_found} deals ({new_deals} new) in {duration_ms}ms"
        )

    except Exception as e:
        duration_ms = int(time.time() * 1000) - start_ms
        logger.error(f"[Scheduler] schedule_id={schedule_id} FAILED: {e}")

        # Log the error to scan_runs
        try:
            if conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO scan_runs
                            (schedule_id, triggered_by, filters_used, deals_found, new_deals,
                             run_at, duration_ms, status, error_message)
                        VALUES (%s, 'scheduled', '{}', 0, 0, NOW(), %s, 'error', %s)
                    """, (schedule_id, duration_ms, str(e)[:500]))
                conn.commit()
        except Exception as log_err:
            logger.error(f"[Scheduler] Failed to log error to scan_runs: {log_err}")

    finally:
        if conn:
            conn.close()


def run_manual_scan(filters: dict = None, schedule_id: int = None) -> dict:
    """
    On-demand scan triggered from the portal UI (Run Now buttons).
    Can run with arbitrary filters (deal blaster page filters)
    or re-run a saved schedule's filters (per-schedule Run Now button).
    Returns a summary dict for the API response.
    """
    filters   = filters or {}
    start_ms  = int(time.time() * 1000)
    conn      = None

    # If running a saved schedule on-demand, load its filters
    if schedule_id:
        try:
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT filters, name FROM scan_schedules WHERE id = %s",
                    (schedule_id,)
                )
                row = cur.fetchone()
                if row:
                    filters = row["filters"] or {}
                    logger.info(
                        f"[Manual] Running schedule '{row['name']}' on-demand"
                    )
        except Exception as e:
            logger.error(f"[Manual] Failed to load schedule {schedule_id}: {e}")
        finally:
            if conn:
                conn.close()
                conn = None

    try:
        conn        = get_db()
        result      = run_scan(filters)
        products    = result["products"]
        deals_found, new_deals = upsert_deals(conn, products)
        duration_ms = int(time.time() * 1000) - start_ms

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scan_runs
                    (schedule_id, triggered_by, filters_used, deals_found, new_deals,
                     run_at, duration_ms, status)
                VALUES (%s, 'manual', %s, %s, %s, NOW(), %s, 'ok')
                RETURNING id
            """, (
                schedule_id,
                filters,
                deals_found,
                new_deals,
                duration_ms,
            ))
            run_id = cur.fetchone()["id"]

            if schedule_id:
                cur.execute(
                    "UPDATE scan_schedules SET last_run_at = NOW() WHERE id = %s",
                    (schedule_id,)
                )

        conn.commit()

        logger.info(
            f"[Manual] Done — {deals_found} deals ({new_deals} new) in {duration_ms}ms"
        )

        return {
            "ok":          True,
            "run_id":      run_id,
            "deals_found": deals_found,
            "new_deals":   new_deals,
            "duration_ms": duration_ms,
        }

    except Exception as e:
        duration_ms = int(time.time() * 1000) - start_ms
        logger.error(f"[Manual] Scan failed: {e}")

        # Log error
        try:
            if conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO scan_runs
                            (schedule_id, triggered_by, filters_used, deals_found, new_deals,
                             run_at, duration_ms, status, error_message)
                        VALUES (%s, 'manual', %s, 0, 0, NOW(), %s, 'error', %s)
                    """, (schedule_id, filters, duration_ms, str(e)[:500]))
                conn.commit()
        except Exception:
            pass

        return {"ok": False, "error": str(e), "duration_ms": duration_ms}

    finally:
        if conn:
            conn.close()


# ── Job management ────────────────────────────────────────────────────────────

def _make_trigger(schedule: dict):
    """Build the APScheduler trigger from a scan_schedules row."""
    if schedule["trigger_type"] == "cron":
        expr = (schedule.get("cron_expression") or "0 8 * * *").split()
        # cron format: minute hour day month day_of_week
        if len(expr) == 5:
            return CronTrigger(
                minute=expr[0], hour=expr[1], day=expr[2],
                month=expr[3],  day_of_week=expr[4],
                timezone="America/New_York",
            )
        return CronTrigger.from_crontab(
            schedule.get("cron_expression", "0 8 * * *"),
            timezone="America/New_York",
        )
    elif schedule["trigger_type"] == "score_alert":
        hours = schedule.get("interval_hours") or 2
        return IntervalTrigger(hours=hours)
    else:
        return IntervalTrigger(hours=6)  # safe default


def _job_id(schedule_id: int) -> str:
    return f"scan_schedule_{schedule_id}"


def add_job(schedule: dict):
    """Register or replace an APScheduler job for a scan_schedules row."""
    job_id  = _job_id(schedule["id"])
    trigger = _make_trigger(schedule)

    if _scheduler.get_job(job_id):
        _scheduler.remove_job(job_id)

    _scheduler.add_job(
        func=_run_schedule_job,
        trigger=trigger,
        args=[schedule["id"]],
        id=job_id,
        name=schedule.get("name", f"Schedule {schedule['id']}"),
        replace_existing=True,
    )
    logger.info(
        f"[Scheduler] Registered job '{schedule['name']}' "
        f"(id={schedule['id']}, trigger={schedule['trigger_type']})"
    )


def pause_job(schedule_id: int):
    job_id = _job_id(schedule_id)
    if _scheduler.get_job(job_id):
        _scheduler.pause_job(job_id)
        logger.info(f"[Scheduler] Paused job for schedule_id={schedule_id}")


def resume_job(schedule_id: int):
    job_id = _job_id(schedule_id)
    if _scheduler.get_job(job_id):
        _scheduler.resume_job(job_id)
        logger.info(f"[Scheduler] Resumed job for schedule_id={schedule_id}")


def remove_job(schedule_id: int):
    job_id = _job_id(schedule_id)
    if _scheduler.get_job(job_id):
        _scheduler.remove_job(job_id)
        logger.info(f"[Scheduler] Removed job for schedule_id={schedule_id}")


def get_job_status(schedule_id: int) -> dict:
    """Returns next run time and paused state for a schedule."""
    job = _scheduler.get_job(_job_id(schedule_id))
    if not job:
        return {"registered": False}
    return {
        "registered": True,
        "paused":     job.next_run_time is None,
        "next_run":   job.next_run_time.isoformat() if job.next_run_time else None,
    }


# ── Init ──────────────────────────────────────────────────────────────────────

def init_scheduler(app):
    """
    Call once in create_app() after blueprints are registered.
    Starts the BackgroundScheduler and loads all active schedules from DB.
    """
    if _scheduler.running:
        logger.warning("[Scheduler] Already running — skipping init")
        return

    _scheduler.start()
    logger.info("[Scheduler] Started")

    # Load all active schedules from DB and register jobs
    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, trigger_type, cron_expression,
                       interval_hours, alert_threshold, filters, mode
                FROM scan_schedules
                WHERE is_active = TRUE
            """)
            schedules = cur.fetchall()

        for s in schedules:
            try:
                add_job(s)
            except Exception as e:
                logger.error(f"[Scheduler] Failed to register schedule_id={s['id']}: {e}")

        logger.info(f"[Scheduler] Loaded {len(schedules)} active schedule(s)")

    except Exception as e:
        logger.error(f"[Scheduler] Failed to load schedules on init: {e}")
    finally:
        if conn:
            conn.close()

    # Graceful shutdown when Flask process exits
    import atexit
    atexit.register(lambda: _scheduler.shutdown(wait=False))
