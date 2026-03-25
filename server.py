"""
Eden Scraper — HTTP server + internal scheduler.

Replaces the Render cron job with a persistent web service that:
  - Accepts POST /run to trigger an immediate scrape (e.g. from "Search now")
  - Runs on its own internal schedule (default every 6 h) without needing Render cron
  - Rejects concurrent runs so Claude API costs don't double up
  - Exposes GET /health so Render keeps it alive and you can monitor it

Deploy on Render as a Web Service:
  startCommand: uvicorn server:app --host 0.0.0.0 --port $PORT
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from main import run as scraper_run

# ── Config ─────────────────────────────────────────────────────────────────
SCRAPER_SECRET = os.environ.get("SCRAPER_SECRET", "")
SCHEDULE_HOURS = int(os.environ.get("SCHEDULE_HOURS", "6"))

# ── State ───────────────────────────────────────────────────────────────────
_run_lock = asyncio.Lock()          # prevents concurrent scraper runs
_last_run: datetime | None = None   # wall-clock time of last completed run
_last_trigger: str = "none"        # "scheduled" | "manual" | "none"


# ── Auth ────────────────────────────────────────────────────────────────────
def _require_auth(request: Request) -> None:
    if not SCRAPER_SECRET:
        return  # no secret set → open (local dev only)
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {SCRAPER_SECRET}":
        raise HTTPException(status_code=401, detail="Invalid or missing token")


# ── Core runner ─────────────────────────────────────────────────────────────
async def _do_run(trigger: str) -> None:
    global _last_run, _last_trigger
    if _run_lock.locked():
        print(f"[Eden Server] Skipping {trigger} — run already in progress")
        return
    async with _run_lock:
        _last_trigger = trigger
        print(f"[Eden Server] ── Starting {trigger} run at {datetime.now()} ──")
        try:
            await scraper_run()
        except Exception as exc:
            print(f"[Eden Server] Run failed: {exc}")
        finally:
            _last_run = datetime.now(timezone.utc)
            print(f"[Eden Server] ── {trigger.capitalize()} run finished at {datetime.now()} ──")


# ── Internal scheduler ───────────────────────────────────────────────────────
async def _scheduler() -> None:
    """Runs the scraper every SCHEDULE_HOURS hours in the background."""
    interval = SCHEDULE_HOURS * 3600
    print(f"[Eden Server] Scheduler armed — will run every {SCHEDULE_HOURS}h")
    while True:
        await asyncio.sleep(interval)
        await _do_run("scheduled")


# ── App lifecycle ────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_scheduler())
    try:
        yield
    finally:
        task.cancel()


app = FastAPI(title="Eden Scraper", lifespan=lifespan)


# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "busy": _run_lock.locked(),
        "last_run": _last_run.isoformat() if _last_run else None,
        "last_trigger": _last_trigger,
        "schedule_hours": SCHEDULE_HOURS,
    }


@app.post("/run")
async def trigger_run(request: Request):
    """Kick off an immediate scraper run. Returns instantly; run happens in background."""
    _require_auth(request)

    if _run_lock.locked():
        return JSONResponse(
            {"status": "busy", "message": "A run is already in progress — check /health for updates"},
            status_code=202,
        )

    asyncio.create_task(_do_run("manual"))
    return {"status": "started", "message": "Scraper run started in background"}


# ── Entrypoint (local dev) ───────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
