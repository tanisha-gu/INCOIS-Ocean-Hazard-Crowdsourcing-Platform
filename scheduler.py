"""
here all data with clear code 
Background job scheduler using APScheduler.
Jobs:
  - Every 15 min: fetch & analyse social media posts
  - Every 30 min: recalculate all hotspots
  - Every 24 h: push critical hotspot alerts to INCOIS webhook
"""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger
import httpx

from app.services.social_media_service import social_service
from app.services.hotspot_service import hotspot_service
from app.models.hotspot import Hotspot, HotspotSeverity
from config.settings import settings
from config.redis import cache

scheduler = AsyncIOScheduler()


async def _job_fetch_social():
    logger.info("⏰ [Scheduler] Fetching social media posts...")
    try:
        result = await social_service.fetch_and_analyze()
        await cache.delete_pattern("social:*")
        await cache.delete_pattern("analytics:*")
        logger.info(f"   → {result}")
    except Exception as e:
        logger.error(f"Social fetch job failed: {e}")


async def _job_recalculate_hotspots():
    logger.info("⏰ [Scheduler] Recalculating hotspots...")
    try:
        count = await hotspot_service.recalculate_all()
        await cache.delete_pattern("hotspots:*")
        await cache.delete_pattern("analytics:*")
        logger.info(f"   → {count} hotspots updated")
    except Exception as e:
        logger.error(f"Hotspot recalculation job failed: {e}")


async def _job_incois_webhook():
    """Push critical/high hotspots to the INCOIS early warning system."""
    if not settings.INCOIS_WEBHOOK_URL:
        return

    try:
        critical_hotspots = await Hotspot.find(
            {"is_active": True, "severity": {"$in": [HotspotSeverity.CRITICAL, HotspotSeverity.HIGH]}}
        ).to_list()

        if not critical_hotspots:
            return

        payload = {
            "source": "INCOIS_CROWDSOURCE_PLATFORM",
            "hotspots": [
                {
                    "id": str(h.id),
                    "center": h.center,
                    "severity": h.severity,
                    "dominant_hazard": h.dominant_hazard_type,
                    "report_count": h.report_count,
                    "density_score": h.density_score,
                    "location": h.location_name,
                    "state": h.state,
                    "last_report_at": str(h.last_report_at),
                }
                for h in critical_hotspots
            ],
        }

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(settings.INCOIS_WEBHOOK_URL, json=payload)
            logger.info(f"⏰ [Scheduler] INCOIS webhook → {resp.status_code}")
    except Exception as e:
        logger.error(f"INCOIS webhook job failed: {e}")


def start_scheduler():
    scheduler.add_job(_job_fetch_social,         IntervalTrigger(minutes=15),  id="social_fetch",  replace_existing=True)
    scheduler.add_job(_job_recalculate_hotspots, IntervalTrigger(minutes=30),  id="hotspot_calc",  replace_existing=True)
    scheduler.add_job(_job_incois_webhook,        IntervalTrigger(hours=1),     id="incois_webhook", replace_existing=True)
    scheduler.start()
    logger.info("✅ Background scheduler started")


def stop_scheduler():
    scheduler.shutdown(wait=False)
    logger.info("Background scheduler stopped")
