#!/usr/bin/env python3
"""
DB-Driven Briefing Runner — Phase F

Generates the new S/A/B/C-tier briefing entirely from SQLite.
No external API calls — purely reads accumulated DB state.

Usage:
    python scripts/run_briefing.py [--output-dir PATH] [--days-back N]

Output:
    data/briefings/briefing_YYYY_MM.txt
    data/briefings/briefing_YYYY_MM.html
"""

import argparse
import logging
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.briefing.db_briefing import generate_briefing_from_db
from src.database import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main(output_dir: str = "./data/briefings", days_back: int = 7, send_notifications: bool = True) -> str:
    """
    Generate and optionally send the DB-driven briefing.

    Returns:
        The generated text briefing string.
    """
    load_dotenv()

    logger.info("=" * 60)
    logger.info("BUFFETT BOT DB BRIEFING")
    logger.info("=" * 60)

    # Resolve output directory with fallback
    out_path = Path(output_dir)
    try:
        out_path.mkdir(parents=True, exist_ok=True)
        (out_path / ".write_test").write_text("ok")
        (out_path / ".write_test").unlink()
    except PermissionError:
        fallback = Path(tempfile.gettempdir()) / "buffett-bot-briefings"
        logger.warning("Cannot write to %s — using fallback: %s", out_path, fallback)
        out_path = fallback
        out_path.mkdir(parents=True, exist_ok=True)

    db = Database()

    logger.info("Pulling data from DB (%s)...", db.path)
    text, html = generate_briefing_from_db(db, output_dir=str(out_path), days_back=days_back)

    logger.info("Briefing saved to %s", out_path)
    logger.info("  Text: briefing_*.txt")
    logger.info("  HTML: briefing_*.html")

    # Send notifications if configured
    if send_notifications:
        try:
            from src.notifications import NotificationManager

            notifier = NotificationManager()
            html_path = sorted(out_path.glob("briefing_*.html"), key=lambda p: p.stat().st_mtime)
            html_content = html_path[-1].read_text() if html_path else None
            results = notifier.send_briefing(text, html_content=html_content)
            for channel, success in results.items():
                status = "OK" if success else "FAIL"
                logger.info("  [%s] %s", status, channel)
        except Exception as ex:
            logger.warning("Notification failed: %s", ex)
    else:
        logger.info("Notifications skipped (send_notifications=False)")

    # Log a summary
    alerts = db.get_price_alerts()
    s_count = sum(1 for a in alerts if a["tier"] == "S")
    a_count = sum(1 for a in alerts if a["tier"] == "A")
    b_count = sum(1 for a in alerts if a["tier"] == "B")
    c_count = sum(1 for a in alerts if a["tier"] == "C")
    logger.info("")
    logger.info("SUMMARY:")
    logger.info("  S-tier (Buy now, 3 tranches): %d", s_count)
    logger.info("  A-tier (Buy now, 2 tranches): %d", a_count)
    logger.info("  B-tier (Watch):               %d", b_count)
    logger.info("  C-tier (Monitor):             %d", c_count)

    return text


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate DB-driven Buffett Bot briefing")
    parser.add_argument("--output-dir", default="./data/briefings", help="Output directory for briefing files")
    parser.add_argument("--days-back", type=int, default=7, help="Days back for news digest (default 7)")
    parser.add_argument("--no-notify", action="store_true", help="Skip sending notifications")
    args = parser.parse_args()

    main(
        output_dir=args.output_dir,
        days_back=args.days_back,
        send_notifications=not args.no_notify,
    )
