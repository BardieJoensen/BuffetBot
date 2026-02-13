"""
Notifications Module

Delivers briefings and alerts via:
- Email (SMTP)
- Telegram Bot
- Ntfy.sh (simple push notifications)
- Discord Webhook

v2.0 additions:
- Regime-shift alerts (market regime changes)
- Approaching-target alerts (Tier 2 stocks nearing buy range)

Configure your preferred method in .env
"""

import logging
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class EmailNotifier:
    """
    Send briefings via email.

    Requires SMTP configuration in .env:
    - SMTP_HOST
    - SMTP_PORT
    - SMTP_USER
    - SMTP_PASSWORD
    - EMAIL_TO
    """

    def __init__(
        self,
        smtp_host: Optional[str] = None,
        smtp_port: Optional[int] = None,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        email_to: Optional[str] = None,
    ):
        self.smtp_host = smtp_host or os.getenv("SMTP_HOST") or ""
        self.smtp_port = smtp_port or int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = smtp_user or os.getenv("SMTP_USER") or ""
        self.smtp_password = smtp_password or os.getenv("SMTP_PASSWORD") or ""
        self.email_to = email_to or os.getenv("EMAIL_TO") or ""

        self.configured = all([self.smtp_host, self.smtp_user, self.smtp_password, self.email_to])

    def send_briefing(
        self, briefing_text: str, subject: Optional[str] = None, html_content: Optional[str] = None
    ) -> bool:
        """Send monthly briefing via email."""

        if not self.configured:
            logger.warning("Email not configured. Skipping.")
            return False

        subject = subject or f"Watchlist Update - {datetime.now().strftime('%B %Y')}"

        try:
            msg = MIMEMultipart("alternative")
            msg["From"] = self.smtp_user
            msg["To"] = self.email_to
            msg["Subject"] = subject

            msg.attach(MIMEText(briefing_text, "plain"))

            if html_content:
                msg.attach(MIMEText(html_content, "html"))
            else:
                html_body = (
                    "<html><body>"
                    "<pre style=\"font-family:'Courier New',monospace;font-size:12px;\">"
                    f"{briefing_text}</pre></body></html>"
                )
                msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)

            logger.info(f"Briefing sent to {self.email_to}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def send_alert(self, symbol: str, message: str) -> bool:
        """Send urgent alert about a position"""

        if not self.configured:
            return False

        subject = f"ALERT: {symbol}"

        try:
            msg = MIMEMultipart()
            msg["From"] = self.smtp_user
            msg["To"] = self.email_to
            msg["Subject"] = subject
            msg.attach(MIMEText(message, "plain"))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)

            logger.info(f"Alert sent for {symbol}")
            return True

        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
            return False


class TelegramNotifier:
    """
    Send briefings via Telegram bot.

    Setup:
    1. Create a bot via @BotFather on Telegram
    2. Get your chat ID by messaging @userinfobot
    3. Add to .env:
       - TELEGRAM_BOT_TOKEN
       - TELEGRAM_CHAT_ID
    """

    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

        self.configured = bool(self.bot_token and self.chat_id)
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}"

    def send_briefing(self, briefing_text: str) -> bool:
        """Send monthly briefing via Telegram"""

        if not self.configured:
            logger.warning("Telegram not configured. Skipping.")
            return False

        chunks = self._split_message(briefing_text, 4000)

        success = True
        for chunk in chunks:
            formatted = f"```\n{chunk}\n```"
            if not self._send_message(formatted, parse_mode="Markdown"):
                success = False

        return success

    def send_alert(self, symbol: str, message: str) -> bool:
        """Send urgent alert"""

        if not self.configured:
            return False

        text = f"*ALERT: {symbol}*\n\n{message}"
        return self._send_message(text, parse_mode="Markdown")

    def send_summary(self, summary: dict) -> bool:
        """Send quick summary (good for weekly updates)"""

        if not self.configured:
            return False

        text = f"""*Watchlist Update*

Tier 1 (Buy Zone): {summary.get("tier1_count", 0)}
Tier 2 (Watch): {summary.get("tier2_count", 0)}
Tier 3 (Monitor): {summary.get("tier3_count", 0)}
Approaching Target: {summary.get("approaching_count", 0)}

Portfolio: {summary.get("portfolio_return", "N/A")}

{summary.get("top_pick", "No Tier 1 picks this cycle.")}
"""
        return self._send_message(text, parse_mode="Markdown")

    def _send_message(self, text: str, parse_mode: Optional[str] = None) -> bool:
        """Send a single message"""

        try:
            payload = {"chat_id": self.chat_id, "text": text}

            if parse_mode:
                payload["parse_mode"] = parse_mode

            response = requests.post(f"{self.api_url}/sendMessage", json=payload)

            if response.status_code == 200:
                return True
            else:
                logger.error(f"Telegram error: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False

    def _split_message(self, text: str, max_length: int) -> list[str]:
        """Split long message into chunks"""

        if len(text) <= max_length:
            return [text]

        chunks = []
        lines = text.split("\n")
        current_chunk = ""

        for line in lines:
            if len(current_chunk) + len(line) + 1 > max_length:
                chunks.append(current_chunk)
                current_chunk = line
            else:
                current_chunk += "\n" + line if current_chunk else line

        if current_chunk:
            chunks.append(current_chunk)

        return chunks


class NtfyNotifier:
    """
    Send notifications via ntfy.sh (simple push notifications).

    No account needed - just pick a unique topic name.

    Setup:
    1. Install ntfy app on your phone
    2. Subscribe to your topic (e.g., "buffett-bot-yourname")
    3. Add to .env: NTFY_TOPIC=buffett-bot-yourname
    """

    def __init__(self, topic: Optional[str] = None, server: str = "https://ntfy.sh"):
        self.topic = topic or os.getenv("NTFY_TOPIC")
        self.server = os.getenv("NTFY_SERVER", server)

        self.configured = bool(self.topic)

    def send_briefing_ready(self) -> bool:
        """Notify that briefing is ready"""

        if not self.configured:
            return False

        return self._send(
            title="Watchlist Update Ready",
            message="Your investment watchlist update has been generated. Check your email or server.",
            priority=3,
        )

    def send_alert(self, symbol: str, message: str) -> bool:
        """Send urgent alert"""

        if not self.configured:
            return False

        return self._send(
            title=f"ALERT: {symbol}",
            message=message,
            priority=5,
            tags=["warning", "stock"],
        )

    def send_buy_signal(self, symbol: str, message: str) -> bool:
        """Notify of a Tier 1 entry"""

        if not self.configured:
            return False

        return self._send(
            title=f"Tier 1 Entry: {symbol}",
            message=message,
            priority=4,
            tags=["chart_with_upwards_trend"],
        )

    def _send(
        self, message: str, title: Optional[str] = None, priority: int = 3, tags: Optional[list[str]] = None
    ) -> bool:
        """Send notification"""

        try:
            headers = {}

            if title:
                headers["Title"] = title
            if priority:
                headers["Priority"] = str(priority)
            if tags:
                headers["Tags"] = ",".join(tags)

            response = requests.post(f"{self.server}/{self.topic}", data=message.encode("utf-8"), headers=headers)

            return response.status_code == 200

        except Exception as e:
            logger.error(f"Failed to send ntfy notification: {e}")
            return False


class DiscordNotifier:
    """
    Send briefings via Discord webhook.

    Setup:
    1. In Discord: Server Settings -> Integrations -> Webhooks
    2. Create webhook, copy URL
    3. Add to .env: DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
    """

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv("DISCORD_WEBHOOK_URL")
        self.configured = bool(self.webhook_url)

    def send_briefing(self, briefing_text: str) -> bool:
        """Send monthly briefing via Discord"""

        if not self.configured:
            logger.warning("Discord not configured. Skipping.")
            return False

        chunks = self._split_message(briefing_text, 1900)

        success = True
        for i, chunk in enumerate(chunks):
            embed = {
                "title": "Watchlist Update" if i == 0 else f"(continued {i + 1}/{len(chunks)})",
                "description": f"```\n{chunk}\n```",
                "color": 3066993,
            }

            if not self._send(embed=embed):
                success = False

        return success

    def send_alert(self, symbol: str, message: str) -> bool:
        """Send urgent alert"""

        if not self.configured:
            return False

        embed = {
            "title": f"ALERT: {symbol}",
            "description": message,
            "color": 15158332,  # Red
        }

        return self._send(embed=embed)

    def send_buy_signal(self, symbol: str, margin_of_safety: float, thesis: str = "") -> bool:
        """Notify of a new buy candidate"""

        if not self.configured:
            return False

        embed = {
            "title": f"Tier 1 Entry: {symbol}",
            "fields": [
                {"name": "Margin of Safety", "value": f"{margin_of_safety:.1%}", "inline": True},
            ],
            "color": 3066993,
        }

        if thesis:
            embed["fields"].append({"name": "Thesis", "value": thesis[:1000], "inline": False})  # type: ignore[attr-defined]

        return self._send(embed=embed)

    def _send(self, content: Optional[str] = None, embed: Optional[dict] = None) -> bool:
        """Send message to Discord webhook"""

        try:
            payload: dict[str, object] = {}
            if content:
                payload["content"] = content
            if embed:
                payload["embeds"] = [embed]

            response = requests.post(self.webhook_url, json=payload)

            if response.status_code in [200, 204]:
                return True
            else:
                logger.error(f"Discord error: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Failed to send Discord message: {e}")
            return False

    def _split_message(self, text: str, max_length: int) -> list[str]:
        """Split long message into chunks"""

        if len(text) <= max_length:
            return [text]

        chunks = []
        lines = text.split("\n")
        current_chunk = ""

        for line in lines:
            if len(current_chunk) + len(line) + 1 > max_length:
                chunks.append(current_chunk)
                current_chunk = line
            else:
                current_chunk += "\n" + line if current_chunk else line

        if current_chunk:
            chunks.append(current_chunk)

        return chunks


class NotificationManager:
    """
    Unified notification manager.

    Sends via all configured channels.
    """

    def __init__(self):
        self.email = EmailNotifier()
        self.telegram = TelegramNotifier()
        self.ntfy = NtfyNotifier()
        self.discord = DiscordNotifier()

        configured = []
        if self.email.configured:
            configured.append("Email")
        if self.telegram.configured:
            configured.append("Telegram")
        if self.ntfy.configured:
            configured.append("Ntfy")
        if self.discord.configured:
            configured.append("Discord")

        if configured:
            logger.info(f"Notifications configured: {', '.join(configured)}")
        else:
            logger.warning("No notification channels configured")

    def send_briefing(self, briefing_text: str, html_content: Optional[str] = None) -> dict:
        """Send briefing via all channels."""

        results = {}

        if self.email.configured:
            results["email"] = self.email.send_briefing(briefing_text, html_content=html_content)

        if self.telegram.configured:
            results["telegram"] = self.telegram.send_briefing(briefing_text)

        if self.ntfy.configured:
            results["ntfy"] = self.ntfy.send_briefing_ready()

        if self.discord.configured:
            results["discord"] = self.discord.send_briefing(briefing_text)

        return results

    def send_alert(self, symbol: str, message: str) -> dict:
        """Send alert via all channels"""

        results = {}

        if self.email.configured:
            results["email"] = self.email.send_alert(symbol, message)

        if self.telegram.configured:
            results["telegram"] = self.telegram.send_alert(symbol, message)

        if self.ntfy.configured:
            results["ntfy"] = self.ntfy.send_alert(symbol, message)

        if self.discord.configured:
            results["discord"] = self.discord.send_alert(symbol, message)

        return results

    def send_regime_shift_alert(
        self, previous_regime: str, new_regime: str, tier2_approaching: list[dict]
    ) -> dict:
        """
        Send alert when market regime shifts significantly.

        Triggered when regime moves from overvalued/euphoria to correction/crisis.

        Args:
            previous_regime: Previous regime name
            new_regime: New regime name
            tier2_approaching: List of dicts with symbol, price_gap_pct, target_entry_price
        """
        lines = [
            "MARKET REGIME SHIFT",
            "",
            f"Previous: {previous_regime.upper()}",
            f"Current:  {new_regime.upper()}",
            "",
        ]

        if new_regime in ("correction", "crisis"):
            lines.append("Opportunities may be developing. Check your Tier 2 watchlist:")
            lines.append("")
            for stock in tier2_approaching[:10]:
                sym = stock.get("symbol", "?")
                gap = stock.get("price_gap_pct", 0)
                target = stock.get("target_entry_price", 0)
                lines.append(f"  {sym}: {gap:+.0%} from target ${target:,.0f}")
            lines.append("")
            lines.append("Review your watchlist and prepare staged entry plans.")
        else:
            lines.append("Consider adjusting deployment strategy accordingly.")

        message = "\n".join(lines)

        results = {}
        if self.email.configured:
            results["email"] = self.email.send_alert("REGIME", message)
        if self.telegram.configured:
            results["telegram"] = self.telegram.send_alert("REGIME", message)
        if self.ntfy.configured:
            results["ntfy"] = self.ntfy.send_alert("REGIME", message)
        if self.discord.configured:
            results["discord"] = self.discord.send_alert("REGIME", message)

        return results

    def send_approaching_target_alert(self, stocks: list[dict]) -> dict:
        """
        Send alert when Tier 2 stocks approach their target entry price.

        Args:
            stocks: List of dicts with symbol, current_price, target_entry_price, price_gap_pct
        """
        if not stocks:
            return {}

        lines = [
            "APPROACHING TARGET PRICE ALERT",
            "",
            "The following Tier 2 stocks are approaching their target entry price:",
            "",
        ]

        for stock in stocks:
            sym = stock.get("symbol", "?")
            price = stock.get("current_price", 0)
            target = stock.get("target_entry_price", 0)
            gap = stock.get("price_gap_pct", 0)
            lines.append(f"  {sym}: ${price:,.0f} -> target ${target:,.0f} ({gap:+.0%})")

        lines.append("")
        lines.append("Consider preparing staged entry plans for these positions.")

        message = "\n".join(lines)

        results = {}
        if self.email.configured:
            results["email"] = self.email.send_briefing(
                message, subject="Approaching Target: " + ", ".join(s.get("symbol", "") for s in stocks[:5])
            )
        if self.telegram.configured:
            results["telegram"] = self.telegram.send_alert("TARGET", message)
        if self.ntfy.configured:
            results["ntfy"] = self.ntfy.send_alert("TARGET", message)
        if self.discord.configured:
            results["discord"] = self.discord.send_alert("TARGET", message)

        return results


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    logging.basicConfig(level=logging.INFO)

    # Test notifications
    manager = NotificationManager()

    # Test briefing
    test_briefing = """
    ==================================================
    TEST BRIEFING
    ==================================================

    This is a test of the notification system.

    Tier 1 Entry: ACME ($175 target)
    """

    results = manager.send_briefing(test_briefing)
    print(f"Notification results: {results}")
