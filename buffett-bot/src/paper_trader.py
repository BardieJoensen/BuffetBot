"""
Paper Trading Module

Wraps Alpaca's paper trading API for automated order execution.
Gracefully degrades if Alpaca credentials are not configured.

Safety features:
- Position size limits (MAX_POSITION_PCT of account)
- Duplicate buy prevention
- Paper trading only (never real money)
- Kill switch via AUTO_TRADE_ENABLED=false in .env
- All trades logged to data/trade_log.json
- Trade notifications sent via configured channels
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Trade log location
_trade_log_dir = Path("./data")


def set_trade_log_dir(path: Path):
    """Override the trade log directory"""
    global _trade_log_dir
    _trade_log_dir = path


class PaperTrader:
    """
    Paper trading via Alpaca API.

    Reads ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_PAPER from environment.
    If keys are missing, all operations are no-ops (graceful degradation).

    Kill switch: set AUTO_TRADE_ENABLED=false in .env to disable all
    automated trades without removing Alpaca keys. Manual trades from
    run_monthly_briefing.py still work when this is false — only the
    scheduler's weekly_auto_trade checks this flag.
    """

    MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", 0.15))  # 15% max per position

    def __init__(self):
        self._enabled = False
        self._client = None
        self._trading_client = None
        self._notifier = None

        api_key = os.getenv("ALPACA_API_KEY", "")
        secret_key = os.getenv("ALPACA_SECRET_KEY", "")
        paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"

        if not api_key or not secret_key or api_key.startswith("your_"):
            logger.warning("Alpaca API keys not configured — paper trading disabled")
            return

        if not paper:
            logger.warning("ALPACA_PAPER is not true — refusing to trade with real money")
            return

        try:
            from alpaca.trading.client import TradingClient
            self._trading_client = TradingClient(api_key, secret_key, paper=True)
            self._enabled = True
            logger.info("Alpaca paper trading initialized")
        except ImportError:
            logger.warning("alpaca-py not installed — paper trading disabled")
        except Exception as e:
            logger.warning(f"Alpaca initialization failed: {e}")

    @staticmethod
    def auto_trade_enabled() -> bool:
        """
        Check the AUTO_TRADE_ENABLED kill switch.

        Returns False if AUTO_TRADE_ENABLED is explicitly set to 'false'.
        Defaults to True if not set (backward compatible).
        """
        return os.getenv("AUTO_TRADE_ENABLED", "true").lower() != "false"

    def is_enabled(self) -> bool:
        """Returns True if paper trading is properly configured."""
        return self._enabled

    def get_account(self) -> dict:
        """Return account balance info."""
        if not self._enabled:
            return {"error": "Trading not enabled"}

        try:
            account = self._trading_client.get_account()
            return {
                "equity": float(account.equity),
                "cash": float(account.cash),
                "buying_power": float(account.buying_power),
                "portfolio_value": float(account.portfolio_value),
            }
        except Exception as e:
            logger.error(f"Failed to get account info: {e}")
            return {"error": str(e)}

    def get_positions(self) -> list[dict]:
        """Return current paper positions."""
        if not self._enabled:
            return []

        try:
            positions = self._trading_client.get_all_positions()
            return [
                {
                    "symbol": p.symbol,
                    "qty": float(p.qty),
                    "market_value": float(p.market_value),
                    "avg_entry_price": float(p.avg_entry_price),
                    "current_price": float(p.current_price),
                    "unrealized_pl": float(p.unrealized_pl),
                    "unrealized_plpc": float(p.unrealized_plpc),
                }
                for p in positions
            ]
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            return []

    def get_open_orders(self) -> list[dict]:
        """Return open/pending orders."""
        if not self._enabled:
            return []

        try:
            from alpaca.trading.requests import GetOrdersRequest
            from alpaca.trading.enums import QueryOrderStatus

            request = GetOrdersRequest(status=QueryOrderStatus.OPEN)
            orders = self._trading_client.get_orders(filter=request)
            return [
                {
                    "symbol": o.symbol,
                    "side": str(o.side).lower(),
                    "notional": float(o.notional) if o.notional else None,
                    "qty": float(o.qty) if o.qty else None,
                    "status": str(o.status),
                    "order_id": str(o.id),
                }
                for o in orders
            ]
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []

    def buy(self, symbol: str, dollar_amount: float) -> Optional[dict]:
        """
        Place a market buy order for a given dollar amount.

        Safety checks:
        - Validates dollar_amount against MAX_POSITION_PCT of account value
        - Prevents duplicate buys of same symbol
        """
        if not self._enabled:
            logger.warning(f"Paper trading disabled — skipping buy for {symbol}")
            return None

        try:
            # Check for existing position (prevent duplicates)
            existing = self.get_positions()
            if any(p["symbol"] == symbol for p in existing):
                logger.warning(f"Already holding {symbol} — skipping duplicate buy")
                return None

            # Check for pending/open orders (e.g. market closed, order queued)
            pending = self.get_open_orders()
            if any(o["symbol"] == symbol and o["side"] == "buy" for o in pending):
                logger.warning(f"Already have a pending buy order for {symbol} — skipping duplicate")
                return None

            # Validate position size against account
            account = self.get_account()
            portfolio_value = account.get("portfolio_value", 0)
            if portfolio_value > 0:
                max_amount = portfolio_value * self.MAX_POSITION_PCT
                if dollar_amount > max_amount:
                    logger.warning(
                        f"Requested ${dollar_amount:,.0f} for {symbol} exceeds "
                        f"{self.MAX_POSITION_PCT:.0%} limit (${max_amount:,.0f}). Capping."
                    )
                    dollar_amount = max_amount

            if dollar_amount <= 0:
                logger.warning(f"Invalid buy amount ${dollar_amount} for {symbol}")
                return None

            from alpaca.trading.requests import MarketOrderRequest
            from alpaca.trading.enums import OrderSide, TimeInForce

            order_request = MarketOrderRequest(
                symbol=symbol,
                notional=round(dollar_amount, 2),
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
            )

            order = self._trading_client.submit_order(order_request)
            logger.info(f"BUY order submitted: {symbol} for ${dollar_amount:,.2f} (order {order.id})")

            result = {
                "symbol": symbol,
                "side": "buy",
                "notional": dollar_amount,
                "order_id": str(order.id),
                "status": str(order.status),
            }

            self._log_trade(result)
            self._notify_trade(result)
            return result

        except Exception as e:
            logger.error(f"Failed to buy {symbol}: {e}")
            return None

    def sell(self, symbol: str, reason: str = "") -> Optional[dict]:
        """
        Sell entire position in a symbol.

        Args:
            symbol: Stock ticker to sell
            reason: Why we're selling (logged for record-keeping)
        """
        if not self._enabled:
            logger.warning(f"Paper trading disabled — skipping sell for {symbol}")
            return None

        try:
            # Check we actually hold it
            positions = self.get_positions()
            held = [p for p in positions if p["symbol"] == symbol]
            if not held:
                logger.warning(f"No position in {symbol} to sell")
                return None

            from alpaca.trading.requests import MarketOrderRequest
            from alpaca.trading.enums import OrderSide, TimeInForce

            qty = held[0]["qty"]
            order_request = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
            )

            order = self._trading_client.submit_order(order_request)
            logger.info(f"SELL order submitted: {symbol} x{qty} (reason: {reason}) (order {order.id})")

            result = {
                "symbol": symbol,
                "side": "sell",
                "qty": qty,
                "order_id": str(order.id),
                "status": str(order.status),
                "reason": reason,
            }

            self._log_trade(result)
            self._notify_trade(result)
            return result

        except Exception as e:
            logger.error(f"Failed to sell {symbol}: {e}")
            return None

    def _log_trade(self, trade: dict):
        """Append trade to the JSON trade log file."""
        try:
            log_file = _trade_log_dir / "trade_log.json"
            log_file.parent.mkdir(parents=True, exist_ok=True)

            # Load existing log
            trades = []
            if log_file.exists():
                try:
                    trades = json.loads(log_file.read_text())
                except (json.JSONDecodeError, Exception):
                    trades = []

            # Append new trade with timestamp
            entry = {
                "timestamp": datetime.now().isoformat(),
                **trade,
            }
            trades.append(entry)

            log_file.write_text(json.dumps(trades, indent=2))
            logger.info(f"Trade logged to {log_file}")
        except Exception as e:
            logger.warning(f"Failed to log trade: {e}")

    def _notify_trade(self, trade: dict):
        """Send trade notification via all configured channels."""
        try:
            from src.notifications import NotificationManager

            if self._notifier is None:
                self._notifier = NotificationManager()

            symbol = trade["symbol"]
            side = trade["side"].upper()
            reason = trade.get("reason", "")

            if side == "BUY":
                amount = trade.get("notional", 0)
                message = f"{side} {symbol} for ${amount:,.2f}"
            else:
                qty = trade.get("qty", 0)
                message = f"{side} {symbol} x{qty}"
                if reason:
                    message += f"\nReason: {reason}"

            self._notifier.send_alert(symbol, f"Trade executed: {message}")

        except Exception as e:
            # Notifications are best-effort — don't break trading on failure
            logger.warning(f"Failed to send trade notification: {e}")
