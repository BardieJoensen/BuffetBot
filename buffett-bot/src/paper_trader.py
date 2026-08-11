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

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .config import config

logger = logging.getLogger(__name__)

# Trade log location
_trade_log_dir = Path("./data")

# Symbol -> (tradable, status, checked_at_monotonic). Module-level because the
# scheduler is one long-lived process: after the first pass the tradability
# check costs nothing. A delisting is permanent, but a trading halt is not, so
# entries expire rather than pinning forever — a re-listed or un-halted name
# recovers within the TTL instead of needing a container restart.
_ASSET_CACHE: dict[str, tuple[bool, str, float]] = {}


class BrokerReadError(RuntimeError):
    """Raised when broker state cannot be read safely for a trade decision."""


def _enum_value(value) -> str:
    """Normalize Alpaca enums across SDK versions (BUY -> ``buy``)."""
    raw = getattr(value, "value", value)
    return str(raw).lower().rsplit(".", 1)[-1]


def _finite_float(value) -> Optional[float]:
    """Return a finite float, otherwise None."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive_finite(value) -> Optional[float]:
    """Return a positive finite broker value, otherwise None."""
    number = _finite_float(value)
    return number if number is not None and number > 0 else None


def _order_result(order, *, symbol: str, side: str, **extra) -> dict:
    """Build a stable result shape from an Alpaca order object."""
    filled_price = getattr(order, "filled_avg_price", None)
    filled_qty = getattr(order, "filled_qty", None)
    return {
        "symbol": symbol,
        "side": side,
        "order_id": str(order.id),
        "status": _enum_value(order.status),
        "filled_avg_price": _positive_finite(filled_price),
        "filled_qty": _positive_finite(filled_qty),
        **extra,
    }


def set_trade_log_dir(path: Path):
    """Override the trade log directory"""
    global _trade_log_dir
    _trade_log_dir = path


def clear_asset_cache() -> None:
    """Reset the tradability cache. Tests must call this between cases —
    module-level state otherwise bleeds and produces order-dependent failures."""
    _ASSET_CACHE.clear()


class PaperTrader:
    """
    Paper trading via Alpaca API.

    Reads ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_PAPER from environment.
    If keys are missing, all operations are no-ops (graceful degradation).

    Kill switch: set AUTO_TRADE_ENABLED=false in .env to disable all
    automated weekly trades without removing Alpaca keys. Briefing-triggered
    orders have a separate BRIEFING_PAPER_TRADES_ENABLED gate in the manual
    pipeline.
    """

    MAX_POSITION_PCT = config.max_position_pct

    def __init__(self):
        self._enabled = False
        self._client: Any = None
        self._trading_client: Any = None
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
        Defaults to False so unattended trading is explicitly opt-in.
        """
        return config.auto_trade_enabled

    def is_enabled(self) -> bool:
        """Returns True if paper trading is properly configured."""
        return self._enabled

    def get_account(self) -> dict:
        """Return account balance info."""
        if not self._enabled:
            return {"error": "Trading not enabled"}

        try:
            account = self._trading_client.get_account()
            result = {
                "equity": float(account.equity),
                "cash": float(account.cash),
                "buying_power": float(account.buying_power),
                "portfolio_value": float(account.portfolio_value),
            }
            if not all(math.isfinite(value) for value in result.values()):
                raise ValueError("broker returned non-finite account values")
            if result["equity"] < 0 or result["portfolio_value"] <= 0 or result["buying_power"] < 0:
                raise ValueError("broker returned an invalid account balance")
            return result
        except Exception as e:
            logger.error(f"Failed to get account info: {e}")
            return {"error": str(e)}

    def get_asset_status(self, symbol: str) -> tuple[bool, str]:
        """
        Return (tradable, broker_status) for one symbol, cached with a TTL.

        Fails OPEN: anything that isn't a definitive 404 returns
        (True, "unknown"). The returned value is only ever used to *subtract*
        value from equity, so treating an Alpaca outage as "everything is
        untradable" would collapse reported equity toward cash, blow the
        deployment gap up to nearly the whole cash balance, and fire the
        overweight-rotation branch on every holding at once. A 404 is
        different — Alpaca not knowing the symbol is an answer, not a failure.
        """
        if not self._enabled:
            return True, "unknown"

        cached = _ASSET_CACHE.get(symbol)
        if cached and (time.monotonic() - cached[2]) < config.asset_status_cache_hours * 3600:
            return cached[0], cached[1]

        try:
            asset = self._trading_client.get_asset(symbol)
            tradable = bool(asset.tradable)
            # str(AssetStatus.INACTIVE) renders as "AssetStatus.INACTIVE".
            status = str(getattr(asset, "status", "")).lower().rsplit(".", 1)[-1]
        except Exception as e:
            if getattr(e, "status_code", None) == 404:
                tradable, status = False, "not_found"
            else:
                logger.warning("Asset lookup failed for %s: %s — assuming tradable", symbol, e)
                return True, "unknown"

        if not tradable:
            logger.warning("%s is not tradable at the broker (status=%s)", symbol, status)
        _ASSET_CACHE[symbol] = (tradable, status, time.monotonic())
        return tradable, status

    def get_positions(self) -> list[dict]:
        """
        Return current paper positions, annotated with broker tradability.

        Positions are annotated rather than filtered: this method also answers
        "do we already hold this?" for duplicate-buy prevention and for the
        sell path, so dropping a delisted holding here would make the bot
        believe it doesn't hold it — and therefore that it is buyable again.
        Callers that need to exclude quarantined holdings filter on "tradable".
        """
        if not self._enabled:
            return []

        try:
            positions = self._trading_client.get_all_positions()
            out = []
            for p in positions:
                tradable, status = self.get_asset_status(p.symbol)
                qty = _positive_finite(p.qty)
                market_value = _finite_float(p.market_value)
                avg_entry_price = _positive_finite(p.avg_entry_price)
                current_price = _finite_float(p.current_price)
                unrealized_pl = _finite_float(p.unrealized_pl)
                unrealized_plpc = _finite_float(p.unrealized_plpc)
                if (
                    qty is None
                    or market_value is None
                    or avg_entry_price is None
                    or current_price is None
                    or unrealized_pl is None
                    or unrealized_plpc is None
                ):
                    raise ValueError(f"broker returned invalid position values for {p.symbol}")
                if market_value < 0 or current_price < 0 or (tradable and (market_value == 0 or current_price == 0)):
                    raise ValueError(f"broker returned non-actionable position values for {p.symbol}")
                out.append(
                    {
                        "symbol": p.symbol,
                        "qty": qty,
                        "market_value": market_value,
                        "avg_entry_price": avg_entry_price,
                        "current_price": current_price,
                        "unrealized_pl": unrealized_pl,
                        "unrealized_plpc": unrealized_plpc,
                        "tradable": tradable,
                        "asset_status": status,
                    }
                )
            return out
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            raise BrokerReadError(f"Failed to get positions: {e}") from e

    def get_open_orders(self) -> list[dict]:
        """Return open/pending orders."""
        if not self._enabled:
            return []

        try:
            from alpaca.trading.enums import QueryOrderStatus
            from alpaca.trading.requests import GetOrdersRequest

            request = GetOrdersRequest(status=QueryOrderStatus.OPEN)
            orders = self._trading_client.get_orders(filter=request)
            return [
                {
                    "symbol": o.symbol,
                    "side": _enum_value(o.side),
                    "notional": float(o.notional) if o.notional else None,
                    "qty": float(o.qty) if o.qty else None,
                    "status": _enum_value(o.status),
                    "order_id": str(o.id),
                }
                for o in orders
            ]
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            raise BrokerReadError(f"Failed to get open orders: {e}") from e

    def get_order(self, order_id: str) -> dict:
        """Return normalized status and fill details for one submitted order."""
        if not self._enabled:
            raise BrokerReadError("Trading not enabled")
        try:
            order = self._trading_client.get_order_by_id(order_id)
            return _order_result(order, symbol=order.symbol, side=_enum_value(order.side))
        except Exception as e:
            raise BrokerReadError(f"Failed to get order {order_id}: {e}") from e

    def buy(self, symbol: str, dollar_amount: float) -> Optional[dict]:
        """
        Place a market buy order for a given dollar amount.

        Safety checks:
        - Refuses symbols the broker reports as non-tradable
        - Validates dollar_amount against MAX_POSITION_PCT of account value
        - Prevents duplicate buys of same symbol
        """
        if not self._enabled:
            logger.warning(f"Paper trading disabled — skipping buy for {symbol}")
            return None

        try:
            amount = _positive_finite(dollar_amount)
            if amount is None:
                logger.warning("Invalid buy amount %r for %s", dollar_amount, symbol)
                return None
            dollar_amount = amount

            # Refuse up front rather than letting the broker reject it. The
            # blanket except below cannot tell a "not tradable" rejection from
            # a network blip, so the only way to surface this clearly is to
            # never submit the doomed order.
            tradable, status = self.get_asset_status(symbol)
            if not tradable or status == "unknown":
                logger.error("Refusing to buy %s — broker reports it non-tradable (status=%s)", symbol, status)
                return None

            # Check for existing position (prevent duplicates)
            existing = self.get_positions()
            if any(p["symbol"] == symbol for p in existing):
                logger.warning(f"Already holding {symbol} — skipping duplicate buy")
                return None
            tradable_position_count = sum(1 for position in existing if position.get("tradable", True))
            if tradable_position_count >= config.max_positions:
                logger.warning(
                    "Portfolio already has %d/%d tradable positions — skipping %s",
                    tradable_position_count,
                    config.max_positions,
                    symbol,
                )
                return None

            # Check for pending/open orders (e.g. market closed, order queued)
            pending = self.get_open_orders()
            if any(o["symbol"] == symbol and o["side"] == "buy" for o in pending):
                logger.warning(f"Already have a pending buy order for {symbol} — skipping duplicate")
                return None

            # Validate position size against account
            account = self.get_account()
            if "error" in account:
                raise BrokerReadError(f"Failed to validate account before buy: {account['error']}")
            portfolio_value = _positive_finite(account.get("portfolio_value"))
            buying_power = _positive_finite(account.get("buying_power"))
            if portfolio_value is None or buying_power is None:
                logger.error("Refusing to buy %s — broker returned invalid portfolio value or buying power", symbol)
                return None
            max_position_pct = _positive_finite(self.MAX_POSITION_PCT)
            if max_position_pct is None or max_position_pct > 1:
                logger.error("Refusing to buy %s — MAX_POSITION_PCT is invalid", symbol)
                return None
            max_amount = portfolio_value * max_position_pct
            allowed_amount = min(max_amount, buying_power)
            if dollar_amount > allowed_amount:
                logger.warning(
                    "Requested $%.0f for %s exceeds the position/buying-power limit ($%.0f). Capping.",
                    dollar_amount,
                    symbol,
                    allowed_amount,
                )
                dollar_amount = allowed_amount

            if _positive_finite(dollar_amount) is None:
                logger.warning("Position-size cap left no valid buy amount for %s", symbol)
                return None

            from alpaca.trading.enums import OrderSide, TimeInForce
            from alpaca.trading.requests import MarketOrderRequest

            order_request = MarketOrderRequest(
                symbol=symbol,
                notional=round(dollar_amount, 2),
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                client_order_id=self._client_order_id(symbol, "buy"),
            )

            order = self._trading_client.submit_order(order_request)
            logger.info(f"BUY order submitted: {symbol} for ${dollar_amount:,.2f} (order {order.id})")

            result = _order_result(order, symbol=symbol, side="buy", notional=dollar_amount)

            self._log_trade(result)
            self._notify_trade(result)
            return result

        except Exception as e:
            logger.error(f"Failed to buy {symbol}: {e}")
            return None

    def sell(self, symbol: str, reason: str = "", quantity: Optional[float] = None) -> Optional[dict]:
        """
        Sell all or part of a position in a symbol.

        Args:
            symbol: Stock ticker to sell
            reason: Why we're selling (logged for record-keeping)
            quantity: Shares to sell. None sells the entire position.
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

            # A delisted holding is stuck: the broker rejects the order, so
            # retrying weekly just produces noise. It is quarantined elsewhere
            # (excluded from equity, sizing and slots) and surfaced by alert.
            if not held[0].get("tradable", True):
                logger.error(
                    "Refusing to sell %s — broker reports it non-tradable (status=%s). "
                    "The order would be rejected; resolve this position manually.",
                    symbol,
                    held[0].get("asset_status"),
                )
                return None

            pending = self.get_open_orders()
            if any(o["symbol"] == symbol and o["side"] == "sell" for o in pending):
                logger.warning("Already have a pending sell order for %s — skipping duplicate", symbol)
                return None

            from alpaca.trading.enums import OrderSide, TimeInForce
            from alpaca.trading.requests import MarketOrderRequest

            held_qty = _positive_finite(held[0].get("qty"))
            requested_qty = held_qty if quantity is None else _positive_finite(quantity)
            if held_qty is None or requested_qty is None:
                logger.warning("Invalid sell quantity %s for %s", quantity, symbol)
                return None
            qty = min(requested_qty, held_qty)
            order_request = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
                client_order_id=self._client_order_id(symbol, "sell"),
            )

            order = self._trading_client.submit_order(order_request)
            logger.info(f"SELL order submitted: {symbol} x{qty} (reason: {reason}) (order {order.id})")

            result = _order_result(order, symbol=symbol, side="sell", qty=qty, reason=reason)

            self._log_trade(result)
            self._notify_trade(result)
            return result

        except Exception as e:
            logger.error(f"Failed to sell {symbol}: {e}")
            return None

    def get_portfolio_summary(self) -> dict:
        """
        Build a portfolio summary from Alpaca positions.

        Returns the same structure as PortfolioTracker.get_portfolio_summary()
        so the briefing can use either source seamlessly.
        """
        if not self._enabled:
            return {}

        try:
            account = self.get_account()
            all_positions = self.get_positions()

            # Quarantined holdings are excluded from every figure below: their
            # mark is frozen at the last price before delisting, so including
            # them overstates value and fabricates a P/L that can never change.
            positions = [p for p in all_positions if p.get("tradable", True)]
            untradable = [p for p in all_positions if not p.get("tradable", True)]
            untradable_value = sum(p["market_value"] for p in untradable)

            if not positions:
                return {
                    "positions": [],
                    "position_count": 0,
                    "total_invested": 0,
                    "current_value": float(account.get("equity", 0)) - untradable_value,
                    "total_gain_loss": 0,
                    "total_gain_loss_pct": 0,
                    "sector_exposure": {},
                    "sector_warnings": [],
                    "untradable_positions": untradable,
                    "untradable_value": untradable_value,
                }

            total_invested = sum(p["avg_entry_price"] * p["qty"] for p in positions)
            current_value = sum(p["market_value"] for p in positions)
            total_pl = sum(p["unrealized_pl"] for p in positions)
            total_pl_pct = total_pl / total_invested if total_invested > 0 else 0

            # Fetch sectors for exposure calculation
            import yfinance as yf

            sector_values: dict[str, float] = {}
            for p in positions:
                try:
                    info = yf.Ticker(p["symbol"]).info
                    sector = info.get("sector", "Unknown")
                except Exception:
                    sector = "Unknown"
                p["sector"] = sector
                sector_values[sector] = sector_values.get(sector, 0) + p["market_value"]

            sector_exposure = {s: v / current_value for s, v in sector_values.items()} if current_value > 0 else {}

            sector_warnings = [
                f"{s}: {pct:.1%} (above 30% threshold)" for s, pct in sector_exposure.items() if pct > 0.30
            ]

            return {
                "positions": positions,
                "position_count": len(positions),
                "total_invested": total_invested,
                "current_value": current_value,
                "total_gain_loss": total_pl,
                "total_gain_loss_pct": total_pl_pct,
                "sector_exposure": sector_exposure,
                "sector_warnings": sector_warnings,
                "untradable_positions": untradable,
                "untradable_value": untradable_value,
            }

        except Exception as e:
            logger.error(f"Failed to build Alpaca portfolio summary: {e}")
            return {}

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

    @staticmethod
    def _client_order_id(symbol: str, side: str) -> str:
        """Deterministic daily key; broker rejection is a final duplicate guard."""
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        return f"buffettbot-{day}-{side}-{symbol}"[:48]

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

            status = trade.get("status", "submitted")
            event = "Trade filled" if status == "filled" else f"Order submitted ({status})"
            self._notifier.send_alert(symbol, f"{event}: {message}")

        except Exception as e:
            # Notifications are best-effort — don't break trading on failure
            logger.warning(f"Failed to send trade notification: {e}")
