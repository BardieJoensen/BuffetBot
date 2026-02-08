"""
Portfolio Tracker Module

Tracks your positions, monitors performance, and manages risk.

Features:
- Position tracking with cost basis
- Performance metrics vs benchmark
- Sector exposure monitoring
- Position sizing recommendations
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import yfinance as yf

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """A single position in your portfolio"""

    symbol: str
    shares: float
    cost_basis: float  # Price per share when bought
    purchase_date: date
    thesis: str  # Why you bought it
    thesis_breaking_events: list[str] = field(default_factory=list)
    conviction: str = "MEDIUM"  # HIGH, MEDIUM, LOW

    # Updated by tracker
    current_price: Optional[float] = None
    current_value: Optional[float] = None
    gain_loss: Optional[float] = None
    gain_loss_pct: Optional[float] = None
    sector: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "shares": self.shares,
            "cost_basis": self.cost_basis,
            "purchase_date": self.purchase_date.isoformat()
            if isinstance(self.purchase_date, date)
            else self.purchase_date,
            "thesis": self.thesis,
            "thesis_breaking_events": self.thesis_breaking_events,
            "conviction": self.conviction,
            "current_price": self.current_price,
            "current_value": self.current_value,
            "gain_loss": self.gain_loss,
            "gain_loss_pct": self.gain_loss_pct,
            "sector": self.sector,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Position":
        raw_date = data.get("purchase_date")
        if isinstance(raw_date, str):
            purchase_date = date.fromisoformat(raw_date)
        elif isinstance(raw_date, date):
            purchase_date = raw_date
        else:
            purchase_date = date.today()

        return cls(
            symbol=data["symbol"],
            shares=data["shares"],
            cost_basis=data["cost_basis"],
            purchase_date=purchase_date,
            thesis=data.get("thesis", ""),
            thesis_breaking_events=data.get("thesis_breaking_events", []),
            conviction=data.get("conviction", "MEDIUM"),
            current_price=data.get("current_price"),
            current_value=data.get("current_value"),
            gain_loss=data.get("gain_loss"),
            gain_loss_pct=data.get("gain_loss_pct"),
            sector=data.get("sector"),
        )


@dataclass
class TradeRecord:
    """Record of a completed trade for performance tracking"""

    symbol: str
    action: str  # BUY or SELL
    shares: float
    price: float
    date: date
    reason: str  # Why you made this trade

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "action": self.action,
            "shares": self.shares,
            "price": self.price,
            "date": self.date.isoformat() if isinstance(self.date, date) else self.date,
            "reason": self.reason,
        }


@dataclass
class PerformanceMetrics:
    """Portfolio performance summary"""

    total_invested: float
    current_value: float
    total_gain_loss: float
    total_gain_loss_pct: float

    # Trade statistics
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float

    # vs Benchmark
    benchmark_return: Optional[float] = None
    alpha: Optional[float] = None  # Your return - benchmark return

    # Time-based
    period_start: Optional[date] = None
    period_end: Optional[date] = None


class PortfolioTracker:
    """
    Tracks portfolio positions and performance.
    """

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.portfolio_file = self.data_dir / "portfolio.json"
        self.trades_file = self.data_dir / "trades.json"

        self.positions: list[Position] = []
        self.trades: list[TradeRecord] = []
        self.cash: float = 0

        self._load()

    def _load(self):
        """Load portfolio from disk"""
        if self.portfolio_file.exists():
            data = json.loads(self.portfolio_file.read_text())
            self.positions = [Position.from_dict(p) for p in data.get("positions", [])]
            self.cash = data.get("cash", 0)

        if self.trades_file.exists():
            data = json.loads(self.trades_file.read_text())
            self.trades = [TradeRecord(**t) for t in data.get("trades", [])]

    def _save(self):
        """Save portfolio to disk"""
        portfolio_data = {
            "updated_at": datetime.now().isoformat(),
            "cash": self.cash,
            "positions": [p.to_dict() for p in self.positions],
        }
        self.portfolio_file.write_text(json.dumps(portfolio_data, indent=2))

        trades_data = {"trades": [t.to_dict() for t in self.trades]}
        self.trades_file.write_text(json.dumps(trades_data, indent=2))

    def add_position(
        self,
        symbol: str,
        shares: float,
        price: float,
        thesis: str,
        thesis_breaking_events: list[str],
        conviction: str = "MEDIUM",
    ):
        """Add a new position to portfolio"""
        position = Position(
            symbol=symbol.upper(),
            shares=shares,
            cost_basis=price,
            purchase_date=date.today(),
            thesis=thesis,
            thesis_breaking_events=thesis_breaking_events,
            conviction=conviction,
        )

        # Check if position already exists
        existing = next((p for p in self.positions if p.symbol == symbol.upper()), None)
        if existing:
            # Average into position
            total_shares = existing.shares + shares
            total_cost = (existing.shares * existing.cost_basis) + (shares * price)
            existing.shares = total_shares
            existing.cost_basis = total_cost / total_shares
        else:
            self.positions.append(position)

        # Record the trade
        self.trades.append(
            TradeRecord(
                symbol=symbol.upper(), action="BUY", shares=shares, price=price, date=date.today(), reason=thesis
            )
        )

        self._save()
        logger.info(f"Added position: {shares} shares of {symbol} at ${price}")

    def close_position(self, symbol: str, price: float, reason: str):
        """Sell entire position"""
        symbol = symbol.upper()
        position = next((p for p in self.positions if p.symbol == symbol), None)

        if not position:
            logger.warning(f"No position found for {symbol}")
            return

        # Record the trade
        self.trades.append(
            TradeRecord(
                symbol=symbol, action="SELL", shares=position.shares, price=price, date=date.today(), reason=reason
            )
        )

        # Remove position
        self.positions = [p for p in self.positions if p.symbol != symbol]

        self._save()
        logger.info(f"Closed position: {symbol} at ${price}")

    def update_prices(self):
        """Fetch current prices for all positions using yfinance"""
        if not self.positions:
            return

        for position in self.positions:
            try:
                ticker = yf.Ticker(position.symbol)
                info = ticker.info

                price = info.get("regularMarketPrice") or info.get("currentPrice")
                if price:
                    position.current_price = price
                    position.current_value = price * position.shares
                    position.gain_loss = position.current_value - (position.cost_basis * position.shares)
                    position.gain_loss_pct = (price - position.cost_basis) / position.cost_basis
                    position.sector = info.get("sector", "Unknown")

            except Exception as e:
                logger.error(f"Error fetching price for {position.symbol}: {e}")

        self._save()

    def get_sector_exposure(self) -> dict[str, float]:
        """Calculate sector allocation percentages"""
        self.update_prices()

        total_value = sum(p.current_value or 0 for p in self.positions)
        if total_value == 0:
            return {}

        sectors: dict[str, float] = {}
        for position in self.positions:
            sector = position.sector or "Unknown"
            value = position.current_value or 0.0

            if sector not in sectors:
                sectors[sector] = 0.0
            sectors[sector] += value

        # Convert to percentages
        return {sector: value / total_value for sector, value in sectors.items()}

    def get_sector_warnings(self, max_sector_pct: float = 0.30) -> list[str]:
        """Check if any sector is overweight"""
        exposure = self.get_sector_exposure()
        warnings = []

        for sector, pct in exposure.items():
            if pct > max_sector_pct:
                warnings.append(f"{sector}: {pct:.1%} (above {max_sector_pct:.0%} threshold)")

        return warnings

    def get_performance_metrics(self) -> PerformanceMetrics:
        """Calculate overall portfolio performance"""
        self.update_prices()

        total_invested = sum(p.cost_basis * p.shares for p in self.positions)
        current_value = sum(p.current_value or 0 for p in self.positions)
        total_gain_loss = current_value - total_invested
        total_gain_loss_pct = total_gain_loss / total_invested if total_invested > 0 else 0

        # Analyze closed trades
        closed_trades = self._get_closed_trades()
        winning = sum(1 for t in closed_trades if t["gain_loss"] > 0)
        losing = sum(1 for t in closed_trades if t["gain_loss"] <= 0)
        total = winning + losing

        return PerformanceMetrics(
            total_invested=total_invested,
            current_value=current_value,
            total_gain_loss=total_gain_loss,
            total_gain_loss_pct=total_gain_loss_pct,
            total_trades=total,
            winning_trades=winning,
            losing_trades=losing,
            win_rate=winning / total if total > 0 else 0,
        )

    def _get_closed_trades(self) -> list[dict]:
        """Match buy/sell trades to calculate gains"""
        closed = []

        # Group trades by symbol
        by_symbol: dict[str, dict[str, list]] = {}
        for trade in self.trades:
            if trade.symbol not in by_symbol:
                by_symbol[trade.symbol] = {"buys": [], "sells": []}

            if trade.action == "BUY":
                by_symbol[trade.symbol]["buys"].append(trade)
            else:
                by_symbol[trade.symbol]["sells"].append(trade)

        # Calculate gains for symbols that have been sold
        for symbol, trades in by_symbol.items():
            if trades["sells"]:
                # Simplified: average cost basis
                total_bought = sum(t.shares * t.price for t in trades["buys"])
                shares_bought = sum(t.shares for t in trades["buys"])
                avg_cost = total_bought / shares_bought if shares_bought > 0 else 0

                for sell in trades["sells"]:
                    gain_loss = (sell.price - avg_cost) * sell.shares
                    closed.append(
                        {
                            "symbol": symbol,
                            "gain_loss": gain_loss,
                            "gain_loss_pct": (sell.price - avg_cost) / avg_cost if avg_cost > 0 else 0,
                        }
                    )

        return closed

    def get_portfolio_summary(self) -> dict:
        """Get complete portfolio summary for briefing"""
        self.update_prices()

        metrics = self.get_performance_metrics()
        exposure = self.get_sector_exposure()
        warnings = self.get_sector_warnings()

        return {
            "positions": [p.to_dict() for p in self.positions],
            "position_count": len(self.positions),
            "total_invested": metrics.total_invested,
            "current_value": metrics.current_value,
            "total_gain_loss": metrics.total_gain_loss,
            "total_gain_loss_pct": metrics.total_gain_loss_pct,
            "sector_exposure": exposure,
            "sector_warnings": warnings,
            "performance": {
                "total_trades": metrics.total_trades,
                "winning_trades": metrics.winning_trades,
                "losing_trades": metrics.losing_trades,
                "win_rate": metrics.win_rate,
            },
        }


def calculate_position_size(
    portfolio_value: float, conviction: str, current_positions: int, max_position_pct: float = 0.15
) -> dict:
    """
    Calculate recommended position size based on conviction and portfolio.

    Returns dollar amount and percentage.
    """

    # Base allocation by conviction
    conviction_allocation = {
        "HIGH": 0.12,  # 12% of portfolio
        "MEDIUM": 0.08,  # 8% of portfolio
        "LOW": 0.05,  # 5% of portfolio
    }

    base_pct = conviction_allocation.get(conviction, 0.08)

    # Adjust if portfolio is concentrated
    if current_positions < 5:
        # Fewer positions = can allocate more per position
        base_pct = min(base_pct * 1.2, max_position_pct)
    elif current_positions > 10:
        # Many positions = smaller per position
        base_pct = base_pct * 0.8

    # Calculate amounts
    dollar_amount = portfolio_value * base_pct

    return {
        "conviction": conviction,
        "recommended_pct": base_pct,
        "recommended_amount": dollar_amount,
        "max_pct": max_position_pct,
        "max_amount": portfolio_value * max_position_pct,
        "reasoning": f"{conviction} conviction = {base_pct:.0%} allocation (${dollar_amount:,.0f})",
    }


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    logging.basicConfig(level=logging.INFO)

    tracker = PortfolioTracker(data_dir="/tmp/test_portfolio")

    # Test adding positions
    tracker.add_position(
        symbol="AAPL",
        shares=10,
        price=150.00,
        thesis="Strong ecosystem moat",
        thesis_breaking_events=["Loss of iPhone market share below 15%"],
        conviction="HIGH",
    )

    tracker.add_position(
        symbol="MSFT",
        shares=5,
        price=380.00,
        thesis="Cloud dominance",
        thesis_breaking_events=["Azure growth below 20%"],
        conviction="MEDIUM",
    )

    summary = tracker.get_portfolio_summary()
    print(json.dumps(summary, indent=2, default=str))

    # Test position sizing
    sizing = calculate_position_size(portfolio_value=50000, conviction="HIGH", current_positions=2)
    print(f"\nPosition sizing: {sizing}")
