"""
Portfolio Tracker Module

v2.0 — ASK (Aktiesparekonto) Portfolio Construction

Features:
- Position tracking with cost basis
- Performance metrics vs benchmark
- Sector exposure monitoring
- ASK-specific constraints:
  - 5-8 position concentration management
  - Conviction-based allocation (Tier 1 sizing)
  - Staged entry tracking
  - Annual contribution cycle tracking
  - Dividend tracking (17% tax in ASK)
  - Sector concentration warnings
  - Portfolio gap analysis (what you're missing)
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import yfinance as yf

logger = logging.getLogger(__name__)

# ASK constraints
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "8"))
MAX_SINGLE_POSITION_PCT = 0.25  # Never >25% in one stock
ASK_CONTRIBUTION_LIMIT_DKK = int(os.getenv("ASK_CONTRIBUTION_LIMIT", "135900"))  # 2026 limit


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
    tier: int = 1  # Tier when purchased

    # Staged entry tracking
    tranches_filled: int = 0  # How many of planned tranches are filled
    tranches_planned: int = 3  # Total planned tranches

    # Updated by tracker
    current_price: Optional[float] = None
    current_value: Optional[float] = None
    gain_loss: Optional[float] = None
    gain_loss_pct: Optional[float] = None
    sector: Optional[str] = None
    dividend_yield: Optional[float] = None

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
            "tier": self.tier,
            "tranches_filled": self.tranches_filled,
            "tranches_planned": self.tranches_planned,
            "current_price": self.current_price,
            "current_value": self.current_value,
            "gain_loss": self.gain_loss,
            "gain_loss_pct": self.gain_loss_pct,
            "sector": self.sector,
            "dividend_yield": self.dividend_yield,
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
            tier=data.get("tier", 1),
            tranches_filled=data.get("tranches_filled", 0),
            tranches_planned=data.get("tranches_planned", 3),
            current_price=data.get("current_price"),
            current_value=data.get("current_value"),
            gain_loss=data.get("gain_loss"),
            gain_loss_pct=data.get("gain_loss_pct"),
            sector=data.get("sector"),
            dividend_yield=data.get("dividend_yield"),
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
    tranche: int = 0  # Which tranche (1, 2, 3) for staged entries

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "action": self.action,
            "shares": self.shares,
            "price": self.price,
            "date": self.date.isoformat() if isinstance(self.date, date) else self.date,
            "reason": self.reason,
            "tranche": self.tranche,
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
    alpha: Optional[float] = None

    # Time-based
    period_start: Optional[date] = None
    period_end: Optional[date] = None


@dataclass
class ContributionStatus:
    """ASK contribution tracking for the current year."""

    year: int
    limit_dkk: int
    contributed_dkk: float
    remaining_dkk: float


class PortfolioTracker:
    """
    Tracks portfolio positions and performance.

    v2.0 — ASK-aware with concentration management.
    """

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.portfolio_file = self.data_dir / "portfolio.json"
        self.trades_file = self.data_dir / "trades.json"
        self.contributions_file = self.data_dir / "contributions.json"

        self.positions: list[Position] = []
        self.trades: list[TradeRecord] = []
        self.cash: float = 0

        self._load()

    def _load(self):
        """Load portfolio from disk"""
        if self.portfolio_file.exists():
            try:
                data = json.loads(self.portfolio_file.read_text())
                self.positions = [Position.from_dict(p) for p in data.get("positions", [])]
                self.cash = data.get("cash", 0)
            except Exception as e:
                logger.warning(f"Failed to load portfolio: {e}")

        if self.trades_file.exists():
            try:
                data = json.loads(self.trades_file.read_text())
                trades_raw = data.get("trades", [])
                for t in trades_raw:
                    if isinstance(t.get("date"), str):
                        t["date"] = date.fromisoformat(t["date"])
                self.trades = [TradeRecord(**t) for t in trades_raw]
            except Exception as e:
                logger.warning(f"Failed to load trades: {e}")

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
        thesis_breaking_events: list[str] | None = None,
        conviction: str = "MEDIUM",
        tier: int = 1,
        tranche: int = 1,
    ):
        """Add a new position to portfolio"""
        position = Position(
            symbol=symbol.upper(),
            shares=shares,
            cost_basis=price,
            purchase_date=date.today(),
            thesis=thesis,
            thesis_breaking_events=thesis_breaking_events or [],
            conviction=conviction,
            tier=tier,
            tranches_filled=tranche,
        )

        # Check if position already exists (adding to existing = staged entry)
        existing = next((p for p in self.positions if p.symbol == symbol.upper()), None)
        if existing:
            # Average into position
            total_shares = existing.shares + shares
            total_cost = (existing.shares * existing.cost_basis) + (shares * price)
            existing.shares = total_shares
            existing.cost_basis = total_cost / total_shares
            existing.tranches_filled = max(existing.tranches_filled, tranche)
        else:
            self.positions.append(position)

        # Record the trade
        self.trades.append(
            TradeRecord(
                symbol=symbol.upper(),
                action="BUY",
                shares=shares,
                price=price,
                date=date.today(),
                reason=thesis,
                tranche=tranche,
            )
        )

        self._save()
        logger.info(f"Added position: {shares} shares of {symbol} at ${price} (tranche {tranche})")

    def close_position(self, symbol: str, price: float, reason: str):
        """Sell entire position"""
        symbol = symbol.upper()
        position = next((p for p in self.positions if p.symbol == symbol), None)

        if not position:
            logger.warning(f"No position found for {symbol}")
            return

        self.trades.append(
            TradeRecord(
                symbol=symbol, action="SELL", shares=position.shares, price=price, date=date.today(), reason=reason
            )
        )

        self.positions = [p for p in self.positions if p.symbol != symbol]

        self._save()
        logger.info(f"Closed position: {symbol} at ${price}")

    def update_prices(self):
        """Fetch current prices for all positions using yfinance (cached for 60s)"""
        if not self.positions:
            return

        now = datetime.now()
        if hasattr(self, "_last_price_update") and (now - self._last_price_update).total_seconds() < 60:
            return
        self._last_price_update = now

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
                    position.dividend_yield = info.get("dividendYield")

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

        return {sector: value / total_value for sector, value in sectors.items()}

    def get_sector_warnings(self, max_sector_pct: float = 0.30) -> list[str]:
        """Check if any sector is overweight"""
        exposure = self.get_sector_exposure()
        warnings = []

        for sector, pct in exposure.items():
            if pct > max_sector_pct:
                warnings.append(f"{sector}: {pct:.1%} (above {max_sector_pct:.0%} threshold)")

        # Count positions per sector
        sector_counts: dict[str, int] = {}
        for p in self.positions:
            s = p.sector or "Unknown"
            sector_counts[s] = sector_counts.get(s, 0) + 1
        for sector, count in sector_counts.items():
            if count >= 3:
                warnings.append(f"{sector}: {count} positions — consider diversifying")

        return warnings

    def get_concentration_status(self) -> dict:
        """Check portfolio concentration against ASK targets."""
        self.update_prices()

        total_value = sum(p.current_value or 0 for p in self.positions)
        position_count = len(self.positions)
        position_room = MAX_POSITIONS - position_count

        # Check individual position weights
        overweight = []
        for p in self.positions:
            if total_value > 0 and p.current_value:
                weight = p.current_value / total_value
                if weight > MAX_SINGLE_POSITION_PCT:
                    overweight.append(
                        {
                            "symbol": p.symbol,
                            "weight": weight,
                            "max": MAX_SINGLE_POSITION_PCT,
                        }
                    )

        # Check staged entry completeness
        incomplete_entries = []
        for p in self.positions:
            if p.tranches_filled < p.tranches_planned:
                incomplete_entries.append(
                    {
                        "symbol": p.symbol,
                        "filled": p.tranches_filled,
                        "planned": p.tranches_planned,
                    }
                )

        return {
            "position_count": position_count,
            "max_positions": MAX_POSITIONS,
            "room_for_new": position_room,
            "overweight_positions": overweight,
            "incomplete_staged_entries": incomplete_entries,
        }

    def get_dividend_summary(self) -> dict:
        """Summarize dividend yield across portfolio."""
        self.update_prices()

        total_value = sum(p.current_value or 0 for p in self.positions)
        if total_value == 0:
            return {"weighted_yield": 0, "estimated_annual_dkk": 0, "positions": []}

        weighted_yield = 0.0
        position_dividends = []

        for p in self.positions:
            if p.dividend_yield and p.current_value:
                weight = p.current_value / total_value
                weighted_yield += p.dividend_yield * weight
                est_annual = p.current_value * p.dividend_yield
                position_dividends.append(
                    {
                        "symbol": p.symbol,
                        "yield": p.dividend_yield,
                        "est_annual_usd": est_annual,
                    }
                )

        return {
            "weighted_yield": weighted_yield,
            "estimated_annual_usd": total_value * weighted_yield,
            "ask_tax_rate": 0.17,
            "positions": position_dividends,
        }

    def get_contribution_status(self) -> ContributionStatus:
        """Track ASK contribution room for the current year."""
        year = datetime.now().year
        contributed = 0.0

        if self.contributions_file.exists():
            try:
                data = json.loads(self.contributions_file.read_text())
                if data.get("year") == year:
                    contributed = data.get("contributed_dkk", 0.0)
            except Exception:
                pass

        return ContributionStatus(
            year=year,
            limit_dkk=ASK_CONTRIBUTION_LIMIT_DKK,
            contributed_dkk=contributed,
            remaining_dkk=max(0, ASK_CONTRIBUTION_LIMIT_DKK - contributed),
        )

    def record_contribution(self, amount_dkk: float):
        """Record a contribution to the ASK."""
        status = self.get_contribution_status()
        new_total = status.contributed_dkk + amount_dkk

        data = {
            "year": status.year,
            "contributed_dkk": new_total,
            "limit_dkk": status.limit_dkk,
            "updated_at": datetime.now().isoformat(),
        }

        try:
            self.contributions_file.write_text(json.dumps(data, indent=2))
            logger.info(f"Recorded ASK contribution: {amount_dkk:,.0f} DKK (total: {new_total:,.0f})")
        except Exception as e:
            logger.warning(f"Failed to save contribution: {e}")

    def get_portfolio_gap_analysis(self, tier1_symbols: list[str], tier2_approaching: list[str]) -> dict:
        """
        Analyze what the portfolio is missing.

        Args:
            tier1_symbols: Current Tier 1 candidates
            tier2_approaching: Tier 2 stocks approaching target

        Returns dict with recommendations.
        """
        current_symbols = {p.symbol for p in self.positions}
        concentration = self.get_concentration_status()
        room = concentration["room_for_new"]

        # Tier 1 picks not yet in portfolio
        new_tier1 = [s for s in tier1_symbols if s not in current_symbols]

        # Approaching Tier 2 not in portfolio
        new_approaching = [s for s in tier2_approaching if s not in current_symbols]

        # Existing positions with incomplete staged entries
        incomplete = concentration["incomplete_staged_entries"]

        recommendations = []
        if room > 0 and new_tier1:
            recommendations.append(
                f"You have room for {room} more positions. Current Tier 1 candidates: {', '.join(new_tier1[:5])}"
            )
        elif room == 0 and new_tier1:
            recommendations.append(
                f"Portfolio full ({len(self.positions)}/{MAX_POSITIONS}). "
                f"Consider replacing weakest position for: {', '.join(new_tier1[:3])}"
            )
        if incomplete:
            for entry in incomplete:
                recommendations.append(
                    f"{entry['symbol']}: staged entry {entry['filled']}/{entry['planned']} — "
                    f"consider completing next tranche"
                )
        if new_approaching:
            recommendations.append(f"Watch for entry: {', '.join(new_approaching[:5])} approaching target price")

        return {
            "room_for_new": room,
            "new_tier1_candidates": new_tier1,
            "approaching_candidates": new_approaching,
            "incomplete_entries": incomplete,
            "recommendations": recommendations,
        }

    def get_performance_metrics(self) -> PerformanceMetrics:
        """Calculate overall portfolio performance"""
        self.update_prices()

        total_invested = sum(p.cost_basis * p.shares for p in self.positions)
        current_value = sum(p.current_value or 0 for p in self.positions)
        total_gain_loss = current_value - total_invested
        total_gain_loss_pct = total_gain_loss / total_invested if total_invested > 0 else 0

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

        by_symbol: dict[str, dict[str, list]] = {}
        for trade in self.trades:
            if trade.symbol not in by_symbol:
                by_symbol[trade.symbol] = {"buys": [], "sells": []}

            if trade.action == "BUY":
                by_symbol[trade.symbol]["buys"].append(trade)
            else:
                by_symbol[trade.symbol]["sells"].append(trade)

        for symbol, trades in by_symbol.items():
            if trades["sells"]:
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
        concentration = self.get_concentration_status()
        dividends = self.get_dividend_summary()
        contribution = self.get_contribution_status()

        return {
            "positions": [p.to_dict() for p in self.positions],
            "position_count": len(self.positions),
            "total_invested": metrics.total_invested,
            "current_value": metrics.current_value,
            "total_gain_loss": metrics.total_gain_loss,
            "total_gain_loss_pct": metrics.total_gain_loss_pct,
            "sector_exposure": exposure,
            "sector_warnings": warnings,
            "concentration": concentration,
            "dividends": dividends,
            "ask_contribution": {
                "year": contribution.year,
                "limit_dkk": contribution.limit_dkk,
                "contributed_dkk": contribution.contributed_dkk,
                "remaining_dkk": contribution.remaining_dkk,
            },
            "performance": {
                "total_trades": metrics.total_trades,
                "winning_trades": metrics.winning_trades,
                "losing_trades": metrics.losing_trades,
                "win_rate": metrics.win_rate,
            },
        }


def calculate_position_size(
    portfolio_value: float, conviction: str, current_positions: int, max_position_pct: float = 0.25
) -> dict:
    """
    Calculate recommended position size based on conviction and portfolio.

    v2.0 — ASK-aware sizing:
    - Tier 1 + HIGH conviction = 15-25% of portfolio
    - Tier 1 + MEDIUM conviction = 10-15%
    - Never >25% in a single position
    - Staged entry: initial tranche = 1/3 of target position

    Returns dollar amount and percentage.
    """

    # Base allocation by conviction (full position target)
    conviction_allocation = {
        "HIGH": 0.20,  # 20% target position
        "MEDIUM": 0.12,  # 12% target position
        "LOW": 0.08,  # 8% target position
    }

    base_pct = conviction_allocation.get(conviction, 0.12)

    # Adjust based on portfolio concentration
    if current_positions < 3:
        # Very few positions — can be more aggressive
        base_pct = min(base_pct * 1.3, max_position_pct)
    elif current_positions >= MAX_POSITIONS:
        # Portfolio full — smaller incremental sizing
        base_pct = base_pct * 0.6

    # Cap at maximum
    base_pct = min(base_pct, max_position_pct)

    # Full position dollar amount
    full_amount = portfolio_value * base_pct

    # First tranche = 1/3 of full position (staged entry)
    initial_tranche_pct = base_pct / 3
    initial_tranche_amount = full_amount / 3

    return {
        "conviction": conviction,
        "recommended_pct": base_pct,
        "recommended_amount": full_amount,
        "initial_tranche_pct": initial_tranche_pct,
        "initial_tranche_amount": initial_tranche_amount,
        "max_pct": max_position_pct,
        "max_amount": portfolio_value * max_position_pct,
        "reasoning": (
            f"{conviction} conviction = {base_pct:.0%} target position (${full_amount:,.0f}). "
            f"Initial tranche: {initial_tranche_pct:.0%} (${initial_tranche_amount:,.0f})"
        ),
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
        tier=1,
        tranche=1,
    )

    tracker.add_position(
        symbol="MSFT",
        shares=5,
        price=380.00,
        thesis="Cloud dominance",
        thesis_breaking_events=["Azure growth below 20%"],
        conviction="MEDIUM",
        tier=1,
        tranche=1,
    )

    summary = tracker.get_portfolio_summary()
    print(json.dumps(summary, indent=2, default=str))

    # Test ASK position sizing
    sizing = calculate_position_size(portfolio_value=50000, conviction="HIGH", current_positions=2)
    print(f"\nPosition sizing: {json.dumps(sizing, indent=2)}")

    # Test concentration
    concentration = tracker.get_concentration_status()
    print(f"\nConcentration: {json.dumps(concentration, indent=2)}")

    # Test gap analysis
    gaps = tracker.get_portfolio_gap_analysis(
        tier1_symbols=["V", "COST", "AAPL"],
        tier2_approaching=["GOOGL", "UNH"],
    )
    print(f"\nGap analysis: {json.dumps(gaps, indent=2)}")
