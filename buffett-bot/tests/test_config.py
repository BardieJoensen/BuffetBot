"""Configuration-loading regressions for direct ``python -m`` execution."""

import os
import subprocess
import sys
from pathlib import Path


def test_dotenv_is_loaded_before_config_defaults(tmp_path):
    (tmp_path / ".env").write_text(
        "AUTO_TRADE_ENABLED=false\nMONTHLY_BRIEFING_ENABLED=false\nWEDNESDAY_HAIKU_ENABLED=true\nMAX_POSITIONS=13\n"  # pragma: allowlist secret
        "DATABASE_PATH=./custom/state.db\n"
    )
    project_root = Path(__file__).parents[1]
    env = os.environ.copy()
    env.pop("AUTO_TRADE_ENABLED", None)
    env.pop("MONTHLY_BRIEFING_ENABLED", None)
    env.pop("WEDNESDAY_HAIKU_ENABLED", None)
    env.pop("MAX_POSITIONS", None)
    env.pop("DATABASE_PATH", None)
    env["PYTHONPATH"] = str(project_root)

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from src.config import config; "
                "print(config.auto_trade_enabled, config.monthly_briefing_enabled, "
                "config.wednesday_haiku_enabled, config.briefing_paper_trades_enabled, "
                "config.max_positions, config.database_path)"
            ),
        ],
        cwd=tmp_path,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    assert proc.stdout.strip() == "False False True False 13 custom/state.db"
