"""Insert a second, reproducible batch of `clicks` rows, simulating real
activity that happened *after* `seed_sample_data.py`'s original batch and
after Section 38's Bronze full-load snapshot was taken.

Why this script exists: Section 38 built and ran Bronze `clicks` -> Silver
`clicks` against a single full-load snapshot. Section 39 needs a genuine
second, *incremental* Bronze batch to prove Silver can read a full-load
object and an incremental object together -- and that needs new rows to
actually exist in the source `clicks` table first. This script is that
"time passed, new clicks happened" step, generated the same reproducible
way `seed_sample_data.py` generates the original batch (same Faker/random
conventions, same table, same columns) -- just a second call, with its own
documented seed, appending rather than replacing.

Not part of the `url_shortener_analytics` package -- same one-off
operator-script convention as `seed_sample_data.py` and
`write_local_bronze_clicks.py`.

Run:
    python scripts/seed_more_clicks.py
"""

from __future__ import annotations

import random
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from faker import Faker
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion" / "src"))

from url_shortener_analytics.config import get_settings
from url_shortener_analytics.db import engine_from_settings

# One more than seed_sample_data.py's SEED=42 -- a distinct, still-fixed
# seed for this second, later batch, not a reuse of the original one
# (reusing SEED=42 here would just regenerate the exact same values against
# a random-module state that's no longer at its start, which is not
# actually reproducible in a way worth relying on).
SEED = 43
N_NEW_CLICKS = 200


def main() -> None:
    fake = Faker()
    Faker.seed(SEED)
    random.seed(SEED)

    settings = get_settings()
    engine = engine_from_settings(settings)

    with engine.begin() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM clicks")).scalar()
        max_id_before = conn.execute(text("SELECT COALESCE(MAX(id), 0) FROM clicks")).scalar()
        print(f"clicks before: {before} rows, max id {max_id_before}")

        user_ids = [r[0] for r in conn.execute(text("SELECT id FROM users")).fetchall()]
        short_codes = [r[0] for r in conn.execute(text("SELECT short_code FROM urls")).fetchall()]

        # Recent activity -- the last few hours, not the 0-9 day spread
        # seed_sample_data.py used for the original batch. This is what
        # makes this batch read as "what happened since the last
        # extraction" rather than a second copy of the same historical
        # window.
        now = datetime.now(UTC)
        print(f"inserting {N_NEW_CLICKS} new clicks ...")
        for _ in range(N_NEW_CLICKS):
            occurred_at = now - timedelta(minutes=random.randint(0, 180))
            conn.execute(
                text(
                    """
                    INSERT INTO clicks (short_code, occurred_at, device_type, hashed_ip, user_id)
                    VALUES (:short_code, :occurred_at, :device_type, :hashed_ip, :user_id)
                    """
                ),
                {
                    "short_code": random.choice(short_codes),
                    "occurred_at": occurred_at,
                    "device_type": random.choices(
                        ["mobile", "desktop", "tablet"], weights=[60, 35, 5]
                    )[0],
                    "hashed_ip": fake.sha256(),
                    "user_id": random.choice(user_ids) if random.random() < 0.3 else None,
                },
            )

        after = conn.execute(text("SELECT COUNT(*) FROM clicks")).scalar()
        max_id_after = conn.execute(text("SELECT COALESCE(MAX(id), 0) FROM clicks")).scalar()
        print(f"clicks after: {after} rows, max id {max_id_after}")
        print(f"new rows: {after - before} (ids {max_id_before + 1}..{max_id_after})")


if __name__ == "__main__":
    main()
