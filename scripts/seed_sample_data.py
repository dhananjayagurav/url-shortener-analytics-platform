"""Seed the local Postgres with reproducible synthetic data, so the
ingestion pipeline has something real to extract on a fresh `docker compose
up -d`.

Not part of the `url_shortener_analytics` package: this is a one-off
operator script, not something the pipeline imports or depends on at
runtime -- see docs/analytics-engineering-guide.md, "Project Structure",
for why `scripts/` and `ingestion/src/` are kept separate.

Run:
    python scripts/seed_sample_data.py
"""

from __future__ import annotations

import random
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from faker import Faker
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion" / "src"))

from url_shortener_analytics.config import get_settings  # noqa: E402
from url_shortener_analytics.db import engine_from_settings  # noqa: E402

SEED = 42
N_URLS = 500
N_USERS = 200
N_CLICKS = 5000


def main() -> None:
    fake = Faker()
    Faker.seed(SEED)
    random.seed(SEED)

    settings = get_settings()
    engine = engine_from_settings(settings)

    with engine.begin() as conn:
        print(f"Seeding {N_URLS} urls, {N_USERS} users, {N_CLICKS} clicks ...")

        url_ids: list[int] = []
        for _ in range(N_URLS):
            row = conn.execute(
                text("INSERT INTO urls (short_code, original_url) VALUES (:code, :url) RETURNING id"),
                {"code": fake.unique.lexify(text="??????"), "url": fake.uri()},
            ).fetchone()
            url_ids.append(row[0])

        for _ in range(N_USERS):
            conn.execute(
                text("INSERT INTO users (email, plan_type) VALUES (:email, :plan)"),
                {"email": fake.unique.email(), "plan": random.choices(["FREE", "PREMIUM"], weights=[85, 15])[0]},
            )
        user_ids = [r[0] for r in conn.execute(text("SELECT id FROM users")).fetchall()]

        short_codes = [r[0] for r in conn.execute(text("SELECT short_code FROM urls")).fetchall()]
        now = datetime.now(UTC)
        for _ in range(N_CLICKS):
            occurred_at = now - timedelta(
                days=random.randint(0, 9), hours=random.randint(0, 23), minutes=random.randint(0, 59)
            )
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

    print("Done. Row counts:")
    with engine.begin() as conn:
        for table in ("urls", "users", "clicks"):
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"  {table:10s} {count}")


if __name__ == "__main__":
    main()
