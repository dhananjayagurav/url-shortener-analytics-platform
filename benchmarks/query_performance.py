"""Query performance at scale: run this project's own Section 7.1 metrics
catalog directly against a real, temporarily-populated `fact_clicks`
table, at several row-count scales, and time each query for real.

See docs/analytics-engineering-guide.md, Section 26 ("Performance"). This
is the benchmark Section 7's own Design Decision named as the trigger for
revisiting "no pre-aggregation": Alternative 2 there says outright,
"revisit once Section 26's benchmarks show fact_clicks queries are
actually slow." This script, and the numbers it prints, are what closes
that.

`fact_clicks` is schema-only in this project as of Section 26 -- Phase
2's real transform doesn't exist yet (Section 11). This script is NOT
that transform, and does not pretend to be. It inserts clearly-marked
SYNTHETIC rows (click_id >= SYNTHETIC_CLICK_ID_FLOOR, url_key/user_key >=
their own floors) purely to get real query-timing numbers, then DELETES
every row it inserted before exiting, by default, leaving `fact_clicks`,
`dim_url`, and `dim_user` exactly as this script found them. Pass
--keep-data to skip cleanup and inspect the populated tables yourself.

Needs only real Postgres (this project's `sql/analytics/*.sql` schema
already applied via `make create-analytics-schema`) -- no MinIO required.

Run:
    PYTHONPATH=ingestion/src python3 benchmarks/query_performance.py
    PYTHONPATH=ingestion/src python3 benchmarks/query_performance.py --scales 5000,50000,500000
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion" / "src"))

from sqlalchemy import Engine, text  # noqa: E402

from url_shortener_analytics.config import get_settings  # noqa: E402
from url_shortener_analytics.db import engine_from_settings  # noqa: E402

# Well clear of any real or Section-11-verification row this project has
# ever committed (dim_url/dim_user/fact_clicks each have at most a
# handful of low-numbered rows from prior sections' own verification).
SYNTHETIC_URL_KEY_FLOOR = 900_000
SYNTHETIC_USER_KEY_FLOOR = 900_000
SYNTHETIC_CLICK_ID_FLOOR = 900_000_000

DOMAINS = ["example.com", "github.com", "news.example", "shop.example", "blog.example"]

# Every metric in Section 7.1's catalog, as the real SQL a Phase-2-populated
# fact_clicks would actually be queried with.
QUERIES: dict[str, str] = {
    "total_clicks_per_url": """
        SELECT du.short_code, COUNT(*) AS total_clicks
        FROM fact_clicks fc JOIN dim_url du ON fc.url_key = du.url_key
        WHERE fc.click_id >= 900000000
        GROUP BY du.short_code
    """,
    "clicks_over_time": """
        SELECT dd.full_date, COUNT(*) AS clicks
        FROM fact_clicks fc JOIN dim_date dd ON fc.date_key = dd.date_key
        WHERE fc.click_id >= 900000000
        GROUP BY dd.full_date
        ORDER BY dd.full_date
    """,
    "clicks_by_device_type": """
        SELECT ddev.device_type, COUNT(*) AS clicks
        FROM fact_clicks fc JOIN dim_device ddev ON fc.device_key = ddev.device_key
        WHERE fc.click_id >= 900000000
        GROUP BY ddev.device_type
    """,
    "top_10_urls_by_clicks": """
        SELECT du.short_code, COUNT(*) AS total_clicks
        FROM fact_clicks fc JOIN dim_url du ON fc.url_key = du.url_key
        WHERE fc.click_id >= 900000000
        GROUP BY du.short_code
        ORDER BY total_clicks DESC
        LIMIT 10
    """,
    "anonymous_vs_attributed_share": """
        SELECT dus.is_known, COUNT(*) AS clicks
        FROM fact_clicks fc JOIN dim_user dus ON fc.user_key = dus.user_key
        WHERE fc.click_id >= 900000000
        GROUP BY dus.is_known
    """,
    "clicks_by_plan_type": """
        SELECT dus.plan_type, COUNT(*) AS clicks
        FROM fact_clicks fc JOIN dim_user dus ON fc.user_key = dus.user_key
        WHERE fc.click_id >= 900000000 AND dus.is_known = true
        GROUP BY dus.plan_type
    """,
    "clicks_by_destination_domain": """
        SELECT du.original_url_domain, COUNT(*) AS clicks
        FROM fact_clicks fc JOIN dim_url du ON fc.url_key = du.url_key
        WHERE fc.click_id >= 900000000
        GROUP BY du.original_url_domain
    """,
    "active_vs_inactive_url_share": """
        SELECT du.is_active, COUNT(*) AS clicks
        FROM fact_clicks fc JOIN dim_url du ON fc.url_key = du.url_key
        WHERE fc.click_id >= 900000000
        GROUP BY du.is_active
    """,
}


def _real_date_keys(engine: Engine) -> list[int]:
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT date_key FROM dim_date WHERE year = 2026")).fetchall()
    return [r.date_key for r in rows]


def cleanup(engine: Engine) -> None:
    """Delete every row this script could ever have inserted. Called both
    before (in case a prior run crashed and left rows behind -- Section
    22's own lesson about leftover rows from failed verification runs)
    and after a normal run."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_clicks WHERE click_id >= :f"), {"f": SYNTHETIC_CLICK_ID_FLOOR})
        conn.execute(text("DELETE FROM dim_url WHERE url_key >= :f"), {"f": SYNTHETIC_URL_KEY_FLOOR})
        conn.execute(text("DELETE FROM dim_user WHERE user_key >= :f"), {"f": SYNTHETIC_USER_KEY_FLOOR})


def _generate_synthetic_dims(engine: Engine, n_urls: int, n_users: int, date_keys: list[int]) -> None:
    url_rows = [
        {
            "url_key": SYNTHETIC_URL_KEY_FLOOR + i,
            "url_id": SYNTHETIC_URL_KEY_FLOOR + i,
            "short_code": f"bench{i:06d}",
            "original_url_domain": random.choice(DOMAINS),
            "is_active": random.random() > 0.1,
            "created_date_key": random.choice(date_keys),
        }
        for i in range(n_urls)
    ]
    user_rows = [
        {
            "user_key": SYNTHETIC_USER_KEY_FLOOR + i,
            "user_id": SYNTHETIC_USER_KEY_FLOOR + i,
            "plan_type": random.choice(["FREE", "PREMIUM"]),
            "is_known": True,
        }
        for i in range(n_users)
    ]
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dim_url (url_key, url_id, short_code, original_url_domain, is_active, created_date_key) "
                "VALUES (:url_key, :url_id, :short_code, :original_url_domain, :is_active, :created_date_key)"
            ),
            url_rows,
        )
        conn.execute(
            text(
                "INSERT INTO dim_user (user_key, user_id, plan_type, is_known) "
                "VALUES (:user_key, :user_id, :plan_type, :is_known)"
            ),
            user_rows,
        )


def _generate_synthetic_fact_clicks(
    engine: Engine, n_rows: int, n_urls: int, n_users: int, date_keys: list[int]
) -> None:
    real_device_keys = [1, 2, 3, 4]  # dim_device's real, already-seeded rows (Section 10)
    batch = []
    for i in range(n_rows):
        url_offset = random.randrange(n_urls)
        # ~20% anonymous, resolving to dim_user's real Unknown member
        # (user_key = -1) -- Section 10.3's convention, not a NULL FK.
        user_key = -1 if random.random() < 0.2 else SYNTHETIC_USER_KEY_FLOOR + random.randrange(n_users)
        batch.append(
            {
                "click_id": SYNTHETIC_CLICK_ID_FLOOR + i,
                "date_key": random.choice(date_keys),
                "url_key": SYNTHETIC_URL_KEY_FLOOR + url_offset,
                "user_key": user_key,
                "device_key": random.choice(real_device_keys),
                "short_code": f"bench{url_offset:06d}",
            }
        )
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO fact_clicks (click_id, date_key, url_key, user_key, device_key, short_code, occurred_at, click_count) "
                "VALUES (:click_id, :date_key, :url_key, :user_key, :device_key, :short_code, now(), 1)"
            ),
            batch,
        )


def run_queries(engine: Engine, n_repeats: int = 3) -> dict[str, float]:
    """Best-of-`n_repeats` wall-clock seconds per query -- the standard
    way to reduce noise from anything else this shared sandbox is doing
    at the same moment, same reasoning Section 19.6 already used for its
    own timing numbers."""
    timings: dict[str, float] = {}
    with engine.connect() as conn:
        for name, sql in QUERIES.items():
            samples = []
            for _ in range(n_repeats):
                start = time.perf_counter()
                conn.execute(text(sql)).fetchall()
                samples.append(time.perf_counter() - start)
            timings[name] = min(samples)
    return timings


def print_scale_results(scale: int, timings: dict[str, float]) -> None:
    print(f"\n--- {scale:>7,} synthetic fact_clicks rows ---")
    for name, seconds in timings.items():
        print(f"  {name:<32} {seconds * 1000:>9.2f} ms")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scales", type=str, default="5000,50000,500000", help="Comma-separated row counts to test")
    parser.add_argument("--n-urls", type=int, default=500, help="Distinct synthetic URLs (default matches this project's real seeded urls count)")
    parser.add_argument("--n-users", type=int, default=200, help="Distinct synthetic users (default matches this project's real seeded users count)")
    parser.add_argument("--keep-data", action="store_true", help="Skip cleanup; leave synthetic rows in place for manual inspection")
    args = parser.parse_args()
    scales = [int(s) for s in args.scales.split(",")]

    settings = get_settings()
    engine = engine_from_settings(settings)

    date_keys = _real_date_keys(engine)
    print(f"Using {len(date_keys)} real dim_date rows (year 2026) as the date range.")

    cleanup(engine)  # in case a prior failed run left rows behind
    _generate_synthetic_dims(engine, args.n_urls, args.n_users, date_keys)
    print(f"Generated {args.n_urls} synthetic dim_url rows and {args.n_users} synthetic dim_user rows.")

    all_results: list[dict[str, object]] = []
    for scale in scales:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM fact_clicks WHERE click_id >= :f"), {"f": SYNTHETIC_CLICK_ID_FLOOR})
        gen_start = time.perf_counter()
        _generate_synthetic_fact_clicks(engine, scale, args.n_urls, args.n_users, date_keys)
        gen_s = time.perf_counter() - gen_start
        print(f"\nPopulated {scale:,} synthetic fact_clicks rows in {gen_s:.2f}s (this insert itself is NOT part of the timed result).")

        timings = run_queries(engine)
        print_scale_results(scale, timings)
        for name, seconds in timings.items():
            all_results.append({"scale": scale, "query": name, "ms": round(seconds * 1000, 2)})

    if args.keep_data:
        print(f"\n--keep-data set: {scales[-1]:,} synthetic rows left in fact_clicks/dim_url/dim_user for manual inspection.")
    else:
        cleanup(engine)
        print("\nCleaned up -- fact_clicks/dim_url/dim_user restored to their pre-benchmark state.")

    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)
    out_path = results_dir / "query_performance.json"
    out_path.write_text(json.dumps(all_results, indent=2))
    print(f"\n(results also written to {out_path}, gitignored, for the reader's own reference)")


if __name__ == "__main__":
    main()
