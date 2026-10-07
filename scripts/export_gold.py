"""
scripts/export_gold.py

Snapshot the dashboard-facing Gold tables to Parquet so the public Streamlit
dashboard (dashboard/streamlit_app.py) can run without Postgres.

The dashboard reads *only* Gold, exactly like Superset does; this script is the
one-way bridge from the warehouse to the hosted copy. Re-run it after
`make pipeline` and commit dashboard/data/ to refresh the public dashboard.

    make export-gold            # uses .env, Postgres on localhost:25432
    python scripts/export_gold.py --host localhost --port 25432
"""

import argparse
import datetime
import decimal
import os
from pathlib import Path

import pandas as pd
import psycopg2

GOLD_TABLES = ["sales_flat", "inventory_flat", "quality_summary", "quality_scorecard"]
OUT_DIR = Path(__file__).resolve().parent.parent / "dashboard" / "data"


def load_env(path=".env"):
    """Minimal .env reader so the script needs no extra dependency."""
    if not os.path.exists(path):
        return
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def main():
    load_env()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default=os.environ.get("POSTGRES_HOST", "localhost"))
    # 25432 is the host port docker-compose maps to Postgres
    p.add_argument("--port", type=int, default=int(os.environ.get("POSTGRES_PORT", "25432")))
    p.add_argument("--db", default=os.environ.get("POSTGRES_DB", "retaildb"))
    p.add_argument("--out-dir", default=str(OUT_DIR))
    args = p.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.db,
                            user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"])
    try:
        for table in GOLD_TABLES:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM gold.{table}")
                df = pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
            for col in df.columns:
                sample = df[col].dropna()
                if sample.empty:
                    continue
                first = sample.iloc[0]
                if isinstance(first, decimal.Decimal):  # Postgres numeric -> float
                    df[col] = df[col].astype(float)
                elif isinstance(first, datetime.date):  # date / timestamp -> datetime64
                    df[col] = pd.to_datetime(df[col])
            df.to_parquet(out / f"{table}.parquet", index=False)
            print(f"gold.{table:<18} {len(df):>6} rows -> {out / (table + '.parquet')}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
