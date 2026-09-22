"""Shared test setup: make src/ and dags/ importable the same way the
Airflow containers see them (/opt/airflow/src)."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("RETAIL_SRC_DIR", str(ROOT / "src"))
os.environ.setdefault("RETAIL_SAMPLE_DIR", str(ROOT / "data" / "sample"))
for p in (ROOT / "src", ROOT / "dags"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
