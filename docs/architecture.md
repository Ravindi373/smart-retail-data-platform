# Architecture

## Flow

Sources -> Raw (MinIO) -> Bronze (typed) -> Silver (clean) -> Gold (star schema) -> Superset dashboard

Bad rows are pulled aside into a quarantine table during the Silver step
instead of being dropped silently.

## Layer rules

| Layer | What it stores | Main rule |
|---|---|---|
| Raw | Exact source files, unmodified | Never edit or overwrite. Date-partitioned: `raw/source/YYYY/MM/DD` |
| Bronze | Typed source tables | Add ingestion metadata (source, row count, run time) |
| Silver | Clean, trusted data | Deduplicate, standardise timestamps/text, hash PII |
| Gold | Facts and dimensions | Optimised for dashboard queries only |

**Hard rule:** no dashboard chart reads from Raw or Bronze. Everything
customer-facing comes from Gold.

## Why this shape

- **Raw preserves the original.** If a cleaning rule turns out to be wrong
  in week 6, we can reprocess from Raw instead of having destroyed the
  source data.
- **Bronze separates "loaded" from "trustworthy."** Typing and metadata
  happen here, but no business logic yet — this keeps ingestion code and
  cleaning code from tangling together.
- **Silver is the only place cleaning logic lives.** One place to look
  when a business user asks "why does this record look different from the
  source file."
- **Gold is deliberately narrow.** Only the tables a dashboard needs, at
  the grain a dashboard needs, so query logic in Superset stays simple.

## Orchestration

Apache Airflow DAGs (via Astro CLI) land raw files on a schedule and
trigger downstream Bronze parsing. Each DAG run is idempotent — running
the same date twice must not duplicate data.

See `docs/erd.md` for the Gold-layer star schema this architecture feeds.
