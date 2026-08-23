# Week 2 setup notes

These files go into your existing `smart-retail-data-platform/` repo like this:

```
smart-retail-data-platform/
├── docker/
│   ├── docker-compose.yml          <- copy in
│   └── postgres-init/
│       └── 01-init-databases.sql   <- copy in
├── scripts/
│   ├── seed_mock_sources.py        <- copy in (overwrite the placeholder folder)
│   └── requirements.txt            <- copy in
├── data/
│   └── sample/                     <- generated output, gitignored (see below)
└── .env.example                    <- copy in (root of repo)
```

## First-time setup

1. Copy the files above into your repo, matching the paths.
2. Add `data/sample/` to `.gitignore` — generated data doesn't belong in git,
   only the generator script does:
   ```
   Add-Content -Path .gitignore -Value "data/sample/"
   ```
3. Copy `.env.example` to `.env` and fill in real local values:
   ```
   Copy-Item .env.example .env
   ```
4. Generate a real Airflow Fernet key and paste it into `.env`:
   ```
   pip install cryptography
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```
5. Install the generator's dependency and run it:
   ```
   pip install -r scripts/requirements.txt
   python scripts/seed_mock_sources.py --seed 42 --out-dir data/sample
   ```
   You should see six files land in `data/sample/`: `products.csv`,
   `customers.csv`, `pos_sales.csv`, `inventory_snapshots.csv`,
   `ecommerce_orders.json`, `supplier_deliveries.json`.
6. Start the stack from the `docker/` folder (Docker Desktop must be running):
   ```
   cd docker
   docker compose --env-file ../.env up -d
   ```
7. Check everything is healthy:
   - Postgres: `docker exec -it retaillake-postgres psql -U retaillake -d retaildb -c "\dn"`
     should list `bronze`, `silver`, `gold`, `quality` schemas.
   - MinIO console: http://localhost:9001 (login with your `.env` MinIO
     credentials) — should show `retail-raw` and `retail-quarantine` buckets
     already created.
   - Airflow: http://localhost:8080 (login with your `.env` Airflow admin
     credentials) — should load with no DAGs yet (that's Week 3).

## What each piece proves for the Week 2 checkpoint

| Brief requirement | Where it lives |
|---|---|
| Docker Compose for Airflow, Postgres, MinIO | `docker/docker-compose.yml` |
| `.env.example` without secrets | `.env.example` |
| Mock data generator | `scripts/seed_mock_sources.py` |
| Sample files for all sources | `data/sample/*.csv`, `*.json` (generated, not committed) |
| Storage buckets | created automatically by the `minio-init` service |
| Database schemas | created automatically by `docker/postgres-init/01-init-databases.sql` |

## Evidence to capture for the mentor sync

- Screenshot: `docker compose ps` showing all containers healthy/running
- Screenshot: MinIO console with the two buckets visible
- Screenshot: terminal output of the seed script run (row counts)
- Screenshot: `psql \dn` output showing the four schemas
- A short note in your weekly sync doc: env variables explained, one line
  each (why POSTGRES_*, MINIO_*, AIRFLOW_FERNET_KEY exist)

## Known limitation to mention in your report later

The Fernet key and all `.env` values in this setup are for local development
only. In `.env.example` they're placeholders — real values only ever live in
your local `.env`, which is gitignored.
