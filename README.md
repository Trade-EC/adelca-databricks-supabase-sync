# Databricks Connector to Supabase

Syncs `transportistas_final` from Databricks SQL to Supabase via REST API.
Can run locally or as an AWS Lambda on an hourly schedule.

## Project structure

```
├── connector.py                    # Diagnostic: read Databricks table
├── full_sync_to_supabase.py        # Local sync script (Databricks → Supabase REST)
├── test_supabase_connection.py     # Test Supabase connectivity
├── transportistas_sync/
│   ├── .env                        # Credentials (not committed)
│   ├── .env.example                # Template
│   └── databricks_connector.py     # DatabricksConnector class
├── lambda_function/
│   ├── handler.py                  # AWS Lambda handler
│   └── requirements.txt            # Lambda dependencies
├── template.yaml                   # SAM template (Lambda + EventBridge)
├── samconfig.toml                  # SAM deploy config
├── deploy.sh                       # Build & deploy script
├── supabase_create_tables.sql      # SQL to create Supabase tables
└── requirements.txt                # Local dev dependencies
```

## Local setup

### 1. Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Credentials

```bash
cp transportistas_sync/.env.example transportistas_sync/.env
```

Edit `transportistas_sync/.env`:

- **DATABRICKS_HOST** — SQL warehouse hostname (e.g. `dbc-xxx.cloud.databricks.com`)
- **DATABRICKS_HTTP_PATH** — HTTP path (e.g. `/sql/1.0/warehouses/xxx`)
- **DATABRICKS_TOKEN** — Personal access token
- **SUPABASE_URL** — Project URL (e.g. `https://xxx.supabase.co`)
- **SUPABASE_SERVICE_ROLE_KEY** — service_role key from Project Settings → API

### 3. Run locally

```bash
# Test Databricks connection
python connector.py

# Test Supabase connection
python test_supabase_connection.py

# Run sync
python full_sync_to_supabase.py
```

## AWS Lambda deployment (SAM)

### Prerequisites

- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) configured (`aws configure`)
- [SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) installed
- Docker running (for `sam build --use-container`)

### Deploy

```bash
# First time (interactive — sets parameters)
./deploy.sh --guided

# Subsequent deploys
./deploy.sh
```

During guided deploy, you'll be prompted for:

| Parameter | Value |
|-----------|-------|
| DatabricksHost | `dbc-xxx.cloud.databricks.com` |
| DatabricksHttpPath | `/sql/1.0/warehouses/xxx` |
| DatabricksToken | Your PAT (hidden) |
| SupabaseUrl | `https://xxx.supabase.co` |
| SupabaseServiceRoleKey | Your service_role key (hidden) |
| DbxTable | `qas.aplicaciones.transportistas_final` |
| ScheduleExpression | `rate(1 hour)` |

### Monitor

```bash
# Tail logs
sam logs -n databricks-supabase-sync --tail

# Invoke manually
aws lambda invoke --function-name databricks-supabase-sync /dev/stdout

# Check last sync in Supabase
# SELECT * FROM public.etl_watermarks;
```

### How it works

1. **EventBridge** triggers the Lambda every hour
2. Lambda connects to **Databricks SQL** and fetches the full `transportistas_final` table
3. Lambda **upserts** all rows to **Supabase** via REST API (`ON CONFLICT codigo_transportista`)
4. Each row gets `_ingested_at` stamped with the current UTC time
5. `etl_watermarks.last_timestamp` is updated with the sync time
6. Existing rows are updated, new rows are inserted, nothing is deleted

### Costs

- **Lambda**: ~16s per run × 24 runs/day × 256MB = well within free tier
- **Databricks**: SQL warehouse compute time for a small SELECT
- **Supabase**: REST API calls (free tier covers this easily)
