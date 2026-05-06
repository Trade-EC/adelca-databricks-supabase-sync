# Databricks → Supabase Sync (Node.js)

Repositorio refactorizado a Node.js para sincronizar transportistas desde Databricks hacia Supabase.

## Stack actual

- **Lambda AWS**: Node.js 20 (`lambda_function/handler.js`)
- **Infra**: SAM (`template.yaml`)
- **Scripts locales**: Node (`src/scripts`)
- **Dashboard local**: Node/Express (`src/dashboard/server.js`)

## Flujo ETL

1. Lee tabla origen `prod.gldlogistica.db_trade_dim_app_transportistas` desde Databricks (OAuth M2M).
2. Deduplica por `codigo_transportista`.
3. Inserta solo nuevos en Supabase `transportistas`.
4. Ignora duplicados de PK de forma segura.
5. Actualiza `etl_watermarks`.
6. Guarda log estructurado JSON en S3 (`patek-philippe-etl-logs-052124708820`).

## Comandos locales

```bash
npm install
npm run test:connection
npm run sync:incremental
npm run sync:historical
npm run dashboard
```

Dashboard local: `http://localhost:5050`

## Deploy Lambda

```bash
sam build
sam deploy --no-confirm-changeset
```

## Variables de entorno (archivo local)

Se leen desde `transportistas_sync/.env`:

- `DATABRICKS_PRD_HOST`
- `DATABRICKS_PRD_HTTP_PATH`
- `DATABRICKS_PRD_CLIENT_ID`
- `DATABRICKS_PRD_CLIENT_SECRET`

En la Lambda (`handler.js`) también se aceptan los nombres que usa el template SAM: `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` (misma semántica PRD).
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- opcionales: `DBX_TABLE`, `SUPABASE_TABLE`, `FETCH_SIZE`, `LAMBDA_NAME`

## Arquitectura Escalable (multi-pipeline)

- Configuración por pipeline en `lambda_function/pipelines.json`.
- La Lambda recibe `pipeline_name` y ejecuta la estrategia declarada:
  - `insert_only`
  - `upsert`
- Watermark por pipeline en `etl_watermarks.table_name = pipeline_name` (opcional: ver `SKIP_ETL_WATERMARK`).
- Auditoría por corrida (requiere `ETL_LOGS_BUCKET` en la Lambda, bucket ya definido en `template.yaml`):
  - Log detallado: `s3://{bucket}/{pipeline_name}/{yyyy-mm-dd}/{timestamp}.json`
  - **Checkpoint de última corrida OK:** `s3://{bucket}/{ETL_SUCCESS_PREFIX}/{pipeline_name}/latest.json` (se sobrescribe en cada éxito; incluye métricas y `full_log_uri` al log detallado)
- Supabase: tabla `etl_runs` (si existe; se ignora si no está creada).
- Variables: `SKIP_ETL_WATERMARK` (`true` / `1` = no lee ni escribe `etl_watermarks`); `ETL_SUCCESS_PREFIX` (prefijo del checkpoint, default `etl-success`).

Para un registro operacional **sin** Supabase, basta con S3 + `SKIP_ETL_WATERMARK=true`. **DynamoDB** no está en el código: sería un `PutItem` tras el éxito y política IAM; S3 suele bastar para “última corrida OK” y trazabilidad.

### Ejemplo de invocación

```json
{
  "pipeline_name": "transportistas"
}
```

Ferreterías / `hardware_stores` (secondary):

```json
{
  "pipeline_name": "socio_adelca_ferreterias"
}
```

Referencia de payloads por pipeline: `pipeline_registry.json`.
