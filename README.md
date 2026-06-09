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
3. Inserta solo nuevos en Supabase **secondary** `fleet_owners` (pipeline `transportistas`; antes `public.transportistas` en default).
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
- segundo proyecto (pipelines secondary): `SUPABASE_SECONDARY_URL`, `SUPABASE_SECONDARY_SERVICE_ROLE_KEY`
- tercer proyecto (pipelines tertiary): `SUPABASE_TERTIARY_URL`, `SUPABASE_TERTIARY_SERVICE_ROLE_KEY`
- **Base Socio** (pipelines `base_socio`): `SUPABASE_BASE_SOCIO_URL`, `SUPABASE_BASE_SOCIO_SERVICE_ROLE_KEY`
- usar la **secret key** (`sb_secret_…`) en servidor; la publishable (`sb_publishable_…`) no va en Lambda/dashboard server-side
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

### Dominio **transportistas** (Supabase secondary)

Cuatro tablas y cuatro pipelines forman el **proyecto de líneas transportistas** en el proyecto secundario (`demspbhtqlxnhmxjrhts`). No incluye `drivers_hist` (legacy) ni tablas `sa_*` (dominio socio Adelca).

| Pipeline | Tabla secondary | Origen | Modo | Enlace lógico |
|----------|-----------------|--------|------|----------------|
| `transportistas` | `fleet_owners` | Databricks PRD `db_trade_dim_app_transportistas` | insert_only | Maestro de transportista (`unique_code` = código transportista) |
| `vehiculos` | `vehicles` (Supabase **default** `rqlzsdziohmqanalmgyx`) | Databricks PRD `prod.gldlogistica.db_trade_dim_vehiculos` | insert_only + baseline S3 | Solo placas **nuevas** en PRD post corte 2026-06-09; destino espejo ~367+ filas; no usar `audit_created_at`/`synced_at` |
| `viajes` | `ct_freights` (Supabase **default**) | Databricks PRD `prod.gldlogistica.db_trade_fact_transportistas_viajes` | insert_only + baseline S3 | Carga histórica 2026-06-09; luego solo `codigo_viaje` nuevos en PRD (`bootstrapViajesBaseline.js --upload`) |
| `conductores` | `drivers` | Supabase **terciario** `api.vw_maestro_persona_conductor` | upsert | `id` = `person_id` ZK (sin uuid5); placas acumuladas |

Claves de negocio compartidas: `fleet_owner_unique_code` / `unique_code` (transportista), `license_plate` / placa (vehículo y viaje). Los conductores en `drivers` se alimentan del datamart ZK (terciario), no del lake de transportistas.

Cada pipeline declara `"domain": "transportistas"` en `pipelines.json`. Catálogo: `pipeline_registry.json` → `domains.transportistas`.

### Ejemplo de invocación

```json
{
  "pipeline_name": "transportistas"
}
```

Ferreterías / `sa_hardware_stores` (**Base Socio**):

```json
{
  "pipeline_name": "socio_adelca_ferreterias"
}
```

Referencia de payloads por pipeline: `pipeline_registry.json`.

Grupos ferreteros → `sa_hardware_store_groups` (**Base Socio**):

```json
{
  "pipeline_name": "socio_adelca_grupos"
}
```

Materiales → `sa_materials` (**Base Socio**):

```json
{
  "pipeline_name": "socio_adelca_materiales"
}
```

Facturas rebate → `sa_invoices` (**Base Socio**):

```json
{
  "pipeline_name": "socio_adelca_facturas_rebate"
}
```

JSONB en `sa_invoices` (ver `scripts/sql/supabase_sa_invoices_jsonb_schema.sql`):

- **Consulta 1 Zod** — `materials_weight_totals[]`: `{ material_name, material_code, kg, subtotal, taxes, total }` (Lambda `transform_materials_weight_zod` + catálogo `sa_materials`).
- **Consulta 2 tal cual** — `materials_categories_weight_totals[]`: `{ category, total_weight_kg, total_amount }`.

Tras desplegar Lambda: ejecutar SQL de comentarios en **Base Socio** y re-correr el pipeline para backfill del nuevo shape en `materials_weight_totals`.

Validación ODS vs lake/Supabase (categoría rebate por material): [`scripts/docs/sa_invoices_ods_category_mapping.md`](scripts/docs/sa_invoices_ods_category_mapping.md).

### Dominio **socio Adelca + cartera** (Supabase **Base Socio**)

Proyecto de **producción** del socio comercial. Pipelines con `"supabase_profile": "base_socio"` y `write_mode: upsert`.

**Ingesta secuencial (recomendada):** un solo invoke ejecuta los 5 pipelines en orden (config `lambda_function/domain_batches.json`):

```json
{ "domain_batch": "base_socio" }
```

| Orden | Pipeline | Tabla Base Socio |
|---:|---|---|
| 1 | `socio_adelca_grupos` | `sa_hardware_store_groups` |
| 2 | `socio_adelca_ferreterias` | `sa_hardware_stores` |
| 3 | `socio_adelca_materiales` | `sa_materials` |
| 4 | `socio_adelca_facturas_rebate` | `sa_invoices` |
| 5 | `cartera` | `in_debt_at` |

- **Cron:** EventBridge `ScheduledBaseSocioBatch` → `{"domain_batch":"base_socio"}` (reemplaza los 5 crons paralelos).
- **Manual:** `bash scripts/run-base-socio-batch.sh`
- **Logs batch:** `s3://{bucket}/etl-success/base_socio/batch/latest.json` (+ log detallado por corrida).
- **Fallo:** `stop_on_failure=true` — detiene en el primer pipeline con error; CloudWatch + S3 batch registran el paso fallido.
- **Warnings:** p. ej. `sa_materials` vacío antes de facturas (no aborta, solo warn).

Cada pipeline sigue invocable solo: `{"pipeline_name":"socio_adelca_grupos"}`.

| Pipeline | Tabla Base Socio | Origen |
|---|---|---|
| `socio_adelca_grupos` | `sa_hardware_store_groups` | QAS `dim_grupos_ferreteros_mtz` |
| `socio_adelca_ferreterias` | `sa_hardware_stores` | QAS `dim_ferreterias_mtz` |
| `socio_adelca_materiales` | `sa_materials` | QAS `dim_materiales_rebates_slv` |
| `socio_adelca_facturas_rebate` | `sa_invoices` | QAS `fact_invoices_sa` |
| `cartera` | `in_debt_at` | QAS `control_cartera` |

### Vista `api.vw_maestro_persona_conductor` (Supabase terciario)

Pipeline `conductores`: lee esta vista (`Accept-Profile: api`) y hace upsert en `public.drivers` del proyecto **secondary** (`id` = `person_id` de ZK, formateado a uuid Postgres; `national_id` desde cédula si existe; `license_plates` acumulado; `current_license_plate` = placa vigente).

**Regla de negocio:** todo visitante ZK con placa válida (`car_plate` o JSON en `custom_attrs`) cuenta como conductor; `person_id = COALESCE(pers_person.id, visitor.id)`.

1. Cargar Excel una vez en `api.stg_visitantes_reporte_cedula`: `python3 scripts/load_tertiary_stg_cedula_postgrest.py` (CSV con `scripts/load_visitantes_reporte_cedula.py`).
2. Ejecutar `scripts/sql/api_vw_maestro_persona_conductor.sql` en terciario — la vista añade `cedula` con JOIN directo a esa tabla (sin vistas helper).
3. Validar: `SELECT COUNT(*), COUNT(cedula) FROM api.vw_maestro_persona_conductor;`
4. Re-ejecutar pipeline: `{"pipeline_name":"conductores"}` en la Lambda.

### Tabla `public.drivers` (Supabase secondary)

Destino del pipeline `conductores`. Columnas: `name`, `national_id` (desde `cedula`, nullable), `phone`, `license_plates`, `current_license_plate`, `synced_at`. Grain: `id` = `person_id` ZK (`COALESCE(pers_person.id, visitor.id)`), sin uuid5 inventado.

`public.drivers_hist` (si existe en secondary) es **tabla legacy** — no la alimenta ningún pipeline; ver `scripts/sql/supabase_secondary_drivers_hist.sql` solo como referencia histórica.
