-- Transportistas final (synced from Databricks)
-- Run in Supabase SQL Editor

CREATE TABLE IF NOT EXISTS public.transportistas_final (
    codigo_transportista    text PRIMARY KEY,
    nombre                  text,
    documento               text,
    telefono                text,
    email                   text,
    direccion               text,
    ciudad                  text,
    codigo_postal           text,
    pais                    text,
    datos_vehiculo          text,
    datos_dueno_vehiculo    text,
    estado                  text,
    total_transportes       bigint,
    ultima_fecha_transporte date,
    _ingested_at            timestamp DEFAULT CURRENT_TIMESTAMP
);

-- Transportistas PRD (synced from Databricks PRD: prod.gldprd.dim_app_transportistas)
-- Requires ENUM types created first.

CREATE TYPE carrier_status_enum AS ENUM ('activo', 'inactivo', 'pendiente');
CREATE TYPE carrier_roles_enum AS ENUM ('driver', 'manager');

CREATE TABLE IF NOT EXISTS public.transportistas (
    transportista_id        uuid PRIMARY KEY,
    ruc                     text NOT NULL,
    codigo_transportista    text UNIQUE NOT NULL,
    nombre_transportista    text,
    telefono                text,
    email                   text,
    estado_transportista    carrier_status_enum NOT NULL DEFAULT 'pendiente',
    placas                  text[],
    tipos_vehiculo          text[],
    _ingested_at            timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,

    is_first_login          boolean NOT NULL DEFAULT true,
    welcome_at              timestamptz,
    birth_date              date,
    role                    carrier_roles_enum NOT NULL DEFAULT 'driver',
    last_login              timestamptz
);

-- ETL watermarks (tracks last sync timestamp per table)
CREATE TABLE IF NOT EXISTS public.etl_watermarks (
    table_name      text PRIMARY KEY,
    last_timestamp  timestamp
);

-- ETL runs metadata (one row per pipeline execution)
CREATE TABLE IF NOT EXISTS public.etl_runs (
    run_id           bigint generated always as identity primary key,
    pipeline_name    text NOT NULL,
    status           text NOT NULL,
    source_table     text,
    target_table     text,
    inserted_count   integer NOT NULL DEFAULT 0,
    updated_count    integer NOT NULL DEFAULT 0,
    skipped_count    integer NOT NULL DEFAULT 0,
    error_message    text,
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz NOT NULL DEFAULT now(),
    duration_ms      integer,
    s3_log_uri       text,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_etl_runs_pipeline_created_at
  ON public.etl_runs (pipeline_name, created_at DESC);

-- Pipeline 2: QAS qas.aplicaciones.dim_vehiculos → public.vehicles (secondary Supabase).
-- The live `vehicles` table is app-defined; typical columns: id, license_plate, type, model,
-- weight_average_capacity, weight_average_capacity_unit, fleet_owner_unique_code, is_route_based, synced_at.

-- Pipeline socio_adelca_ferreterias: QAS qas.aplicaciones.dim_ferreterias_mtz → public.sa_hardware_stores (secondary).
-- Destino típico (OpenAPI): id, store_group_id, name, tax_id, created_at, updated_at, deleted_at.
-- ETL: id = uuid5(codigo_cliente); store_group_id = uuid5(id_grupo) (mismo namespace que socio_adelca_grupos);
-- ruc→tax_id, nombre_cliente→name; fill_missing_iso_timestamps en created_at/updated_at. No mapeados: ciudad, teléfono, asesor, flags, etc.

-- Pipeline socio_adelca_grupos: QAS qas.aplicaciones.dim_grupos_ferreteros_mtz → public.sa_hardware_store_groups (secondary).
-- Destino típico: id, name, addresses (jsonb), data_representative (jsonb), member_since, created_at, updated_at. ETL: id = uuid5(id_grupo);
-- nombre_grupo→name; fecha_corte→member_since; addresses[0] = { ciudad }; data_representative = { code, name, phone, email } desde columnas MTZ del asesor.
-- Ejecutar en el **mismo** proyecto Supabase donde está `sa_hardware_store_groups` (secondary). Luego: Settings → API → Reload schema.
ALTER TABLE IF EXISTS public.sa_hardware_store_groups
  ADD COLUMN IF NOT EXISTS data_representative jsonb;

-- Pipeline socio_adelca_materiales: QAS qas.aplicaciones.dim_materiales_rebates_slv → public.sa_materials (secondary).
-- Carga full por bajo volumen (upsert) con id = uuid5(codigo_material), synced_at incluido.
-- Mapping actual: codigo_material→code, descripcion_material→description, categoria_rebate→cashback_category.

-- Pipeline socio_adelca_facturas_rebate: QAS qas.aplicaciones.fact_invoices_sa → public.sa_invoices (secondary).
-- El destino en secondary es el modelo de cabecera de factura (alineado con el origen): invoice_number, lines jsonb,
-- materials_* jsonb, issued_date, montos, store_group_id / store_tax_id, timestamps, etc. Ver OpenAPI GET /rest/v1/.
-- Lambda: json_parse_targets para columnas jsonb enviadas como string desde Databricks; fill_missing_iso_timestamps
-- solo created_at si viene vacío; updated_at no se envía desde Databricks (default/trigger en Supabase).
-- include_ingested_at=false (no hay synced_at en esta tabla).
-- Tras ALTER nullable en sa_invoices: Settings → API → Reload schema en Supabase; luego relajar pipelines.json (sin require store_group_id ni coalesce tax).
-- store_tax_id: quitar null_coalesce en pipelines cuando en Supabase sea nullable y PostgREST vea el cambio.
