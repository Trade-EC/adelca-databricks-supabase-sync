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
    transportista_id        text PRIMARY KEY DEFAULT gen_random_uuid()::text,
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
