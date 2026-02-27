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

-- ETL watermarks (tracks last sync timestamp per table)
CREATE TABLE IF NOT EXISTS public.etl_watermarks (
    table_name      text PRIMARY KEY,
    last_timestamp  timestamp
);
