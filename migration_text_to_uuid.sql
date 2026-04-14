-- ================================================================
-- MIGRACION: transportista_id TEXT → UUID nativo
-- Ejecutar en Supabase SQL Editor
-- ================================================================

BEGIN;

-- 1. Tabla principal: transportistas
ALTER TABLE public.transportistas
  ALTER COLUMN transportista_id SET DATA TYPE uuid USING transportista_id::uuid,
  ALTER COLUMN transportista_id DROP DEFAULT;

-- 2. FK: facturas.transportista_id
ALTER TABLE public.facturas
  ALTER COLUMN transportista_id SET DATA TYPE uuid USING transportista_id::uuid;

-- 3. FK: carrier_deletion_feedbacks.carrier_id
ALTER TABLE public.carrier_deletion_feedbacks
  ALTER COLUMN carrier_id SET DATA TYPE uuid USING carrier_id::uuid;

-- 4. FK: purchases.carrier_id
ALTER TABLE public.purchases
  ALTER COLUMN carrier_id SET DATA TYPE uuid USING carrier_id::uuid;

-- 5. dim_transportistas
ALTER TABLE public.dim_transportistas
  ALTER COLUMN transportista_id SET DATA TYPE uuid USING transportista_id::uuid,
  ALTER COLUMN transportista_id DROP DEFAULT;

-- 6. transportistas_duplicate
ALTER TABLE public.transportistas_duplicate
  ALTER COLUMN transportista_id SET DATA TYPE uuid USING transportista_id::uuid,
  ALTER COLUMN transportista_id DROP DEFAULT;

-- 7. Refresh PostgREST schema cache
NOTIFY pgrst, 'reload schema';

COMMIT;
