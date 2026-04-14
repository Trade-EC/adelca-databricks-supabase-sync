# Propuesta: Corrección del campo Email en dim_app_transportistas

**Fecha:** 2026-03-26
**Tabla afectada:** `prod.gldprd.dim_app_transportistas`
**Notebook:** `/Users/felix.cardenas@trade.ec/data_lake_adelca_prod/ingesta/ingesta_gold/TRANSPORTISTAS`

---

## Problema detectado

El campo `email` de la tabla `dim_app_transportistas` está mapeado al campo **`LFA1.LFURL`**, que en SAP corresponde a la **URL/sitio web** del proveedor, no a su dirección de correo electrónico.

### Impacto actual

| Métrica | Valor |
|---------|-------|
| Total de transportistas | 485 |
| Email vacío | 475 (97.9%) |
| Email con valor | 10 (2.1%) |

Los 10 registros que tienen email son datos ingresados manualmente en el campo incorrecto de SAP (`LFURL`). No representan una fuente confiable.

### Código actual (línea del notebook)

```sql
SELECT
    ...
    lfurl AS email,       -- ← Campo incorrecto: LFURL es URL, no email
    ...
FROM slvprd.vttk AS v
LEFT JOIN slvprd.lfa1 AS l ON v.tdlnr = l.lifnr
```

---

## Solución propuesta

### Paso 1: Extraer la tabla `ADR6` de SAP al Data Lake

La tabla `ADR6` es la tabla estándar de SAP para direcciones de correo electrónico (SMTP). Actualmente **no está siendo extraída** al bronze layer.

**Campos relevantes de ADR6:**

| Campo | Descripción |
|-------|-------------|
| `ADDRNUMBER` | Número de dirección (clave para unir con `LFA1.ADRNR`) |
| `PERSNUMBER` | Número de persona |
| `DATE_FROM` | Fecha inicio de validez |
| `CONSNUMBER` | Número consecutivo |
| `FLGDEFAULT` | Indicador de dirección por defecto (`X`) |
| `SMTP_ADDR` | **Dirección de correo electrónico** |
| `SMTP_SRCH` | Email en mayúsculas (para búsqueda) |
| `HOME_FLAG` | Indicador personal/laboral |
| `R3_USER` | Usuario SAP asociado |

**Acción requerida:** Agregar `ADR6` al pipeline de ingesta existente:
- Bronze: `prod.brzprd.adr6`
- Silver: `prod.slvprd.adr6`

### Paso 2: Modificar el notebook TRANSPORTISTAS

Cambiar la query del CTE `base_data` para unir con `ADR6`:

```sql
-- ANTES (actual):
SELECT
    v.tdlnr,
    l.stcd1,
    l.name1,
    l.lfurl,                                    -- Campo incorrecto
    ...
FROM {CATALOGO}.{ESQUEMA_SILVER}.vttk AS v
LEFT JOIN {CATALOGO}.{ESQUEMA_SILVER}.lfa1 AS l
    ON v.tdlnr = l.lifnr
...

-- DESPUÉS (propuesto):
SELECT
    v.tdlnr,
    l.stcd1,
    l.name1,
    a6.smtp_addr AS lfurl,                      -- Email real desde ADR6
    ...
FROM {CATALOGO}.{ESQUEMA_SILVER}.vttk AS v
LEFT JOIN {CATALOGO}.{ESQUEMA_SILVER}.lfa1 AS l
    ON v.tdlnr = l.lifnr
LEFT JOIN {CATALOGO}.{ESQUEMA_SILVER}.adr6 AS a6
    ON l.adrnr = a6.addrnumber
    AND a6.flgdefault = 'X'                     -- Solo el email por defecto
    AND a6.persnumber = '000'                    -- Dirección de la empresa, no personal
...
```

El alias se mantiene como `lfurl` para no romper el resto del notebook (el `GROUP BY` y el `SELECT` final).

### Paso 3: Verificación post-cambio

```sql
-- Verificar cobertura de emails después del cambio
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN smtp_addr IS NOT NULL AND TRIM(smtp_addr) != '' THEN 1 ELSE 0 END) AS con_email,
    SUM(CASE WHEN smtp_addr IS NULL OR TRIM(smtp_addr) = '' THEN 1 ELSE 0 END) AS sin_email
FROM slvprd.lfa1 l
LEFT JOIN slvprd.adr6 a6
    ON l.adrnr = a6.addrnumber
    AND a6.flgdefault = 'X'
    AND a6.persnumber = '000'
WHERE l.lifnr IN (SELECT DISTINCT tdlnr FROM slvprd.vttk WHERE erdat >= '2025-01-01');
```

---

## Diagrama de relación SAP

```
LFA1 (Maestro Proveedores)
  │
  ├── LIFNR (código vendor) ← VTTK.TDLNR (transportista)
  ├── ADRNR (número dirección) ──→ ADR6.ADDRNUMBER
  ├── NAME1 (nombre)
  ├── STCD1 (RUC)
  ├── TELF1, TELF2 (teléfonos)
  └── LFURL (URL, NO email) ← actualmente usado como email ❌
                                    │
                                    ▼
ADR6 (Emails SMTP)  ← TABLA FALTANTE EN DATA LAKE
  │
  ├── ADDRNUMBER ──→ LFA1.ADRNR
  ├── SMTP_ADDR ← email real ✓
  ├── FLGDEFAULT ('X' = principal)
  └── PERSNUMBER ('000' = empresa)
```

---

## Resumen de acciones

| # | Acción | Responsable | Prioridad |
|---|--------|-------------|-----------|
| 1 | Extraer tabla `ADR6` de SAP al bronze/silver | Equipo Data (ingesta) | Alta |
| 2 | Modificar notebook TRANSPORTISTAS (cambiar `lfurl` por `adr6.smtp_addr`) | Felix Cárdenas | Alta |
| 3 | Verificar cobertura de emails post-cambio | QA | Media |
| 4 | Sync automático propagará emails a Supabase vía Lambda `patek-philippe` | Automático | - |

No se requiere ningún cambio en la Lambda ni en Supabase — una vez que el campo `email` llegue correctamente desde Databricks, el sync incremental insertará los nuevos registros con el email correcto.
