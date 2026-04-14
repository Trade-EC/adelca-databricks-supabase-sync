# Databricks PRD → Supabase Incremental Sync

AWS Lambda que sincroniza incrementalmente la tabla `prod.gldprd.dim_app_transportistas` de Databricks (produccion) hacia la tabla `dim_transportistas` en Supabase, ejecutandose automaticamente cada hora via Amazon EventBridge.

## Que hace la Lambda (`patek-philippe`)

### Flujo de ejecucion

```
EventBridge (rate: 1 hora)
        │
        ▼
   Lambda invocada
        │
        ▼
┌─────────────────────────────┐
│  1. Validar acceso a tablas │  Verifica que dim_transportistas
│     Supabase                │  y etl_watermarks esten accesibles
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  2. Leer codigos existentes │  Obtiene todos los codigo_transportista
│     en Supabase             │  ya presentes en dim_transportistas
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  3. Leer tabla completa     │  SELECT completo a Databricks PRD
│     desde Databricks        │  via OAuth M2M (Service Principal)
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  4. Deduplicar registros    │  Elimina codigo_transportista
│     de Databricks           │  duplicados del origen
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  5. Filtrar solo nuevos     │  Compara contra codigos existentes
│                             │  en Supabase, descarta los que ya existen
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  6. Insertar nuevos en      │  POST via REST API en batches de 500
│     Supabase                │  con UUID generado y estado='pendiente'
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  7. Verificar integridad    │  Confirma que el conteo post-sync
│                             │  coincide con lo esperado
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  8. Actualizar watermark    │  Registra timestamp de la ultima
│                             │  sincronizacion en etl_watermarks
└─────────────────────────────┘
```

### Reglas de negocio

| Concepto | Comportamiento |
|----------|---------------|
| **Registros nuevos** | Se insertan con `estado_transportista = 'pendiente'`, `is_first_login = true`, `role = 'driver'` |
| **Registros existentes** | **No se modifican**. La Lambda solo inserta, nunca actualiza registros que ya estan en Supabase |
| **Campos de la app** | `estado_transportista`, `is_first_login`, `role`, `welcome_at`, `birth_date`, `last_login` nunca son sobrescritos por el sync |
| **Clave de negocio** | `codigo_transportista` (UNIQUE) — identifica univocamente a cada transportista |
| **Primary Key** | `transportista_id` — UUID generado automaticamente al insertar |
| **Deduplicacion** | Si Databricks trae duplicados por `codigo_transportista`, se conserva solo el ultimo |

### Autenticacion

| Sistema | Metodo | Credenciales |
|---------|--------|-------------|
| **Databricks PRD** | OAuth M2M (Service Principal) | `client_id` + `client_secret` |
| **Supabase PRD** | REST API + Service Role Key | `project_url` + `service_role_key` |

## Estructura del proyecto

```
├── lambda_function/
│   ├── handler.py                       # Handler de la Lambda (logica principal)
│   └── requirements.txt                 # Dependencias de la Lambda
├── transportistas_sync/
│   ├── .env                             # Credenciales locales (no se commitea)
│   ├── .env.example                     # Plantilla de variables de entorno
│   └── databricks_connector.py          # Clase DatabricksConnector (QAS/PRD)
├── template.yaml                        # SAM template (Lambda + EventBridge)
├── samconfig.toml                       # Configuracion de deploy SAM
├── deploy.sh                            # Script de build & deploy
├── supabase_create_tables.sql           # DDL para crear tablas en Supabase
├── connector.py                         # Diagnostico: lee una tabla de Databricks
├── test_supabase_connection.py          # Test de conectividad a Supabase
├── load_transportistas_historico.py     # Carga historica inicial (uso unico)
├── sync_transportistas_incremental.py   # Sync incremental local (misma logica que la Lambda)
├── sync_dim_app_transportistas.py       # Sync legacy dim_app_transportistas
├── full_sync_to_supabase.py             # Sync legacy full table
├── sync_transportistas_final.py         # Sync legacy transportistas_final
├── sync_module.py                       # Modulo de sincronizacion base
└── requirements.txt                     # Dependencias para desarrollo local
```

## Tabla destino en Supabase: `dim_transportistas`

```sql
CREATE TYPE carrier_status_enum AS ENUM ('activo', 'inactivo', 'pendiente');
CREATE TYPE carrier_roles_enum AS ENUM ('driver', 'manager');

CREATE TABLE dim_transportistas (
    transportista_id        TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    ruc                     TEXT NOT NULL,
    codigo_transportista    TEXT UNIQUE NOT NULL,
    nombre_transportista    TEXT,
    telefono                TEXT,
    email                   TEXT,
    estado_transportista    carrier_status_enum NOT NULL DEFAULT 'pendiente',
    placas                  TEXT[],
    tipos_vehiculo          TEXT[],
    _ingested_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_first_login          BOOLEAN NOT NULL DEFAULT true,
    welcome_at              TIMESTAMPTZ,
    birth_date              DATE,
    role                    carrier_roles_enum NOT NULL DEFAULT 'driver',
    last_login              TIMESTAMPTZ
);

CREATE TABLE etl_watermarks (
    table_name      TEXT PRIMARY KEY,
    last_timestamp  TIMESTAMP
);
```

### Campos sincronizados desde Databricks

| Campo | Origen |
|-------|--------|
| `codigo_transportista` | `dim_app_transportistas.codigo_transportista` |
| `ruc` | `dim_app_transportistas.ruc` |
| `nombre_transportista` | `dim_app_transportistas.nombre_transportista` |
| `telefono` | `dim_app_transportistas.telefono` |
| `email` | `dim_app_transportistas.email` |
| `placas` | `dim_app_transportistas.placas` (array) |
| `tipos_vehiculo` | `dim_app_transportistas.tipos_vehiculo` (array) |

### Campos gestionados por la aplicacion (no tocados por el sync)

| Campo | Default al insertar | Descripcion |
|-------|-------------------|-------------|
| `transportista_id` | UUID generado | Identificador unico interno |
| `estado_transportista` | `'pendiente'` | Se cambia a `'activo'`/`'inactivo'` desde la app |
| `is_first_login` | `true` | Se pone en `false` tras el primer login |
| `role` | `'driver'` | Asignado desde la app |
| `welcome_at` | `null` | Timestamp del primer login |
| `birth_date` | `null` | Ingresado por el usuario |
| `last_login` | `null` | Actualizado por la app en cada login |
| `_ingested_at` | Timestamp UTC del sync | Cuando fue insertado por la Lambda |

## Configuracion de la Lambda

### Variables de entorno

| Variable | Descripcion |
|----------|-------------|
| `DATABRICKS_HOST` | Hostname del SQL Warehouse de Databricks PRD |
| `DATABRICKS_HTTP_PATH` | HTTP path del SQL Warehouse |
| `DATABRICKS_CLIENT_ID` | Client ID del Service Principal |
| `DATABRICKS_CLIENT_SECRET` | Client Secret del Service Principal |
| `SUPABASE_URL` | URL del proyecto Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | Service Role Key de Supabase |
| `DBX_TABLE` | Tabla origen en Databricks (default: `prod.gldprd.dim_app_transportistas`) |
| `BATCH_SIZE` | Registros por batch de insercion (default: `500`) |

### Recursos AWS (SAM)

| Recurso | Tipo | Detalle |
|---------|------|---------|
| `patek-philippe` | Lambda Function | Python 3.12, 256 MB, timeout 300s |
| EventBridge Rule | Schedule | `rate(1 hour)` |
| CloudWatch Log Group | Logs | Retencion 14 dias |

### Dependencias de la Lambda

| Paquete | Version | Uso |
|---------|---------|-----|
| `databricks-sql-connector` | >= 4.0.0 | Conexion SQL a Databricks |
| `databricks-sdk` | >= 0.100.0 | OAuth M2M (Service Principal) |
| `httpx` | >= 0.27.0 | HTTP client para Supabase REST API |

## Setup local

### 1. Entorno Python

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Credenciales

```bash
cp transportistas_sync/.env.example transportistas_sync/.env
```

Editar `transportistas_sync/.env` con las credenciales de ambos ambientes (QAS y PRD).

### 3. Probar conectividad

```bash
# Databricks (QAS o PRD segun DATABRICKS_ENV en .env)
python connector.py --env prd

# Supabase
python test_supabase_connection.py
```

### 4. Sync local

```bash
# Carga historica inicial (una sola vez, tabla vacia)
python load_transportistas_historico.py

# Sync incremental (misma logica que la Lambda)
python sync_transportistas_incremental.py
```

## Deploy a AWS

### Prerequisitos

- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) configurado (`aws configure`)
- [SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) instalado
- Docker corriendo (requerido para `sam build --use-container`)

### Desplegar

```bash
# Primer deploy (interactivo)
./deploy.sh --guided

# Deploys subsiguientes (usa samconfig.toml)
./deploy.sh
```

### Monitorear

```bash
# Logs en tiempo real
sam logs -n patek-philippe --tail

# Invocacion manual
aws lambda invoke --function-name patek-philippe /tmp/response.json && cat /tmp/response.json

# Verificar ultimo sync en Supabase
# SELECT * FROM etl_watermarks WHERE table_name = 'dim_transportistas';
```

### Respuesta de la Lambda

```json
{
  "statusCode": 200,
  "body": {
    "status": "success",
    "inserted": 5,
    "total": 488,
    "sync_timestamp": "2026-03-20T20:02:14.597974+00:00"
  }
}
```

| Campo | Descripcion |
|-------|-------------|
| `status` | `success`, `no_data` (sin registros en Databricks), o `error` |
| `inserted` | Cantidad de registros nuevos insertados en esta ejecucion |
| `total` | Total de registros en `dim_transportistas` post-sync |
| `sync_timestamp` | Timestamp UTC de la sincronizacion |

## Costos estimados

| Servicio | Detalle | Costo |
|----------|---------|-------|
| **Lambda** | ~45s por ejecucion x 24/dia x 256 MB | Dentro del free tier |
| **Databricks** | SQL Warehouse compute por un SELECT pequeno | Minimo |
| **Supabase** | REST API calls | Dentro del free tier |
| **EventBridge** | 24 invocaciones/dia | Gratuito |
| **CloudWatch Logs** | ~14 dias de retencion | Minimo |
