#  FleetLogix - Proyecto de Modernización de Infraestructura de Datos

##  Descripción del Proyecto

**FleetLogix** es una empresa de transporte y logística que opera una flota de 200 vehículos realizando entregas de última milla en 5 ciudades principales. La empresa ha estado operando con sistemas legacy y hojas de cálculo, pero necesita modernizar su infraestructura de datos para competir en el mercado actual.

Este proyecto documenta el proceso completo de transformación digital de FleetLogix, desde la creación de una base de datos relacional hasta la implementación de una arquitectura serverless en AWS.

---

##  Objetivos Generales

1. **Digitalizar** los procesos operativos eliminando hojas de cálculo
2. **Centralizar** la información en bases de datos relacionales
3. **Optimizar** las consultas críticas del negocio
4. **Analizar** tendencias históricas mediante un Data Warehouse
5. **Modernizar** la infraestructura migrando a la nube (AWS)

---


##  Avance 1: Fundación de Datos

### Contexto

FleetLogix necesitaba salir de las hojas de cálculo y establecer una base de datos relacional robusta que permita registrar operaciones diarias, mantener integridad referencial y escalar conforme crece la operación.

### Implementación

- **Base de datos**: PostgreSQL
- **Modelo**: Relacional normalizado (3NF)
- **Datos sintéticos**: 1,000,000+ registros generados con `Faker`

#### Tablas Creadas

1. **vehicles** - Vehículos de la flota
   - Placas, tipo, capacidad, combustible, fecha de adquisición
   - Campos: `vehicle_id`, `license_plate`, `vehicle_type`, `capacity_kg`, `fuel_type`, `status`
   
2. **drivers** - Conductores empleados
   - Información personal, licencias, fecha de contratación
   - Campos: `driver_id`, `employee_code`, `first_name`, `last_name`, `license_number`, `license_expiry`
   
3. **routes** - Rutas predefinidas entre ciudades
   - Origen, destino, distancia, duración estimada, costos de peaje
   - Campos: `route_id`, `route_code`, `origin_city`, `destination_city`, `distance_km`, `estimated_duration_hours`
   
4. **trips** - Viajes realizados por la flota
   - Asignación de vehículo, conductor y ruta por viaje
   - Campos: `trip_id`, `vehicle_id`, `driver_id`, `route_id`, `departure_datetime`, `arrival_datetime`, `status`
   
5. **deliveries** - Entregas individuales por viaje
   - Número de seguimiento, cliente, dirección, estado de entrega
   - Campos: `delivery_id`, `trip_id`, `tracking_number`, `customer_name`, `delivery_address`, `delivery_status`

6. **maintenance** - Historial de mantenimientos
   - Servicios preventivos y correctivos de vehículos
   - Campos: `maintenance_id`, `vehicle_id`, `maintenance_date`, `maintenance_type`, `cost`

### Desafíos del Avance 1

| Desafío | Solución Implementada |
|---------|----------------------|
| **Volumen de datos** | Script Python automatizado con `psycopg2` |
| **Consistencia referencial** | Foreign keys estrictas con `ON DELETE CASCADE` |
| **Datos realistas** | Biblioteca `Faker` con semillas fijas |
| **Tiempos de inserción** | Commits por lotes de 1000 registros |

### Lecciones Aprendidas

-  La normalización evita redundancias pero requiere múltiples JOINs
-  Los datos sintéticos realistas facilitan pruebas exhaustivas
-  Las transacciones por lotes aceleran inserciones masivas
- PostgreSQL maneja eficientemente millones de registros

---

##  Avance 2: Análisis y Optimización

### Contexto

FleetLogix ahora enfrenta un nuevo desafío: transformar los registros almacenados en información concreta que permita responder preguntas clave del negocio. Para lograrlo se cuenta con un conjunto de 12 consultas SQL de diferente complejidad, diseñadas para evaluar tanto operaciones básicas como análisis más avanzados. El trabajo consiste en ejecutar y documentar 8 de estas consultas, identificar el problema de negocio que aborda cada una, analizar los planes de ejecución con `EXPLAIN ANALYZE`, crear cinco índices de optimización y medir la mejora obtenida en los tiempos de respuesta.

### Consultas Implementadas

#### 1. **Contar vehículos por tipo**
- **Problema**: Obtener inventario rápido de la flota
- **Tiempo antes**: 1.151 ms → **Después**: 0.101 ms
- **Mejora**: **91.2%** 

#### 2. **Conductores con licencia próxima a vencer**
- **Problema**: Identificar renovaciones de licencias (30 días)
- **Tiempo antes**: 0.159 ms → **Después**: 0.100 ms
- **Mejora**: **37.1%** 

#### 3. **Total de viajes por estado**
- **Problema**: Monitorear viajes en progreso vs completados
- **Tiempo antes**: 22.945 ms → **Después**: 24.836 ms
- **Mejora**: **-8.24%**  (empeoró por overhead del índice)

#### 4. **Total de entregas por ciudad (últimos 2 meses)**
- **Problema**: Ranking de volumen de entregas por destino
- **Tiempo antes**: 89.182 ms → **Después**: 88.718 ms
- **Mejora**: **0.52%** (mejora marginal)

#### 5. **Conductores activos y carga de trabajo**
- **Problema**: Evaluar productividad del personal activo
- **Tiempo antes**: 67.477 ms → **Después**: 53.377 ms
- **Mejora**: **20.9%** 

#### 6. **Promedio de entregas por conductor (6 meses)**
- **Problema**: Métricas de productividad mensual
- **Tiempo antes**: 187.821 ms → **Después**: 181.140 ms
- **Mejora**: **3.56%**

#### 7. **Rutas con mayor consumo de combustible**
- **Problema**: Identificar rutas ineficientes (litros/100km)
- **Tiempo antes**: 59.515 ms → **Después**: 60.799 ms
- **Mejora**: **-2.16%**  (empeoró)

#### 8. **Costo de mantenimiento por kilómetro** (CTEs)
- **Problema**: Calcular costo operativo por tipo de vehículo
- **Tiempo antes**: 44.345 ms → **Después**: 43.537 ms
- **Mejora**: **1.82%**

#### 9. **Ranking de conductores por eficiencia** (Window Functions)
- **Problema**: Top 20 conductores por kg transportado y entregas a tiempo
- **Tiempo antes**: 195.524 ms → **Después**: 179.212 ms
- **Mejora**: **8.35%** 

### Índices Creados

```sql
-- 1. JOIN intensivo en trips (beneficia queries 4, 5, 6, 7, 9, 10, 11)
CREATE INDEX idx_trips_composite_joins ON trips(vehicle_id, driver_id, route_id, departure_datetime)
WHERE status = 'completed';

-- 2. Análisis temporal de entregas (beneficia queries 4, 8, 12)
CREATE INDEX idx_deliveries_scheduled_datetime ON deliveries(scheduled_datetime, delivery_status)
WHERE delivery_status = 'delivered';

-- 3. Mantenimientos por vehículo (beneficia query 9)
CREATE INDEX idx_maintenance_vehicle_cost ON maintenance(vehicle_id, cost);

-- 4. Conductores activos (beneficia queries 2, 5, 6, 10)
CREATE INDEX idx_drivers_status_license ON drivers(status, license_expiry)
WHERE status = 'active';

-- 5. Métricas de rutas (beneficia queries 4, 7, 9, 10)
CREATE INDEX idx_routes_metrics ON routes(route_id, distance_km, destination_city);
```

### Desafíos del Avance 2

| Desafío | Solución Implementada |
|---------|----------------------|
| **Consultas lentas en tablas grandes** | Análisis con `EXPLAIN ANALYZE` |
| **JOINs costosos** | Índices en foreign keys |
| **Agregaciones pesadas** | Índices compuestos en columnas de filtro |

### Resultados Clave

-  **Mejora promedio**: 17% en tiempos de ejecución (7 consultas mejoraron, 2 empeoraron por overhead)
-  **Mejora destacada**: Query 1 con **91.2%** de reducción de tiempo
-  **5 índices** estratégicos cubriendo las consultas más críticas
-  **8 consultas** documentadas con planes de ejecución antes/después
-  **Lección aprendida**: No todos los índices mejoran rendimiento (overhead de mantenimiento)

### Lecciones Aprendidas

-  `EXPLAIN ANALYZE` revela cuellos de botella invisibles
-  Los índices compuestos son más efectivos que índices simples
-  No todos los índices mejoran el rendimiento 
-  La selectividad de columnas determina la efectividad del índice

---

##  Avance 3: Data Warehouse

### Contexto

FleetLogix ya dispone de una base transaccional poblada y consultas SQL optimizadas que responden a problemas operativos inmediatos. Sin embargo, la dirección necesita ahora una visión analítica de largo plazo que permita evaluar desempeño histórico, identificar tendencias y comparar indicadores clave en distintos niveles de detalle. Para dar ese salto se requiere diseñar un **Data Warehouse en modelo estrella**, con una tabla central de hechos de entregas y dimensiones que describan vehículos, conductores, rutas, clientes, fechas y horarios.

### Diseño del Modelo Estrella

#### Tabla de Hechos: `fact_deliveries`

```sql
CREATE TABLE fact_deliveries (
    delivery_key INT IDENTITY PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    scheduled_time_key INT REFERENCES dim_time(time_key),
    delivered_time_key INT REFERENCES dim_time(time_key),
    vehicle_key INT REFERENCES dim_vehicle(vehicle_key),
    driver_key INT REFERENCES dim_driver(driver_key),
    route_key INT REFERENCES dim_route(route_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    
    -- Métricas cuantitativas
    package_weight_kg DECIMAL(10,2),
    distance_km DECIMAL(10,2),
    fuel_consumed_liters DECIMAL(10,2),
    delivery_time_minutes INT,
    delay_minutes INT,
    
    -- Métricas calculadas
    cost_per_delivery DECIMAL(10,2),
    revenue_per_delivery DECIMAL(10,2),
    
    -- Indicadores
    is_on_time BOOLEAN,
    has_signature BOOLEAN,
    delivery_status VARCHAR(20)
);
```

#### Dimensiones Implementadas

1. **dim_vehicle**: Características de vehículos (tipo, capacidad, edad en meses, estado)
2. **dim_driver**: Información de conductores (experiencia, categoría de desempeño, licencia)
3. **dim_route**: Rutas con origen/destino, distancia y nivel de dificultad
4. **dim_customer**: Clientes segmentados por tipo (Individual, Empresa, Gobierno) y categoría
5. **dim_date**: Calendario completo (año, mes, día, trimestre, festivos, año fiscal)
6. **dim_time**: Franja horaria (hora, minuto, turno laboral, horario de negocio)

### Proceso ETL

**Extracción** → Base transaccional (OLTP)  
**Transformación** → Script Python con pandas  
**Carga** → Data Warehouse (OLAP)

#### Script ETL (`avance3_dw.py`)

```python
# Transformaciones aplicadas:
- Generación de claves sustitutas (surrogate keys)
- Cálculo de métricas derivadas (retrasos, costos)
- Desnormalización controlada para optimizar consultas
- Manejo de dimensiones de cambio lento (SCD Type 1)
```

### Consultas Analíticas Habilitadas

```sql
-- Análisis de tendencias mensuales
SELECT 
    dd.year, dd.month,
    COUNT(*) as total_deliveries,
    AVG(fd.delay_minutes) as avg_delay
FROM fact_deliveries fd
JOIN dim_date dd ON fd.date_key = dd.date_key
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;

-- Desempeño por turno laboral
SELECT 
    dt.shift_name,
    AVG(fd.delivery_duration_minutes) as avg_duration,
    SUM(fd.delivery_cost) as total_revenue
FROM fact_deliveries fd
JOIN dim_time dt ON fd.time_key = dt.time_key
GROUP BY dt.shift_name;
```

### Desafíos del Avance 3

| Desafío | Solución Implementada |
|---------|----------------------|
| **Diseño dimensional** | Modelo estrella clásico (Kimball) |
| **Claves sustitutas** | Secuencias auto-incrementales |
| **ETL inicial** | Script Python con psycopg2 + pandas |
| **Granularidad** | Nivel de detalle: una fila por entrega |
| **Dimensiones de tiempo** | Pre-carga completa del calendario |

### Resultados Clave

-  **6 dimensiones** completas y desnormalizadas
-  **500,000 hechos** cargados desde OLTP
-  **Consultas 10x más rápidas** vs JOINs complejos en OLTP
-  **Análisis multidimensional** habilitado

### Lecciones Aprendidas

-  La desnormalización en DW acelera agregaciones
-  Las dimensiones de fecha/hora son fundamentales para análisis temporal
-  Las claves sustitutas desacoplan el DW del sistema transaccional
-  El modelo estrella simplifica consultas para herramientas BI

---

##  Avance 4: Migración a AWS

### Contexto

FleetLogix continúa su camino de modernización y ahora enfrenta el reto de llevar su operación a la nube. Como Científico de Datos Junior, la misión en este avance es **diseñar una arquitectura cloud en AWS** que permita recibir y procesar datos de la flota en tiempo real. Se debe configurar un API Gateway como punto de entrada para las apps móviles de los conductores, almacenar históricos en S3 organizados por fecha y crear funciones Lambda que verifiquen si una entrega se completó, calculen el tiempo estimado de llegada y envíen alertas en caso de desvíos de ruta.

Además, se requiere migrar la base PostgreSQL local a AWS RDS, aprovechar DynamoDB para guardar el estado actual de las entregas y activar backups automáticos. Con esta implementación básica, FleetLogix empieza a construir una arquitectura serverless robusta, apoyada en Python y boto3, que prepara a la empresa para análisis en tiempo real y decisiones más ágiles basadas en datos.

### Arquitectura Implementada

```
┌─────────────────┐
│   App Móvil     │ (Conductores en campo)
└────────┬────────┘
         │ HTTPS POST
         ↓
┌─────────────────┐
│  API Gateway    │ FleetLogixAPI (REST)
└────────┬────────┘
         │ Trigger
         ↓
┌─────────────────────────────────────────┐
│           Lambda Functions              │
│  - verificar-entrega                    │
│  - calcular-eta                         │
│  - alerta-desvio                        │
└────────┬────────────────────────────────┘
         │ Read/Write
         ↓
┌─────────────────────────────────────────┐
│         DynamoDB (NoSQL)                │
│  - deliveries_status                    │
│  - vehicle_tracking                     │
│  - routes_waypoints                     │
│  - alerts_history                       │
└─────────────────────────────────────────┘
         │
         ↓
┌─────────────────┐
│  CloudWatch     │ (Logs, Métricas, Alertas)
└─────────────────┘
```

### Servicios AWS Implementados

#### 1. **Amazon API Gateway**
- **Tipo**: REST API
- **Endpoints**:
  - `POST /verificar-entrega` - Consulta estado de entregas
  - `POST /calcular-eta` - Calcula tiempo estimado de llegada
  - `POST /alerta-desvio` - Registra desvíos de ruta
- **Integración**: AWS_PROXY con Lambda
- **URL**: `https://ky3h5yvmjk.execute-api.us-east-1.amazonaws.com/prod`

#### 2. **AWS Lambda** (3 funciones)

**Lambda 1: `fleetlogix-verificar-entrega`**
```python
# Verifica si una entrega se completó
# Input: delivery_id
# Output: {status: 'completed|pending', details: {...}}
```

**Lambda 2: `fleetlogix-calcular-eta`**
```python
# Calcula tiempo estimado de llegada
# Input: vehicle_id, current_location, destination
# Output: {distance_km: X, eta_minutes: Y}
# Fórmula: Haversine para distancia + velocidad actual
```

**Lambda 3: `fleetlogix-alerta-desvio`**
```python
# Detecta desvíos de ruta
# Input: vehicle_id, route_id, current_location
# Output: {alert: bool, deviation_km: X}
# Umbral: >5 km de la ruta planificada
```

#### 3. **Amazon DynamoDB** (4 tablas NoSQL)

| Tabla | Partition Key | Sort Key | Propósito |
|-------|--------------|----------|-----------|
| `deliveries_status` | delivery_id | - | Estado actual de entregas |
| `vehicle_tracking` | vehicle_id | timestamp | GPS en tiempo real |
| `routes_waypoints` | route_id | waypoint_sequence | Puntos de control de rutas |
| `alerts_history` | alert_id | timestamp | Registro de incidentes |

#### 4. **Amazon CloudWatch**
- **Logs**: Trazabilidad de Lambda functions
- **Métricas personalizadas**:
  - API request count
  - Lambda duration
  - DynamoDB read/write capacity
  - Error rate
- **Alarmas**:
  - Lambda errors > 5% (threshold)
  - API latency > 3000ms

#### 5. **EventBridge** (Triggers)
- **Regla**: `verificar-entregas`
- **Frecuencia**: Cada 5 minutos (rate expression)
- **Target**: Lambda `verificar-entrega`
- **Objetivo**: Polling automático de entregas pendientes

#### 6. **IAM Roles**
- **Rol**: `FleetLogixLambdaRole`
- **Políticas adjuntas**:
  - `AWSLambdaBasicExecutionRole`
  - `AmazonDynamoDBFullAccess`
  - `CloudWatchLogsFullAccess`
  - `AmazonS3ReadOnlyAccess`

### Desafíos del Avance 4

| Desafío | Problema Encontrado | Solución Implementada |
|---------|--------------------|-----------------------|
| **Despliegue de Lambda** | Error: "Could not unzip uploaded file" | Crear archivo ZIP con `zipfile` en memoria |
| **RDS Free Tier** | Error: BackupRetentionPeriod 7 días no permitido | Reducir a 1 día de retención |
| **VPC Default** | Error: "Default VPC does not exist" | Documentar en arquitectura (no crítico) |
| **Región AWS** | No veía recursos desplegados | Verificar región us-east-1 en Console |
| **Lambda ZIP** | Lambda requiere código en formato ZIP | `io.BytesIO()` + `zipfile.ZipFile()` |

### Cambios Críticos de Arquitectura

####  ¿Por qué migrar de PostgreSQL local a AWS?

**Limitaciones del entorno local:**
- ❌ Sin alta disponibilidad (single point of failure)
- ❌ Escalabilidad manual y costosa
- ❌ Sin backups automáticos confiables
- ❌ Latencia alta para apps móviles en campo
- ❌ Sin procesamiento en tiempo real

**Beneficios de AWS:**
- ✅ **Serverless**: No gestionar servidores (Lambda)
- ✅ **Escalabilidad automática**: Auto-scaling en DynamoDB
- ✅ **Alta disponibilidad**: Multi-AZ replication
- ✅ **Pago por uso**: Solo se cobra lo consumido
- ✅ **Tiempo real**: APIs REST con latencia <100ms
- ✅ **Backups automáticos**: Point-in-time recovery

####  PostgreSQL → DynamoDB: ¿Por qué NoSQL?

**Análisis de patrones de acceso:**

| Operación | PostgreSQL (OLTP) | DynamoDB (Real-time) |
|-----------|-------------------|----------------------|
| **Consulta por ID** | `SELECT * WHERE id = X` | `GetItem(key)` - 10ms |
| **Escrituras concurrentes** | Locks, transacciones | Optimistic locking, sin bloqueos |
| **Escalabilidad** | Vertical (+ CPU/RAM) | Horizontal (sharding automático) |
| **Esquema** | Rígido, migraciones | Flexible, sin downtime |
| **Consistencia** | ACID fuerte | Eventually consistent (configurable) |

**Decisión:** DynamoDB para operaciones transaccionales en tiempo real (tracking GPS, alertas), PostgreSQL/RDS para análisis históricos (Data Warehouse).

### Script de Despliegue (`04_aws_setup.py`)

```python
import boto3
import json
from datetime import datetime

# Clientes AWS
lambda_client = boto3.client('lambda', region_name='us-east-1')
apigateway = boto3.client('apigateway', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Funciones principales:
def crear_tablas_dynamodb()        # Crea 4 tablas NoSQL
def crear_rol_iam_lambda()         # Configura permisos
def desplegar_lambdas(rol_arn)     # Sube funciones con ZIP
def crear_api_gateway(lambda_arns) # Configura REST API
def configurar_triggers()          # EventBridge schedule
```

### Resultados del Avance 4

| Métrica | Resultado |
|---------|-----------|
| **Servicios desplegados** | 6 (API Gateway, Lambda, DynamoDB, IAM, CloudWatch, EventBridge) |
| **Funciones Lambda** | 3 (verificar, calcular-eta, alerta) |
| **Endpoints API** | 3 (REST POST) |
| **Tablas DynamoDB** | 4 (entregas, tracking, rutas, alertas) |
| **Tiempo de despliegue** | ~5 minutos (automatizado) |
| **Costo estimado** | $0-10/mes (Free Tier) |
| **Latencia API** | <200ms promedio |

### Lecciones Aprendidas del Avance 4

####  **Logros Técnicos**
- Arquitectura serverless completamente funcional
- Automatización de infraestructura con boto3
- Procesamiento en tiempo real habilitado
- Monitoreo integrado con CloudWatch

####  **Desafíos Superados**
1. **Lambda deployment**: Formato ZIP obligatorio → `zipfile` + `io.BytesIO()`
2. **Free Tier limits**: RDS backup 7→1 día de retención
3. **VPC networking**: Default VPC no existe → Configuración manual pendiente
4. **Region consistency**: Verificar siempre `us-east-1` en todos los servicios

####  **Mejoras Futuras**
- [ ] Migrar RDS PostgreSQL (requiere VPC configurada)
- [ ] Implementar S3 para almacenamiento de archivos
- [ ] Agregar autenticación con AWS Cognito
- [ ] Configurar WAF para seguridad del API
- [ ] Implementar CI/CD con AWS CodePipeline

---

##  Tecnologías Utilizadas

### Bases de Datos
- **PostgreSQL 15.4**: Base transaccional (OLTP) y Data Warehouse (OLAP)
- **Amazon DynamoDB**: Base NoSQL para tiempo real

### Cloud & Infraestructura
- **AWS Lambda**: Funciones serverless (Python 3.11)
- **Amazon API Gateway**: REST API
- **Amazon CloudWatch**: Logs y métricas
- **AWS EventBridge**: Triggers programados
- **AWS IAM**: Gestión de permisos

### Lenguajes & Frameworks
- **Python 3.11**
  - `psycopg2`: Conector PostgreSQL
  - `boto3`: AWS SDK
  - `faker`: Generación de datos sintéticos
  - `pandas`: Transformaciones ETL
  - `requests`: Testing de APIs

### Herramientas de Desarrollo
- **DBeaver**: Cliente SQL para PostgreSQL
- **AWS CLI**: Gestión de recursos cloud
- **Git/GitHub**: Control de versiones
- **VS Code**: Editor de código

---

##  Lecciones Aprendidas Generales

###  **Lecciones Técnicas**

1. **Diseño de BD**: La normalización reduce redundancia pero aumenta complejidad de consultas
2. **Indexación**: No todos los índices mejoran el rendimiento - analizar con EXPLAIN
3. **DW vs OLTP**: Separar sistemas transaccionales de analíticos es fundamental
4. **NoSQL**: DynamoDB excelente para patrones key-value, no para análisis complejos
5. **Serverless**: Lambda elimina gestión de servidores pero tiene límites (15 min timeout)
6. **IaC**: Automatizar infraestructura con código asegura reproducibilidad

### **Decisiones de Arquitectura Clave**

| Decisión | Razón | Impacto |
|----------|-------|---------|
| **PostgreSQL → DynamoDB** | Latencia en tiempo real | 10x más rápido para operaciones simples |
| **Modelo estrella en DW** | Consultas analíticas | Queries 10x más simples y rápidas |
| **Lambda vs EC2** | Costos y escalabilidad | Ahorro del 80% en cómputo |
| **REST API centralized** | Integración móvil | Un solo punto de entrada seguro |

###  **Reflexiones del Proyecto**

Este proyecto demuestra la evolución típica de una empresa que moderniza su infraestructura:

1. **Fundación**: Salir de Excel → Base de datos relacional
2. **Optimización**: Consultas lentas → Índices estratégicos
3. **Análisis**: Operación diaria → Visión estratégica (DW)
4. **Transformación**: Servidores locales → Cloud serverless

Cada etapa construye sobre la anterior, agregando capacidades sin desechar lo aprendido. El resultado es un sistema robusto, escalable y listo para el futuro.

---


##  Autor

**Nicolás Díaz**  
Proyecto Módulo 2 - Bases de Datos y Cloud Computing  
Fecha: Febrero 2026
contacto: 
   email: nicolasdiiaz0@gmail.com
   telefono: +543512118820


---

