-- =====================================================
-- FLEETLOGIX DATABASE SETUP
-- Sistema de Gestión de Transporte y Logística
-- =====================================================

-- 1. Crear las tablas del modelo relacional

-- Tabla 1: vehicles (vehículos de la flota)
CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    license_plate VARCHAR(20) UNIQUE NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg DECIMAL(10,2),
    fuel_type VARCHAR(20),
    acquisition_date DATE,
    status VARCHAR(20) DEFAULT 'active'
);

-- Tabla 2: drivers (conductores)
CREATE TABLE drivers (
    driver_id SERIAL PRIMARY KEY,
    employee_code VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    license_number VARCHAR(50) UNIQUE NOT NULL,
    license_expiry DATE,
    phone VARCHAR(20),
    hire_date DATE,
    status VARCHAR(20) DEFAULT 'active'
);

-- Tabla 3: routes (rutas predefinidas)
CREATE TABLE routes (
    route_id SERIAL PRIMARY KEY,
    route_code VARCHAR(20) UNIQUE NOT NULL,
    origin_city VARCHAR(100) NOT NULL,
    destination_city VARCHAR(100) NOT NULL,
    distance_km DECIMAL(10,2),
    estimated_duration_hours DECIMAL(5,2),
    toll_cost DECIMAL(10,2) DEFAULT 0
);

-- Tabla 4: trips (viajes realizados)
CREATE TABLE trips (
    trip_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(vehicle_id),
    driver_id INTEGER REFERENCES drivers(driver_id),
    route_id INTEGER REFERENCES routes(route_id),
    departure_datetime TIMESTAMP NOT NULL,
    arrival_datetime TIMESTAMP,
    fuel_consumed_liters DECIMAL(10,2),
    total_weight_kg DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'in_progress'
);

-- Tabla 5: deliveries (entregas individuales)
CREATE TABLE deliveries (
    delivery_id SERIAL PRIMARY KEY,
    trip_id INTEGER REFERENCES trips(trip_id),
    tracking_number VARCHAR(50) UNIQUE NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    delivery_address TEXT NOT NULL,
    package_weight_kg DECIMAL(10,2),
    scheduled_datetime TIMESTAMP,
    delivered_datetime TIMESTAMP,
    delivery_status VARCHAR(20) DEFAULT 'pending',
    recipient_signature BOOLEAN DEFAULT FALSE
);


-- Tabla 6: maintenance (mantenimientos de vehículos)
CREATE TABLE maintenance (
    maintenance_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(vehicle_id),
    maintenance_date DATE NOT NULL,
    maintenance_type VARCHAR(50) NOT NULL,
    description TEXT,
    cost DECIMAL(10,2),
    next_maintenance_date DATE,
    performed_by VARCHAR(200)
);

-- 2. Crear índices básicos proporcionados
CREATE INDEX idx_trips_departure ON trips(departure_datetime);
CREATE INDEX idx_deliveries_status ON deliveries(delivery_status);
CREATE INDEX idx_vehicles_status ON vehicles(status);

-- 3. Agregar comentarios a las tablas para documentación
COMMENT ON TABLE vehicles IS 'Registro de vehículos de la flota de FleetLogix';
COMMENT ON TABLE drivers IS 'Información de conductores empleados';
COMMENT ON TABLE routes IS 'Rutas predefinidas entre ciudades';
COMMENT ON TABLE trips IS 'Registro de viajes realizados';
COMMENT ON TABLE deliveries IS 'Entregas individuales asociadas a cada viaje';
COMMENT ON TABLE maintenance IS 'Historial de mantenimiento de vehículos';

-- 4. Verificar la creación de las tablas
SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = 'public' 
     AND table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- 5. Verificar las relaciones (foreign keys)
SELECT
    tc.table_name AS tabla_origen,
    kcu.column_name AS columna_origen,
    ccu.table_name AS tabla_referencia,
    ccu.column_name AS columna_referencia
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
AND tc.table_schema = 'public';

-- 6. Verificar índices creados
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

--Asegurar consistencia temporal (arrival > departure)
SELECT COUNT(*) AS invalid_trips
FROM trips
WHERE arrival_datetime IS NOT NULL
  AND arrival_datetime < departure_datetime;

-- Trips sin vehículo válido
SELECT COUNT(*) 
FROM trips t
LEFT JOIN vehicles v ON t.vehicle_id = v.vehicle_id
WHERE v.vehicle_id IS NULL;

-- Trips sin conductor válido
SELECT COUNT(*)
FROM trips t
LEFT JOIN drivers d ON t.driver_id = d.driver_id
WHERE d.driver_id IS NULL;

-- Trips sin ruta válida
SELECT COUNT(*)
FROM trips t
LEFT JOIN routes r ON t.route_id = r.route_id
WHERE r.route_id IS NULL;

-- Deliveries sin trip válido
SELECT COUNT(*)
FROM deliveries d
LEFT JOIN trips t ON d.trip_id = t.trip_id
WHERE t.trip_id IS NULL;

-- Maintenance sin vehículo válido
SELECT COUNT(*)
FROM maintenance m
LEFT JOIN vehicles v ON m.vehicle_id = v.vehicle_id
WHERE v.vehicle_id IS NULL;
----


SELECT
            t1.delivery_id, t1.trip_id, t1.tracking_number,
            t2.vehicle_id, t2.driver_id, t2.route_id,
            t1.customer_name, t3.destination_city,
            t1.package_weight_kg, t3.distance_km, t3.toll_cost, t2.fuel_consumed_liters,
            t1.scheduled_datetime, t1.delivered_datetime, t2.departure_datetime, t2.arrival_datetime,
            t1.delivery_status, t1.recipient_signature
        FROM deliveries t1
        JOIN trips t2 ON t1.trip_id = t2.trip_id
        JOIN routes t3 ON t3.route_id = t2.route_id
        WHERE t1.delivery_status = 'delivered'
        AND t1.scheduled_datetime >= (SELECT MAX(scheduled_datetime) FROM deliveries) - INTERVAL '1 day'
        ORDER BY t1.scheduled_datetime desc;