--Query 1: Contar vehículos por tipo
--Resultado esperado: Lista simple con tipos de vehículo y cantidades.

explain ANALYZE
select 
	count(*),
	vehicle_type 
from vehicles
group by vehicle_type;

--Execution Time BEFORE: 0.151 ms
--Execution Time AFTER: 0.101 ms
--Tiempo antes: 1.151 ms
--Tiempo después: 0.101 ms
--Mejora: ((1.151-0.101 ms)/1.151) * 100% = 91.2 % de mejora 
--
--Query 2: Conductores con licencia próxima a vencer
--Resultado esperado: Lista de conductores que deben renovar licencia en 30 días.

--resultado de la query solicitada

explain ANALYZE
select 
	license_expiry,
	concat(first_name, ' ', last_name) as full_name,
	license_number from drivers
WHERE license_expiry < CURRENT_DATE + INTERVAL '30 days';

--Execution Time BEFORE: 0.159 ms
--Execution Time AFTER:  0.100 ms
--Tiempo antes: X ms
--Tiempo después: Y ms
--Mejora: ((0.159-0.100 ms)/0.159) * 100% = 37.1 % de mejora

--chequeo de licencias ya vencidas
select * from drivers
where license_expiry< current_date;

--Query 3: Total de viajes por estado
--Resultado esperado: Conteo simple por estado (in_progress, completed).

explain ANALYZE
select 
	count(*) as trip_count,
	status
from trips
group by status;

--Execution Time BEFORE: 22.945 ms
--Execution Time AFTER: 24.836 ms
--Tiempo antes: 22.945 ms ms
--Tiempo después: 24.836 ms ms
--Mejora: ((22.945-24.836)/22.945) * 100% ≈ -8.24 %

--Query 4: Total de entregas por ciudad (últimos 2 meses, 60 días)
--Resultado esperado: Ranking de ciudades con volumen de entregas y peso total.

explain ANALYZE
select
	count(d.delivery_id) as volumen_de_entregas,
	SUM(d.package_weight_kg) as peso_total,
	r.destination_city
from deliveries as d
inner join trips as t
on t.trip_id = d.trip_id 
inner join routes as r
on r.route_id = t.route_id
where d.delivered_datetime  >= (select max(departure_Datetime) from trips) - interval '2 months'
group by r.destination_city;

--Execution Time BEFORE: 89.182 ms
--Execution Time AFTER: 88.718 ms
--Tiempo antes: 89.182 ms
--Tiempo después: 88.718 ms
--Mejora: ((89.182 ms-88.718 ms)/89.182 ms) * 100% = 0.52 % de mejora


--Query 5: Conductores activos y carga de trabajo
--Resultado esperado: Lista con total de viajes por conductor activo.

explain ANALYZE
select
	count(t.trip_id) as total_viajes,
	concat(d.first_name, ' ', d.last_name) as full_name
from drivers as d
inner join trips as t
on t.driver_id = d.driver_id
where d.status = 'active'
group by d.first_name,d.last_name
order by total_viajes desc;

--Execution Time BEFORE: 67.477 ms
--Execution Time AFTER: 53.377 ms
--Tiempo antes: 67.477 ms
--Tiempo después: 53.377 ms
--Mejora: ((67.477 ms-53.377 ms)/67.477 ms) * 100% = 20.9 % de mejora

--Query 6: Promedio de entregas por conductor (6 meses)
--Resultado esperado: Métricas de productividad por conductor.

explain ANALYZE
select
	concat(dr.first_name, ' ', dr.last_name) as full_name,
	count(d.delivery_id) as total_deliveries,
	COUNT(d.delivery_id) / 6 AS promedio_mensual_entregas
from drivers as dr
left join trips as t
on t.driver_id = dr.driver_id
left join deliveries as d
on d.trip_id = t.trip_id
where d.delivered_datetime >= (select max(delivered_datetime) from deliveries) - INTERVAL '6 months'
group by dr.first_name, dr.last_name
order by total_deliveries desc;

--Execution Time BEFORE: 187.821 ms
-- Execution Time AFTER: 181.140 ms
--Tiempo antes: 187.821 ms
--Tiempo después: 181.140 ms
--Mejora: ((187.821 ms-181.140 ms)/187.821 ms) * 100% = 3.56 % de mejora

--Query 7: Rutas con mayor consumo de combustible
--Resultado esperado: Top 10 rutas con mayor consumo litros/100km.

explain ANALYZE
select
	concat(r.origin_City, ' a ', r.destination_city),
	sum(r.distance_km) as distancia_Recorrida,
	sum(t.fuel_consumed_liters) as combustible_consumido,
	(sum(r.distance_km) / sum(t.fuel_consumed_liters) * 100) as litros_100km
from routes as r
inner join trips as t
on t.route_id = r.route_id
group by r.origin_city, r.destination_city
order by litros_100km desc
limit 10;

--Execution Time BEFORE: 59.515 ms
-- Execution Time AFTER: 60.799 ms
--Tiempo antes: 59.515 ms
--Tiempo después: 60.799 ms
--Mejora: ((59.515 ms-60.799 ms)/59.515 ms) * 100% = -2.16 %

--Query 9: Costo de mantenimiento por kilómetro
--Resultado esperado: Costo por km para cada tipo de vehículo usando CTEs.

explain ANALYZE
with km_por_vehiculos as (
	select
		t.vehicle_id,
		SUM(r.distance_km) as total_km
	from trips as t
	inner join routes as r
	on r.route_id = t.route_id
	group by t.vehicle_id
	order by total_km desc
	),
	costo_mantenimiento_por_vehiculo as (
	select
		v.vehicle_id,
		v.vehicle_type,
		sum(m.cost) as costo_mantenimiento
	from maintenance m
	join vehicles as v
	on v.vehicle_id = m.vehicle_id
	group by v.vehicle_id
	)
select 
	c.vehicle_type,
	round(sum(c.costo_mantenimiento)/sum(k.total_km),2) as costo_mantenimiento_km,
	sum(k.total_km) as total_km,
	sum(c.costo_mantenimiento) as costo_mantenimiento_total
from costo_mantenimiento_por_vehiculo as c
join km_por_vehiculos as k
on k.vehicle_id = c.vehicle_id
group by c.vehicle_type ;

--Execution Time BEFORE: 44.345 ms
 --Execution Time AFTER: 43.537 ms
--Tiempo antes: 44.345 ms
--Tiempo después: 43.537 ms
--Mejora: (44.345 ms-43.537 ms)/44.345 ms) * 100% = 1.82 % de mejora

--Query 10: Ranking de conductores por eficiencia
--Resultado esperado: Top 20 conductores con ranking múltiple usando Window Functions (RANK).

explain ANALYZE
WITH driver_ranking AS (
  SELECT
    d.driver_id,
    d.first_name,
    d.last_name,
    SUM(del.package_weight_kg) AS total_kg,
    SUM(CASE WHEN del.delivered_datetime <= del.scheduled_datetime THEN 1 ELSE 0 END) AS entregas_a_tiempo,
    COUNT(*) AS entregas_totales
  FROM drivers as  d
  JOIN trips as t 
  		ON t.driver_id = d.driver_id
  JOIN deliveries as del 
  		ON del.trip_id = t.trip_id
  WHERE del.delivered_datetime IS NOT NULL
    	AND del.scheduled_datetime IS NOT NULL
  GROUP BY d.driver_id, d.first_name, d.last_name
)
SELECT
  RANK() OVER (
    ORDER BY (total_kg * (entregas_a_tiempo::numeric / nullif (entregas_totales, 0))) DESC
  ) AS rank,
  driver_id,
  concat(first_name, ' ' ,last_name),
  total_kg,
  entregas_a_tiempo,
  entregas_totales,
  ROUND(100.0 *(entregas_a_tiempo::numeric / nullif(entregas_totales, 0)), 2) AS porcent_a_tiempo
FROM driver_ranking
ORDER BY rank
LIMIT 20;

--Execution Time BEFORE: 195.524 ms
-- Execution Time AFTER: 179.212 ms
--Tiempo antes: 195.524 ms
--Tiempo después: 179.212 ms
--Mejora: (195.524 ms-179.212 ms)/195.524 ms) * 100% = 8.35 % de mejora

select count(distinct vehicle_id ) from trips 
where vehicle_id not in (select vehicle_id from vehicles );

select count(distinct route_id ) from routes
where route_id not in (select route_id from routes );

