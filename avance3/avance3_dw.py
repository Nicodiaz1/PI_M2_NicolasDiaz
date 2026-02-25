"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma y Carga en Snowflake
Ejecución diaria automatizada
"""

import psycopg2
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import schedule
import time
import json
import os
from typing import Dict, List, Tuple
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuración de conexiones desde .env
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'database': os.getenv('POSTGRES_DB', 'fleetlogix'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'port': int(os.getenv('POSTGRES_PORT', '5432'))
}

SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

class FleetLogixETL:
    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': 0
        }
    
    def connect_databases(self):
        """Establecer conexiones con PostgreSQL y Snowflake"""
        try:
            # PostgreSQL
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info(" Conectado a PostgreSQL")
            
            # Snowflake
            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info(" Conectado a Snowflake")
            
            return True
        except Exception as e:
            logging.error(f" Error en conexión: {e}")
            return False
    
    def populate_dim_date(self):
        """Poblar dimensión de fechas"""
        logging.info(" Poblando dim_date...")
        cursor = self.sf_conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM dim_date")
            count = cursor.fetchone()[0]
            if count > 0:
                logging.info(f" dim_date ya tiene {count} registros")
                return
            
            dates = pd.date_range('2020-01-01', '2030-12-31', freq='D')
            date_data = []
            for date in dates:
                date_data.append((
                    int(date.strftime('%Y%m%d')), date.date(), date.weekday(), 
                    date.strftime('%A'), date.day, date.timetuple().tm_yday, 
                    date.isocalendar()[1], date.month, date.strftime('%B'), 
                    (date.month-1)//3+1, date.year, date.weekday()>=5, False, 
                    (date.month-1)//3+1, date.year
                ))
            
            cursor.executemany("""
                INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, 
                day_of_month, day_of_year, week_of_year, month_num, month_name, 
                quarter, year, is_weekend, is_holiday, fiscal_quarter, fiscal_year)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, date_data)
            self.sf_conn.commit()
            logging.info(f" dim_date poblada con {len(date_data)} registros")
        except Exception as e:
            logging.error(f" Error en dim_date: {e}")
    
    def populate_dim_time(self):
        """Poblar dimensión de tiempo"""
        cursor = self.sf_conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM dim_time")
            count = cursor.fetchone()[0]
            if count > 0:
                logging.info(f" dim_time ya tiene {count} registros")
                return
            
            time_data = []
            for hour in range(24):
                for minute in [0, 30]:
                    time_key = hour * 100 + minute
                    shift = 'Turno 1' if 6<=hour<14 else ('Turno 2' if 14<=hour<22 else 'Turno 3')
                    time_data.append((
                        time_key, hour, minute, 0, 
                        'Mañana' if 6<=hour<12 else ('Tarde' if 12<=hour<20 else 'Noche'),
                        f"{hour:02d}:{minute:02d}", 
                        f"{hour%12 if hour%12!=0 else 12}:{minute:02d}",
                        'PM' if hour>=12 else 'AM', 8<=hour<18, shift
                    ))
            
            cursor.executemany("""
                INSERT INTO dim_time (time_key, hour, minute, second, 
                time_of_day, hour_24, hour_12, am_pm, is_business_hour, shift)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, time_data)
            self.sf_conn.commit()
            logging.info(f" dim_time poblada con {len(time_data)} registros")
        except Exception as e:
            logging.error(f" Error en dim_time: {e}")
    
    def extract_daily_data(self) -> pd.DataFrame:
        """Extraer datos del día anterior de PostgreSQL"""
        logging.info(" Iniciando extracción de datos...")
        
        query = """
        SELECT
            --identificadores de deliveries
            t1.delivery_id,
            t1.trip_id,
            t1.tracking_number,
            t2.vehicle_id,
            t2.driver_id,
            t2.route_id,
            t1.customer_name,
            t3.destination_city,
            t1.package_weight_kg,
            t3.distance_km,
            t3.toll_cost,
            t2.fuel_consumed_liters,
            t1.scheduled_datetime,
            t1.delivered_datetime,
            t2.departure_datetime,
            t2.arrival_datetime,
            t1.delivery_status,
            t1.recipient_signature
        FROM deliveries as t1
        JOIN trips as t2
            ON t1.trip_id = t2.trip_id
        JOIN routes as t3
            ON t3.route_id = t2.route_id
        WHERE delivery_status ='delivered'
        AND scheduled_datetime >= (select max(scheduled_datetime) from deliveries) - INTERVAL '1 day'
        """

        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f" Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f" Error en extracción: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar datos para el modelo dimensional"""
        logging.info(" Iniciando transformación de datos...")
        
        try:
            # Calcular métricas
            df['delivery_time_minutes'] = (
                (pd.to_datetime(df['delivered_datetime']) - 
                 pd.to_datetime(df['scheduled_datetime'])).dt.total_seconds() / 60
            ).round(2)
            
            df['delay_minutes'] = df['delivery_time_minutes'].apply(
                lambda x: max(0, x) if x > 0 else 0
            )
            
            df['is_on_time'] = df['delay_minutes'] <= 30
            
            # Calcular entregas por hora
            df['trip_duration_hours'] = (
                (pd.to_datetime(df['arrival_datetime']) - 
                 pd.to_datetime(df['departure_datetime'])).dt.total_seconds() / 3600
            ).round(2)
            
            # Agrupar entregas por trip para calcular entregas/hora
            deliveries_per_trip = df.groupby('trip_id').size()
            df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)
            df['deliveries_per_hour'] = (
                df['deliveries_in_trip'] / df['trip_duration_hours']
            ).round(2)
            
            # Eficiencia de combustible
            df['fuel_efficiency_km_per_liter'] = (
                df['distance_km'] / df['fuel_consumed_liters']
            ).round(2)
            
            # Costo estimado por entrega
            df['cost_per_delivery'] = (
                (df['fuel_consumed_liters'] * 5000 + df['toll_cost']) / 
                df['deliveries_in_trip']
            ).round(2)
            
            # Revenue estimado (ejemplo: $20,000 base + $500 por kg)
            df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)
            
            # Validaciones de calidad
            # No permitir tiempos negativos
            ##df = df[df['delivery_time_minutes'] >= 0]
            ##No se usara ya que hay delays en entregas con tiempos negativos.
            
            # No permitir pesos fuera de rango
            df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)]
            
            # Manejar cambios históricos (SCD Type 2 para conductor/vehículo)
            df['valid_from'] = pd.to_datetime(df['scheduled_datetime']).dt.date
            df['valid_to'] = pd.to_datetime('9999-12-31')
            df['is_current'] = True
            
            self.metrics['records_transformed'] = len(df)
            logging.info(f" Transformados {len(df)} registros")
            
            return df
            
        except Exception as e:
            logging.error(f" Error en transformación: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def load_dimensions(self, df: pd.DataFrame):
        """Cargar o actualizar dimensiones en Snowflake"""
        logging.info(" Cargando dimensiones...")
        
        cursor = self.sf_conn.cursor()
        pg_cursor = self.pg_conn.cursor()
        
        try:
            # Cargar dim_vehicle (solo primera vez)
            cursor.execute("SELECT COUNT(*) FROM dim_vehicle")
            if cursor.fetchone()[0] == 0:
                logging.info(" Cargando dim_vehicle...")
                pg_cursor.execute("SELECT vehicle_id, license_plate, vehicle_type, capacity_kg, fuel_type, acquisition_date, status FROM vehicles")
                vehicles = pg_cursor.fetchall()
                cursor.executemany("""
                    INSERT INTO dim_vehicle (vehicle_id, license_plate, vehicle_type, capacity_kg, 
                    fuel_type, acquisition_date, status, valid_from, valid_to, is_current) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s, CURRENT_DATE(), '9999-12-31'::DATE, TRUE)
                """, vehicles)
                logging.info(f" Cargados {len(vehicles)} vehículos")
            
            # Cargar dim_driver (solo primera vez)
            cursor.execute("SELECT COUNT(*) FROM dim_driver")
            if cursor.fetchone()[0] == 0:
                logging.info(" Cargando dim_driver...")
                pg_cursor.execute("""SELECT driver_id, employee_code, first_name || ' ' || last_name, license_number, 
                    license_expiry, phone, hire_date, status, 
                    EXTRACT(YEAR FROM AGE(CURRENT_DATE, hire_date))*12 + EXTRACT(MONTH FROM AGE(CURRENT_DATE, hire_date)) as experience_months
                    FROM drivers""")
                drivers = [(d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[8], d[7], 'Regular') for d in pg_cursor.fetchall()]
                cursor.executemany("""
                    INSERT INTO dim_driver (driver_id, employee_code, full_name, license_number, 
                    license_expiry, phone, hire_date, experience_months, status, performance_category, 
                    valid_from, valid_to, is_current) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, CURRENT_DATE(), '9999-12-31'::DATE, TRUE)
                """, drivers)
                logging.info(f" Cargados {len(drivers)} conductores")
            
            # Cargar dim_route (solo primera vez)
            cursor.execute("SELECT COUNT(*) FROM dim_route")
            if cursor.fetchone()[0] == 0:
                logging.info(" Cargando dim_route...")
                pg_cursor.execute("SELECT route_id, route_code, origin_city, destination_city, distance_km, estimated_duration_hours, toll_cost FROM routes")
                routes = []
                for r in pg_cursor.fetchall():
                    difficulty = 'Fácil' if r[4] < 100 else ('Medio' if r[4] < 300 else 'Difícil')
                    route_type = 'Urbana' if r[4] < 50 else ('Interurbana' if r[4] < 200 else 'Rural')
                    routes.append((r[0], r[1], r[2], r[3], r[4], r[5], r[6], difficulty, route_type))
                cursor.executemany("""
                    INSERT INTO dim_route (route_id, route_code, origin_city, destination_city, 
                    distance_km, estimated_duration_hours, toll_cost, difficulty_level, route_type) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, routes)
                logging.info(f" Cargadas {len(routes)} rutas")
            
            # dim_customer - cargar solo nuevos clientes
            logging.info(" Cargando dim_customer...")
            customers = df[['customer_name', 'destination_city']].drop_duplicates()
            
            # Obtener clientes existentes
            cursor.execute("SELECT customer_name FROM dim_customer")
            existing_customers = {row[0] for row in cursor.fetchall()}
            
            # Filtrar solo nuevos
            new_customers = []
            for _, row in customers.iterrows():
                if row['customer_name'] not in existing_customers:
                    new_customers.append((row['customer_name'], 'Individual', row['destination_city'], 0, 'Regular'))
            
            # Insertar en batch
            if new_customers:
                cursor.executemany("""
                    INSERT INTO dim_customer (customer_name, customer_type, city, 
                    first_delivery_date, total_deliveries, customer_category)
                    VALUES (%s, %s, %s, CURRENT_DATE(), %s, %s)
                """, new_customers)
                logging.info(f" Cargados {len(new_customers)} clientes nuevos")
            else:
                logging.info(" No hay clientes nuevos")
            
            self.sf_conn.commit()
            logging.info(" Dimensiones cargadas")
            
        except Exception as e:
            logging.error(f" Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
    
    def load_facts(self, df: pd.DataFrame):
        """Cargar hechos en Snowflake"""
        logging.info(" Cargando tabla de hechos...")
        
        cursor = self.sf_conn.cursor()
        
        try:
            # Obtener TODOS los keys de dimensiones en memoria (1 query por dimensión)
            cursor.execute("SELECT vehicle_id, vehicle_key FROM dim_vehicle")
            vehicle_keys = {vid: vkey for vid, vkey in cursor.fetchall()}
            
            cursor.execute("SELECT driver_id, driver_key FROM dim_driver WHERE is_current = TRUE")
            driver_keys = {did: dkey for did, dkey in cursor.fetchall()}
            
            cursor.execute("SELECT route_id, route_key FROM dim_route")
            route_keys = {rid: rkey for rid, rkey in cursor.fetchall()}
            
            cursor.execute("SELECT customer_name, customer_key FROM dim_customer")
            customer_keys = {cname: ckey for cname, ckey in cursor.fetchall()}
            
            # Preparar datos para inserción
            fact_data = []
            for _, row in df.iterrows():
                # Obtener keys de dimensiones
                date_key = int(pd.to_datetime(row['scheduled_datetime']).strftime('%Y%m%d'))
                
                # Redondear a slots de 30 minutos (0 o 30)
                sched_dt = pd.to_datetime(row['scheduled_datetime'])
                sched_minute = 0 if sched_dt.minute < 30 else 30
                scheduled_time_key = sched_dt.hour * 100 + sched_minute
                
                deliv_dt = pd.to_datetime(row['delivered_datetime'])
                deliv_minute = 0 if deliv_dt.minute < 30 else 30
                delivered_time_key = deliv_dt.hour * 100 + deliv_minute
                
                # Buscar claves surrogadas en memoria
                vehicle_key = vehicle_keys.get(row['vehicle_id'])
                driver_key = driver_keys.get(row['driver_id'])
                route_key = route_keys.get(row['route_id'])
                customer_key = customer_keys.get(row['customer_name'])
                
                fact_data.append((
                    date_key,
                    scheduled_time_key,
                    delivered_time_key,
                    vehicle_key,
                    driver_key,
                    route_key,
                    customer_key,
                    row['delivery_id'],
                    row['trip_id'],
                    row['tracking_number'],
                    row['package_weight_kg'],
                    row['distance_km'],
                    row['fuel_consumed_liters'],
                    row['delivery_time_minutes'],
                    row['delay_minutes'],
                    row['deliveries_per_hour'],
                    row['fuel_efficiency_km_per_liter'],
                    row['cost_per_delivery'],
                    row['revenue_per_delivery'],
                    row['is_on_time'],
                    False,  # is_damaged
                    row['recipient_signature'],
                    row['delivery_status'],
                    self.batch_id
                ))
            
            # Insertar en batch
            cursor.executemany("""
                INSERT INTO fact_deliveries (
                    date_key, scheduled_time_key, delivered_time_key,
                    vehicle_key, driver_key, route_key, customer_key,
                    delivery_id, trip_id, tracking_number,
                    package_weight_kg, distance_km, fuel_consumed_liters,
                    delivery_time_minutes, delay_minutes, deliveries_per_hour,
                    fuel_efficiency_km_per_liter, cost_per_delivery, revenue_per_delivery,
                    is_on_time, is_damaged, has_signature, delivery_status,
                    etl_batch_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, fact_data)
            
            self.sf_conn.commit()
            self.metrics['records_loaded'] = len(fact_data)
            logging.info(f" Cargados {len(fact_data)} registros en fact_deliveries")
            
        except Exception as e:
            logging.error(f" Error cargando hechos: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
    
    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        start_time = datetime.now()
        logging.info(f" Iniciando ETL - Batch ID: {self.batch_id}")
        
        try:
            # Conectar
            if not self.connect_databases():
                return
            
            # Poblar dimensiones de tiempo (solo primera vez)
            self.populate_dim_date()
            self.populate_dim_time()
            
            # ETL
            df = self.extract_daily_data()
            if not df.empty:
                df_transformed = self.transform_data(df)
                if not df_transformed.empty:
                    self.load_dimensions(df_transformed)
                    self.load_facts(df_transformed)
            
            # Calcular totales para reportes (opcional)
            # self._calculate_daily_totals()
            
            # Cerrar conexiones
            self.close_connections()
            
            # Log final
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f" ETL completado en {duration:.2f} segundos")
            logging.info(f" Métricas: {json.dumps(self.metrics, indent=2)}")
            
        except Exception as e:
            logging.error(f" Error fatal en ETL: {e}")
            self.metrics['errors'] += 1
            self.close_connections()
    
    def _calculate_daily_totals(self):
        """Pre-calcular totales para reportes rápidos"""
        cursor = self.sf_conn.cursor()
        
        try:
            # Insertar totales del día
            cursor.execute("""
                INSERT INTO daily_delivery_totals 
                SELECT
                    DATE_KEY,
                    COUNT(*) as TOTAL_DELIVERIES,
                    SUM(CASE WHEN IS_ON_TIME THEN 1 ELSE 0 END) as ON_TIME_DELIVERIES,
                    SUM(CASE WHEN NOT IS_ON_TIME THEN 1 ELSE 0 END) as TOTAL_DELAYED,
                    AVG(DELAY_MINUTES) as AVG_DELAY_MINUTES,
                    SUM(REVENUE_PER_DELIVERY) as TOTAL_REVENUE,
                    SUM(COST_PER_DELIVERY) as TOTAL_COST,
                    CURRENT_TIMESTAMP() as ETL_TIMESTAMP
                FROM fact_deliveries
                WHERE ETL_BATCH_ID = %s
                GROUP BY DATE_KEY
            """, (self.batch_id,))
            
            self.sf_conn.commit()
            logging.info(" Totales diarios calculados")
            
        except Exception as e:
            logging.error(f" Error calculando totales: {e}")
    
    def close_connections(self):
        """Cerrar conexiones a bases de datos"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()
        logging.info(" Conexiones cerradas")

def job():
    """Función para programar con schedule"""
    etl = FleetLogixETL()
    etl.run_etl()

def main():
    """Función principal - Automatización diaria"""
    logging.info(" Pipeline ETL FleetLogix iniciado")
    
    # Programar ejecución diaria a las 2:00 AM
    schedule.every().day.at("02:00").do(job)
    
    logging.info(" ETL programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("Presiona Ctrl+C para detener")
    
    # Ejecutar una vez al inicio (para pruebas)
    job()
    
    # Loop infinito esperando la hora programada
    while True:
        schedule.run_pending()
        time.sleep(60)  # Verificar cada minuto

if __name__ == "__main__":
    main()