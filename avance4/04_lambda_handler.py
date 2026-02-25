"""
FleetLogix - Funciones Lambda para AWS
3 funciones simples para procesamiento básico
"""

import json
import boto3
from datetime import datetime, timedelta, timezone
from decimal import Decimal

# Clientes AWS
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

# =====================================================
# HELPER FUNCTIONS
# =====================================================

def convert_floats(obj):
    """
    Convierte recursivamente floats a Decimal para DynamoDB.
    DynamoDB no acepta floats nativos de Python.
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats(i) for i in obj]
    return obj

def parse_event_body(event):
    """
    Parsea el body del event que viene desde API Gateway.
    API Gateway envía body como string JSON en proxy integration.
    """
    # Si viene desde API Gateway con proxy integration
    if 'body' in event:
        body = event['body']
        if isinstance(body, str):
            return json.loads(body)
        return body
    # Si viene directo (test manual)
    return event

# =====================================================
# LAMBDA 1: Verificar si una entrega se completó
# =====================================================
def lambda_verificar_entrega(event, context):
    """
    Verifica si una entrega se completó comparando con DynamoDB
    """
    
    # Parsear body desde API Gateway
    body = parse_event_body(event)
    
    # Obtener datos del evento
    delivery_id = body.get('delivery_id')
    tracking_number = body.get('tracking_number')
    
    if not delivery_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'delivery_id es requerido'}, default=str)
        }
    
    # Conectar a tabla DynamoDB
    table = dynamodb.Table('deliveries_status')
    
    try:
        # Buscar entrega
        response = table.get_item(
            Key={'delivery_id': delivery_id}
        )
        
        if 'Item' in response:
            item = response['Item']
            is_completed = item.get('status') == 'delivered'
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'delivery_id': delivery_id,
                    'tracking_number': item.get('tracking_number'),
                    'is_completed': is_completed,
                    'status': item.get('status'),
                    'delivered_datetime': str(item.get('delivered_datetime', ''))
                }, default=str)
            }
        else:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'error': 'Entrega no encontrada',
                    'delivery_id': delivery_id
                }, default=str)
            }
            
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            }, default=str)
        }

# =====================================================
# LAMBDA 2: Calcular tiempo estimado de llegada (ETA)
# =====================================================
def lambda_calcular_eta(event, context):
    """
    Calcula ETA basado en ubicación actual y destino
    """
    
    # Parsear body desde API Gateway
    body = parse_event_body(event)
    
    # Obtener datos del evento
    vehicle_id = body.get('vehicle_id')
    current_location = body.get('current_location')  # {lat, lon}
    destination = body.get('destination')  # {lat, lon}
    current_speed_kmh = body.get('current_speed_kmh', 60)
    
    if not all([vehicle_id, current_location, destination]):
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Faltan parámetros requeridos'}, default=str)
        }
    
    try:
        # Calcular distancia simple (Haversine simplificado)
        lat_diff = abs(destination['lat'] - current_location['lat'])
        lon_diff = abs(destination['lon'] - current_location['lon'])
        
        # Aproximación simple: 111 km por grado
        distance_km = ((lat_diff ** 2 + lon_diff ** 2) ** 0.5) * 111
        
        # Calcular tiempo
        if current_speed_kmh > 0:
            hours = distance_km / current_speed_kmh
            eta = datetime.now(timezone.utc) + timedelta(hours=hours)
        else:
            eta = None
        
        # Guardar en DynamoDB (convertir floats a Decimal)
        table = dynamodb.Table('vehicle_tracking')
        item = {
            'vehicle_id': vehicle_id,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'current_location': current_location,
            'destination': destination,
            'distance_remaining_km': round(distance_km, 2),
            'eta': eta.isoformat() if eta else None,
            'current_speed_kmh': current_speed_kmh
        }
        table.put_item(Item=convert_floats(item))
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'vehicle_id': vehicle_id,
                'distance_remaining_km': round(distance_km, 2),
                'eta': eta.isoformat() if eta else 'No disponible',
                'estimated_minutes': round(hours * 60) if eta else None
            }, default=str)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            }, default=str)
        }

# =====================================================
# LAMBDA 3: Enviar alerta si camión se desvía de ruta
# =====================================================
def lambda_alerta_desvio(event, context):
    """
    Detecta desvíos de ruta y envía alertas
    """
    
    # Parsear body desde API Gateway
    body = parse_event_body(event)
    
    # Obtener datos del evento
    vehicle_id = body.get('vehicle_id')
    current_location = body.get('current_location')  # {lat, lon}
    route_id = body.get('route_id')
    driver_id = body.get('driver_id')
    
    if not all([vehicle_id, current_location, route_id]):
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Faltan parámetros requeridos'}, default=str)
        }
    
    try:
        # Obtener ruta esperada de DynamoDB
        table = dynamodb.Table('routes_waypoints')
        response = table.get_item(
            Key={'route_id': route_id}
        )
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Ruta no encontrada'}, default=str)
            }
        
        waypoints = response['Item'].get('waypoints', [])
        
        # Calcular distancia mínima a la ruta
        min_distance = float('inf')
        for waypoint in waypoints:
            lat_diff = abs(waypoint['lat'] - current_location['lat'])
            lon_diff = abs(waypoint['lon'] - current_location['lon'])
            distance = ((lat_diff ** 2 + lon_diff ** 2) ** 0.5) * 111  # km
            min_distance = min(min_distance, distance)
        
        # Umbral de desvío: 5 km
        DEVIATION_THRESHOLD_KM = 5
        is_deviated = min_distance > DEVIATION_THRESHOLD_KM
        
        if is_deviated:
            # Guardar alerta en DynamoDB (convertir floats a Decimal)
            alerts_table = dynamodb.Table('alerts_history')
            alert_item = {
                'vehicle_id': vehicle_id,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'driver_id': driver_id,
                'route_id': route_id,
                'deviation_km': round(min_distance, 2),
                'current_location': current_location,
                'alert_type': 'ROUTE_DEVIATION'
            }
            alerts_table.put_item(Item=convert_floats(alert_item))
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'vehicle_id': vehicle_id,
                'is_deviated': is_deviated,
                'deviation_km': round(min_distance, 2),
                'alert_sent': is_deviated,
                'threshold_km': DEVIATION_THRESHOLD_KM
            }, default=str)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            }, default=str)
        }
