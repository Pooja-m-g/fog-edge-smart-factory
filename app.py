from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
import requests
import json
import logging

app = Flask(__name__)

# Configuration
API_URL = "https://j2cosuv4ge.execute-api.us-east-1.amazonaws.com/default/x23389401-store-fetch-lambda"
USE_DUMMY_DATA = False  # Set to True only for testing without Lambda
REFRESH_INTERVAL = 5  # seconds

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_timestamp(timestamp_str):
    """Parse timestamp from various formats"""
    if not timestamp_str:
        return None
    
    # Common timestamp formats
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%SZ',
        '%d/%m/%Y %H:%M:%S',
        '%m/%d/%Y %H:%M:%S',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except:
            continue
    
    # Try to parse ISO format
    try:
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except:
        pass
    
    return None

def fetch_real_data():
    """Fetch real data from Lambda API"""
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Handle different response formats
        if isinstance(data, dict):
            if 'data' in data:
                data = data['data']
            elif 'items' in data:
                data = data['items']
            elif 'body' in data:
                try:
                    data = json.loads(data['body'])
                except:
                    data = data['body']
        
        # Ensure data is a list
        if not isinstance(data, list):
            data = [data] if data else []
        
        # Sort by timestamp (newest first)
        data.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        logger.info(f"Fetched {len(data)} records from Lambda")
        return data, None
        
    except requests.exceptions.Timeout:
        error_msg = "Lambda API timeout - No response received"
        logger.error(error_msg)
        return [], error_msg
    except requests.exceptions.ConnectionError:
        error_msg = "Cannot connect to Lambda API - Check network"
        logger.error(error_msg)
        return [], error_msg
    except requests.exceptions.HTTPError as e:
        error_msg = f"Lambda API HTTP error: {e.response.status_code}"
        logger.error(error_msg)
        return [], error_msg
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON response from Lambda: {str(e)}"
        logger.error(error_msg)
        return [], error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return [], error_msg

def generate_dummy_data():
    """Generate dummy data for testing (only when USE_DUMMY_DATA=True)"""
    import random
    data = []
    now = datetime.now()
    
    devices = ['FOG-EDGE-01', 'FOG-EDGE-02', 'SMART-SENSOR-03']
    
    for i in range(100):
        timestamp = now - timedelta(minutes=i*2)
        
        # Simulate realistic variations
        hour_factor = 1 + 0.3 * abs(12 - timestamp.hour) / 12
        
        data.append({
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'device_id': devices[i % len(devices)],
            'temperature': round(22 + 8 * hour_factor + random.uniform(-2, 2), 1),
            'humidity': round(55 + 15 * (1 - hour_factor) + random.uniform(-5, 5), 1),
            'cpu': round(45 + 25 * hour_factor + random.uniform(-10, 10), 1),
            'air_quality': round(70 + 20 * hour_factor + random.uniform(-5, 5), 1),
            'pressure': round(1013 + random.uniform(-8, 8), 1)
        })
    
    return data

def fetch_data():
    """Main data fetcher - uses real or dummy based on config"""
    if USE_DUMMY_DATA:
        logger.info("Using dummy data mode")
        return generate_dummy_data(), None
    else:
        return fetch_real_data()

@app.route("/")
def index():
    return render_template("dashboard.html", refresh_interval=REFRESH_INTERVAL)

@app.route("/api/data")
def get_data():
    """Get filtered data with error handling"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    device_id = request.args.get('device_id')
    limit = request.args.get('limit', 100, type=int)
    
    data, error = fetch_data()
    
    if error:
        return jsonify({
            "success": False,
            "error": error,
            "data": [],
            "message": "Unable to fetch sensor data"
        }), 200
    
    if not data:
        return jsonify({
            "success": True,
            "data": [],
            "message": "No sensor data available",
            "total_records": 0
        })
    
    # Apply filters
    filtered = data
    
    # Date filter - parse timestamps for proper comparison
    if start_date:
        try:
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            filtered = [d for d in filtered if parse_timestamp(d.get('timestamp', '')) and parse_timestamp(d.get('timestamp', '')) >= start_dt]
        except Exception as e:
            logger.warning(f"Start date filter error: {e}")
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            filtered = [d for d in filtered if parse_timestamp(d.get('timestamp', '')) and parse_timestamp(d.get('timestamp', '')) <= end_dt]
        except Exception as e:
            logger.warning(f"End date filter error: {e}")
    
    # Device filter
    if device_id:
        filtered = [d for d in filtered if d.get('device_id') == device_id]
    
    # Apply limit
    if limit and len(filtered) > limit:
        filtered = filtered[:limit]
    
    return jsonify({
        "success": True,
        "data": filtered,
        "total_records": len(filtered),
        "timestamp": datetime.now().isoformat()
    })

@app.route("/api/stats")
def get_stats():
    """Get statistical summary of data"""
    data, error = fetch_data()
    
    if error or not data:
        return jsonify({
            "success": False,
            "error": error or "No data available",
            "stats": {}
        })
    
    # Calculate statistics
    def get_stats_list(key):
        values = [d.get(key, 0) for d in data if d.get(key) is not None]
        return values if values else [0]
    
    temp_values = get_stats_list('temperature')
    humidity_values = get_stats_list('humidity')
    cpu_values = get_stats_list('cpu')
    air_values = get_stats_list('air_quality')
    pressure_values = get_stats_list('pressure')
    
    stats = {
        "temperature": {
            "current": temp_values[0] if temp_values else 0,
            "avg": round(sum(temp_values) / len(temp_values), 2) if temp_values else 0,
            "min": min(temp_values) if temp_values else 0,
            "max": max(temp_values) if temp_values else 0
        },
        "humidity": {
            "current": humidity_values[0] if humidity_values else 0,
            "avg": round(sum(humidity_values) / len(humidity_values), 2) if humidity_values else 0,
            "min": min(humidity_values) if humidity_values else 0,
            "max": max(humidity_values) if humidity_values else 0
        },
        "cpu": {
            "current": cpu_values[0] if cpu_values else 0,
            "avg": round(sum(cpu_values) / len(cpu_values), 2) if cpu_values else 0,
            "min": min(cpu_values) if cpu_values else 0,
            "max": max(cpu_values) if cpu_values else 0
        },
        "air_quality": {
            "current": air_values[0] if air_values else 0,
            "avg": round(sum(air_values) / len(air_values), 2) if air_values else 0,
            "min": min(air_values) if air_values else 0,
            "max": max(air_values) if air_values else 0
        },
        "pressure": {
            "current": pressure_values[0] if pressure_values else 0,
            "avg": round(sum(pressure_values) / len(pressure_values), 2) if pressure_values else 0,
            "min": min(pressure_values) if pressure_values else 0,
            "max": max(pressure_values) if pressure_values else 0
        },
        "total_records": len(data)
    }
    
    return jsonify({"success": True, "stats": stats})

@app.route("/api/devices")
def get_devices():
    """Get list of unique devices"""
    data, error = fetch_data()
    
    if error or not data:
        return jsonify({"success": True, "devices": []})
    
    devices = list(set([d.get('device_id', 'Unknown') for d in data if d.get('device_id')]))
    return jsonify({"success": True, "devices": devices})

@app.route("/api/alerts")
def get_alerts():
    """Get alerts based on thresholds"""
    data, error = fetch_data()
    
    if error or not data:
        return jsonify({"success": True, "alerts": [], "error": error})
    
    thresholds = {
        "temperature": {"min": 15, "max": 35},
        "humidity": {"min": 30, "max": 70},
        "cpu": {"max": 80},
        "air_quality": {"max": 100},
        "pressure": {"min": 980, "max": 1020}
    }
    
    alerts = []
    for record in data[:50]:
        alerts_list = []
        
        temp = record.get('temperature')
        if temp:
            if temp > thresholds['temperature']['max']:
                alerts_list.append(f"High Temperature: {temp}C")
            elif temp < thresholds['temperature']['min']:
                alerts_list.append(f"Low Temperature: {temp}C")
        
        humidity = record.get('humidity')
        if humidity:
            if humidity > thresholds['humidity']['max']:
                alerts_list.append(f"High Humidity: {humidity}%")
            elif humidity < thresholds['humidity']['min']:
                alerts_list.append(f"Low Humidity: {humidity}%")
        
        cpu = record.get('cpu')
        if cpu and cpu > thresholds['cpu']['max']:
            alerts_list.append(f"High CPU Usage: {cpu}%")
        
        air = record.get('air_quality')
        if air and air > thresholds['air_quality']['max']:
            alerts_list.append(f"Poor Air Quality: {air}")
        
        pressure = record.get('pressure')
        if pressure:
            if pressure > thresholds['pressure']['max']:
                alerts_list.append(f"High Pressure: {pressure} hPa")
            elif pressure < thresholds['pressure']['min']:
                alerts_list.append(f"Low Pressure: {pressure} hPa")
        
        if alerts_list:
            alerts.append({
                "timestamp": record.get('timestamp'),
                "device_id": record.get('device_id', 'Unknown'),
                "alerts": alerts_list
            })
    
    return jsonify({"success": True, "alerts": alerts[:20]})

@app.route("/api/status")
def get_status():
    """Get API connection status"""
    data, error = fetch_data()
    
    return jsonify({
        "success": error is None,
        "error": error,
        "data_available": len(data) > 0 if data else False,
        "record_count": len(data) if data else 0,
        "timestamp": datetime.now().isoformat()
    })

if __name__ == "__main__":
    print("=" * 50)
    print("Smart Factory Fog Edge Dashboard")
    print("=" * 50)
    print(f"Data Source: {'DUMMY DATA' if USE_DUMMY_DATA else 'LAMBDA API'}")
    if not USE_DUMMY_DATA:
        print(f"API URL: {API_URL}")
    print(f"Refresh Interval: {REFRESH_INTERVAL} seconds")
    print("=" * 50)
    print("Dashboard URL: http://localhost:5000")
    print("=" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=5000)