from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
import requests
import json
import logging
import boto3
import os

application = Flask(__name__)
app = application

# Configuration
API_URL = "https://j2cosuv4ge.execute-api.us-east-1.amazonaws.com/default/x23389401-store-fetch-lambda"
USE_DUMMY_DATA = False  # Set to True only for testing without Lambda
REFRESH_INTERVAL = 5  # seconds

# SNS Configuration - Update these values
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:145075166360:x23389401-sns-fog-edge'
SNS_REGION = 'us-east-1'
EMAIL_RECIPIENT = 'x23389401@student.ncirl.ie'
# Initialize SNS client
try:
    sns_client = boto3.client('sns', region_name=SNS_REGION)
    print(f"SNS client initialized successfully")
except Exception as e:
    print(f"Failed to initialize SNS client: {e}")
    sns_client = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Critical thresholds for each sensor
CRITICAL_THRESHOLDS = {
    "temperature": {"min": 10, "max": 40, "warning_min": 15, "warning_max": 35},
    "humidity": {"min": 25, "max": 80, "warning_min": 30, "warning_max": 70},
    "cpu": {"max": 90, "warning_max": 80},
    "air_quality": {"max": 120, "warning_max": 100},
    "pressure": {"min": 970, "max": 1030, "warning_min": 980, "warning_max": 1020}
}

# Email tracking to avoid spam
last_email_sent_time = None
EMAIL_COOLDOWN_SECONDS = 300  # 5 minutes between emails

def is_sensor_critical(sensor_name, value):
    """Check if a single sensor is at critical level"""
    if value is None:
        return False
    
    thresholds = CRITICAL_THRESHOLDS.get(sensor_name, {})
    
    if sensor_name == "cpu":
        return value > thresholds.get('max', 90)
    elif sensor_name == "air_quality":
        return value > thresholds.get('max', 120)
    else:
        min_val = thresholds.get('min')
        max_val = thresholds.get('max')
        if min_val is not None and value < min_val:
            return True
        if max_val is not None and value > max_val:
            return True
    return False

def is_sensor_warning(sensor_name, value):
    """Check if a single sensor is at warning level"""
    if value is None:
        return False
    
    thresholds = CRITICAL_THRESHOLDS.get(sensor_name, {})
    
    if sensor_name == "cpu":
        warning_max = thresholds.get('warning_max', 80)
        critical_max = thresholds.get('max', 90)
        return warning_max < value <= critical_max
    elif sensor_name == "air_quality":
        warning_max = thresholds.get('warning_max', 100)
        critical_max = thresholds.get('max', 120)
        return warning_max < value <= critical_max
    else:
        warning_min = thresholds.get('warning_min')
        warning_max = thresholds.get('warning_max')
        critical_min = thresholds.get('min')
        critical_max = thresholds.get('max')
        
        if warning_min is not None and critical_min is not None:
            if critical_min < value <= warning_min:
                return True
        if warning_max is not None and critical_max is not None:
            if warning_max <= value < critical_max:
                return True
    return False

def determine_system_status(data):
    """
    Determine overall system status based on sensor data.
    CRITICAL: ALL sensors are at critical levels simultaneously
    WARNING: Any sensor is at warning level (but not all at critical)
    NORMAL: All sensors within normal range
    """
    if not data or len(data) == 0:
        return "unknown", "No data available", [], None
    
    # Get latest record (first one since sorted newest first)
    latest = data[0]
    
    # Track critical and warning sensors
    critical_sensors = []
    warning_sensors = []
    
    # Check each sensor
    sensors_to_check = ['temperature', 'humidity', 'cpu', 'air_quality', 'pressure']
    
    for sensor in sensors_to_check:
        value = latest.get(sensor)
        if value is not None:
            if is_sensor_critical(sensor, value):
                critical_sensors.append(sensor)
            elif is_sensor_warning(sensor, value):
                warning_sensors.append(sensor)
    
    # Determine status - CRITICAL only when ALL sensors are critical
    all_sensors_present = all(latest.get(s) is not None for s in sensors_to_check)
    
    if all_sensors_present and len(critical_sensors) == len(sensors_to_check):
        status = "critical"
        status_message = "CRITICAL - All sensors are at critical levels"
        issues = [f"{s} at critical level: {latest.get(s)}" for s in critical_sensors]
    elif len(critical_sensors) > 0:
        # Some sensors critical but not all
        status = "warning"
        status_message = f"WARNING - {len(critical_sensors)} sensor(s) at critical level, {len(warning_sensors)} at warning level"
        issues = [f"{s} at critical level: {latest.get(s)}" for s in critical_sensors]
        issues.extend([f"{s} at warning level: {latest.get(s)}" for s in warning_sensors])
    elif len(warning_sensors) > 0:
        status = "warning"
        status_message = f"WARNING - {len(warning_sensors)} sensor(s) approaching critical levels"
        issues = [f"{s} at warning level: {latest.get(s)}" for s in warning_sensors]
    else:
        status = "normal"
        status_message = "NORMAL - All sensors operating within normal range"
        issues = []
    
    return status, status_message, issues, latest

def send_critical_alert_email(data_summary):
    """Send SNS email alert when ALL sensors are at critical status"""
    global last_email_sent_time
    
    # Check cooldown to avoid spam
    current_time = datetime.now()
    if last_email_sent_time:
        time_diff = (current_time - last_email_sent_time).total_seconds()
        if time_diff < EMAIL_COOLDOWN_SECONDS:
            logger.info(f"Email cooldown active, skipping. Last sent {time_diff:.0f} seconds ago")
            return False
    
    if not sns_client:
        logger.error("SNS client not available")
        return False
    
    try:
        subject = f"[CRITICAL] Smart Factory Alert - ALL SENSORS CRITICAL - {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
        
        message = f"""
CRITICAL STATUS DETECTED IN SMART FACTORY
ALL SENSORS ARE AT CRITICAL LEVELS

Timestamp: {current_time.strftime('%Y-%m-%d %H:%M:%S')}

Current Sensor Readings (ALL CRITICAL):
--------------------------------------------------
Temperature: {data_summary.get('temperature', 'N/A')}C (Critical threshold: >40C or <10C)
Humidity: {data_summary.get('humidity', 'N/A')}% (Critical threshold: >80% or <25%)
CPU Usage: {data_summary.get('cpu', 'N/A')}% (Critical threshold: >90%)
Air Quality: {data_summary.get('air_quality', 'N/A')} (Critical threshold: >120)
Pressure: {data_summary.get('pressure', 'N/A')} hPa (Critical threshold: >1030 or <970 hPa)
Device ID: {data_summary.get('device_id', 'N/A')}
Record Count: {data_summary.get('total_records', 0)}
--------------------------------------------------

IMMEDIATE ACTION REQUIRED: All systems are operating at critical levels.
Please check the dashboard immediately.

Dashboard URL: http://localhost:5000
        """
        
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        
        last_email_sent_time = current_time
        logger.info(f"Critical alert email sent. Message ID: {response.get('MessageId')}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send SNS email: {e}")
        return False

def parse_timestamp(timestamp_str):
    """Parse timestamp from various formats"""
    if not timestamp_str:
        return None
    
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
        
        if not isinstance(data, list):
            data = [data] if data else []
        
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
    """Generate dummy data for testing"""
    import random
    data = []
    now = datetime.now()
    
    devices = ['FOG-EDGE-01', 'FOG-EDGE-02', 'SMART-SENSOR-03']
    
    for i in range(100):
        timestamp = now - timedelta(minutes=i*2)
        hour_factor = 1 + 0.3 * abs(12 - timestamp.hour) / 12
        
        # For testing critical status - occasionally make all sensors critical
        if i % 50 == 0:  # Every 50th record, make all sensors critical
            temp = 45.0
            humidity = 85.0
            cpu = 95.0
            air_quality = 130.0
            pressure = 1040.0
        else:
            temp = round(22 + 8 * hour_factor + random.uniform(-2, 2), 1)
            humidity = round(55 + 15 * (1 - hour_factor) + random.uniform(-5, 5), 1)
            cpu = round(45 + 25 * hour_factor + random.uniform(-10, 10), 1)
            air_quality = round(70 + 20 * hour_factor + random.uniform(-5, 5), 1)
            pressure = round(1013 + random.uniform(-8, 8), 1)
        
        data.append({
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'device_id': devices[i % len(devices)],
            'temperature': temp,
            'humidity': humidity,
            'cpu': cpu,
            'air_quality': air_quality,
            'pressure': pressure
        })
    
    return data

def fetch_data():
    """Main data fetcher"""
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
    """Get filtered data with error handling and status"""
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
    
    filtered = data
    
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
    
    if device_id:
        filtered = [d for d in filtered if d.get('device_id') == device_id]
    
    if limit and len(filtered) > limit:
        filtered = filtered[:limit]
    
    # Determine system status for each record
    critical_email_sent = False
    for record in filtered:
        status, status_message, issues, _ = determine_system_status([record])
        record['status'] = status
        record['status_message'] = status_message
        record['critical_sensors'] = issues
    
    # Get overall status from all data (for the dashboard)
    overall_status, overall_message, overall_issues, latest_data = determine_system_status(data)
    
    # Send critical alert ONLY when ALL sensors are critical
    if overall_status == "critical" and latest_data and not critical_email_sent:
        data_summary = {
            'temperature': latest_data.get('temperature'),
            'humidity': latest_data.get('humidity'),
            'cpu': latest_data.get('cpu'),
            'air_quality': latest_data.get('air_quality'),
            'pressure': latest_data.get('pressure'),
            'device_id': latest_data.get('device_id', 'Unknown'),
            'total_records': len(data)
        }
        email_sent = send_critical_alert_email(data_summary)
        critical_email_sent = True
    
    return jsonify({
        "success": True,
        "data": filtered,
        "total_records": len(filtered),
        "overall_status": overall_status,
        "overall_status_message": overall_message,
        "overall_issues": overall_issues,
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
    
    def get_stats_list(key):
        values = [d.get(key, 0) for d in data if d.get(key) is not None]
        return values if values else [0]
    
    temp_values = get_stats_list('temperature')
    humidity_values = get_stats_list('humidity')
    cpu_values = get_stats_list('cpu')
    air_values = get_stats_list('air_quality')
    pressure_values = get_stats_list('pressure')
    
    # Get overall status
    overall_status, overall_message, _, _ = determine_system_status(data)
    
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
        "total_records": len(data),
        "overall_status": overall_status,
        "overall_status_message": overall_message
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
    print(f"SNS Enabled: {sns_client is not None}")
    print("=" * 50)
    print("CRITICAL STATUS CONDITION: ALL sensors must be at critical levels")
    print("Critical thresholds: Temp>40C or <10C, Humidity>80% or <25%, CPU>90%, Air>120, Pressure>1030 or <970")
    print("=" * 50)
    print("Dashboard URL: http://localhost:5000")
    print("=" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=5000)