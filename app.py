from flask import Flask, request, jsonify
import time
import random
import logging
from datetime import datetime
import threading
import json
import os

# Setup logging with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('service_logs.txt'),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)

# Global metrics tracking
concurrent_requests = 0
concurrent_lock = threading.Lock()

# Metrics tracking variables
metrics_lock = threading.Lock()
start_time = datetime.now()
total_requests = 0
successful_requests = 0
failed_requests = 0
response_times = []
cycles = []

# Log level control
current_log_level = logging.INFO

def save_metrics():
    """Save current metrics to extracted_metrics.json"""
    with metrics_lock:
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        
        metrics_data = {
            "start_time": start_time.isoformat(),
            "total_requests": total_requests,
            "successful_requests": successful_requests,
            "failed_requests": failed_requests,
            "average_response_time": round(avg_response_time, 3),
            "cycles": cycles
        }
        
        try:
            with open('extracted_metrics.json', 'w') as f:
                json.dump(metrics_data, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save metrics: {e}")

def get_uptime_hours():
    """Calculate server uptime in hours"""
    return (datetime.now() - start_time).total_seconds() / 3600

def get_success_rate():
    """Calculate success rate percentage"""
    if total_requests == 0:
        return 0.0
    return (successful_requests / total_requests) * 100

def get_avg_points_per_minute():
    """Calculate average points processed per minute"""
    uptime_minutes = get_uptime_hours() * 60
    if uptime_minutes == 0:
        return 0.0
    return (successful_requests / uptime_minutes)

@app.route('/process', methods=['POST'])
def process_batch():
    """Process a batch of points with concurrent request tracking"""
    global concurrent_requests, total_requests, successful_requests, failed_requests
    
    # Increment concurrent request counter
    with concurrent_lock:
        concurrent_requests += 1
        current_concurrent = concurrent_requests
    
    # Increment total requests
    with metrics_lock:
        total_requests += 1
    
    request_start_time = time.time()
    request_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    try:
        data = request.get_json()
        points = data.get('points', [])
        
        # Log incoming batch with concurrent count
        point_ids = [point['id'] for point in points]
        logging.info(f'[REQUEST START] {request_timestamp} - Received batch with {len(points)} points: {point_ids} - Concurrent requests: {current_concurrent}')
        
        # Record processing start time
        processing_start_time = time.time()
        
        # Simulate processing time
        processing_time = 2.0  # Fixed 2-second processing time for testing
        time.sleep(processing_time)
        
        # Record processing end time
        processing_end_time = time.time()
        actual_processing_time = processing_end_time - processing_start_time
        
        # Simulate occasional failures (2% failure rate)
        if random.random() < 0.02:
            response_time = time.time() - request_start_time
            response_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            # Update failed requests and response times
            with metrics_lock:
                failed_requests += 1
                response_times.append(response_time)
                # Keep only last 1000 response times for memory efficiency
                if len(response_times) > 1000:
                    response_times.pop(0)
            
            logging.warning(f'[REQUEST FAILED] {response_timestamp} - Simulated failure for batch: {point_ids} - Total time: {response_time:.3f}s, Processing time: {actual_processing_time:.3f}s - Concurrent requests: {current_concurrent}')
            
            # Save metrics after failure
            save_metrics()
            
            return jsonify({
                "success": False,
                "error": "Simulated processing failure",
                "concurrent_requests": current_concurrent
            }), 500
        
        # Log successful processing
        response_time = time.time() - request_start_time
        response_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # Update successful requests and response times
        with metrics_lock:
            successful_requests += 1
            response_times.append(response_time)
            # Keep only last 1000 response times for memory efficiency
            if len(response_times) > 1000:
                response_times.pop(0)
            
            # Add cycle information
            cycle_info = {
                "timestamp": request_timestamp,
                "points_processed": len(points),
                "processing_time": actual_processing_time,
                "total_time": response_time,
                "concurrent_requests": current_concurrent
            }
            cycles.append(cycle_info)
            # Keep only last 100 cycles for memory efficiency
            if len(cycles) > 100:
                cycles.pop(0)
        
        logging.info(f'[PROCESSING] Successfully processed point: {points[0]["id"]} in {actual_processing_time:.3f}s - Concurrent requests: {current_concurrent}')
        logging.info(f'[REQUEST SUCCESS] {response_timestamp} - Batch completed successfully: {point_ids} - Total time: {response_time:.3f}s, Processing time: {actual_processing_time:.3f}s - Concurrent requests: {current_concurrent}')
        
        # Save metrics after success
        save_metrics()
        
        return jsonify({
            "success": True,
            "processed_count": len(points),
            "processing_time": actual_processing_time,
            "total_time": response_time,
            "concurrent_requests": current_concurrent
        })
        
    except Exception as e:
        response_time = time.time() - request_start_time
        response_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        error_msg = f"Processing error: {str(e)}"
        
        # Update failed requests and response times
        with metrics_lock:
            failed_requests += 1
            response_times.append(response_time)
            if len(response_times) > 1000:
                response_times.pop(0)
        
        logging.error(f'[REQUEST ERROR] {response_timestamp} - {error_msg} - Total time: {response_time:.3f}s - Concurrent requests: {current_concurrent}')
        
        # Save metrics after error
        save_metrics()
        
        return jsonify({
            "success": False,
            "error": error_msg,
            "concurrent_requests": current_concurrent
        }), 500
    
    finally:
        # Decrement concurrent request counter
        with concurrent_lock:
            concurrent_requests -= 1
            final_concurrent = concurrent_requests
        end_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        logging.info(f'[REQUEST END] {end_timestamp} - Request completed - Remaining concurrent requests: {final_concurrent}')

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint with concurrent request count"""
    with concurrent_lock:
        current_concurrent = concurrent_requests
    
    return jsonify({
        "status": "healthy",
        "timestamp": time.time(),
        "server_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        "concurrent_requests": current_concurrent
    })

@app.route('/status', methods=['GET'])
def get_status():
    """Detailed status endpoint with server metrics"""
    with concurrent_lock:
        current_concurrent = concurrent_requests
    
    return jsonify({
        "concurrent_requests": current_concurrent,
        "max_threads": 50,
        "utilization_percent": round((current_concurrent / 50) * 100, 1),
        "timestamp": time.time(),
        "server_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        "status": "healthy" if current_concurrent <= 50 else "overloaded"
    })

@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Comprehensive metrics endpoint for monitoring"""
    with concurrent_lock:
        current_concurrent = concurrent_requests
    
    with metrics_lock:
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        
        return jsonify({
            "uptime_hours": round(get_uptime_hours(), 2),
            "total_points": successful_requests,  # Assuming 1 point per successful request
            "total_requests": total_requests,
            "successful_requests": successful_requests,
            "failed_requests": failed_requests,
            "success_rate": round(get_success_rate(), 1),
            "avg_response_time": round(avg_response_time, 3),
            "avg_points_per_minute": round(get_avg_points_per_minute(), 1),
            "current_concurrent": current_concurrent,
            "start_time": start_time.isoformat(),
            "cycles_count": len(cycles),
            "timestamp": time.time(),
            "server_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        })

@app.route('/logs/level', methods=['POST'])
def set_log_level():
    """Set server log level"""
    global current_log_level
    
    try:
        data = request.get_json()
        level = data.get('level', '').upper()
        
        if level == 'VERBOSE':
            current_log_level = logging.DEBUG
        elif level == 'INFO':
            current_log_level = logging.INFO
        elif level == 'METRICS_ONLY':
            current_log_level = logging.WARNING
        else:
            return jsonify({
                "success": False,
                "error": "Invalid log level. Use: VERBOSE, INFO, or METRICS_ONLY"
            }), 400
        
        # Update logging level
        logging.getLogger().setLevel(current_log_level)
        for handler in logging.getLogger().handlers:
            handler.setLevel(current_log_level)
        
        return jsonify({
            "success": True,
            "current_level": level,
            "message": f"Log level set to {level}"
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Failed to set log level: {str(e)}"
        }), 500

@app.route('/metrics/reset', methods=['POST'])
def reset_metrics():
    """Reset all metrics counters"""
    global total_requests, successful_requests, failed_requests, response_times, cycles, start_time
    
    with metrics_lock:
        total_requests = 0
        successful_requests = 0
        failed_requests = 0
        response_times = []
        cycles = []
        start_time = datetime.now()
    
    # Save reset metrics
    save_metrics()
    
    return jsonify({
        "success": True,
        "message": "Metrics reset successfully"
    })

if __name__ == '__main__':
    # Initialize metrics file on startup
    save_metrics()
    app.run(debug=True, host='0.0.0.0', port=8000)