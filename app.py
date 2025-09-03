# FIFO Batch Processing Engine - Flask Application
# This application provides a REST API for processing batches of data points
# with comprehensive metrics tracking, concurrent request handling, and monitoring capabilities

from flask import Flask, request, jsonify
import time
import random
import logging
from datetime import datetime
import threading
import json
import os

# Setup logging with detailed format for debugging and monitoring
# Logs are written to both file (service_logs.txt) and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('service_logs.txt'),  # Persistent log file
        logging.StreamHandler()  # Console output
    ]
)

# Initialize Flask application
app = Flask(__name__)

# =============================================================================
# GLOBAL VARIABLES AND THREAD SAFETY
# =============================================================================

# Concurrent request tracking - thread-safe counter for active requests
concurrent_requests = 0
concurrent_lock = threading.Lock()  # Protects concurrent_requests variable

# Metrics tracking variables - all protected by metrics_lock for thread safety
metrics_lock = threading.Lock()  # Protects all metrics variables
start_time = datetime.now()      # Server startup time for uptime calculation
total_requests = 0               # Total number of requests processed
successful_requests = 0          # Number of successfully processed requests
failed_requests = 0              # Number of failed requests
response_times = []              # List of response times for averaging
cycles = []                      # List of detailed request cycle information

# Log level control - allows dynamic log level changes via API
current_log_level = logging.INFO

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def save_metrics():
    """
    Save current metrics to extracted_metrics.json file.
    This function is thread-safe and called after each request to persist metrics.
    """
    with metrics_lock:  # Ensure thread safety when accessing metrics
        # Calculate average response time from stored response times
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        
        # Build metrics data structure matching the expected format
        metrics_data = {
            "start_time": start_time.isoformat(),           # Server startup time
            "total_requests": total_requests,               # Total requests processed
            "successful_requests": successful_requests,     # Successful requests
            "failed_requests": failed_requests,             # Failed requests
            "average_response_time": round(avg_response_time, 3),  # Average response time
            "cycles": cycles                                # Detailed cycle information
        }
        
        try:
            # Write metrics to JSON file with proper formatting
            with open('extracted_metrics.json', 'w') as f:
                json.dump(metrics_data, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save metrics: {e}")

def get_uptime_hours():
    """
    Calculate server uptime in hours.
    Returns the time elapsed since server startup.
    """
    return (datetime.now() - start_time).total_seconds() / 3600

def get_success_rate():
    """
    Calculate success rate percentage.
    Returns the percentage of successful requests out of total requests.
    """
    if total_requests == 0:
        return 0.0
    return (successful_requests / total_requests) * 100

def get_avg_points_per_minute():
    """
    Calculate average points processed per minute.
    This metric helps understand system throughput over time.
    """
    uptime_minutes = get_uptime_hours() * 60
    if uptime_minutes == 0:
        return 0.0
    return (successful_requests / uptime_minutes)

# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.route('/process', methods=['POST'])
def process_batch():
    """
    Main processing endpoint for FIFO batch processing.
    
    This endpoint:
    1. Accepts batches of data points for processing
    2. Tracks concurrent requests for load monitoring
    3. Simulates processing time (2 seconds per batch)
    4. Handles failures with 2% failure rate simulation
    5. Updates comprehensive metrics after each request
    6. Returns detailed response with processing information
    
    Request format:
    {
        "points": [
            {
                "id": "point_id",
                "data": {...}
            }
        ]
    }
    
    Response format:
    {
        "success": true/false,
        "processed_count": number,
        "processing_time": seconds,
        "total_time": seconds,
        "concurrent_requests": number
    }
    """
    global concurrent_requests, total_requests, successful_requests, failed_requests
    
    # Increment concurrent request counter (thread-safe)
    with concurrent_lock:
        concurrent_requests += 1
        current_concurrent = concurrent_requests
    
    # Increment total requests counter (thread-safe)
    with metrics_lock:
        total_requests += 1
    
    # Record request start time for response time calculation
    request_start_time = time.time()
    request_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    try:
        # Parse incoming JSON data
        data = request.get_json()
        points = data.get('points', [])
        
        # Log incoming batch with concurrent count for monitoring
        point_ids = [point['id'] for point in points]
        logging.info(f'[REQUEST START] {request_timestamp} - Received batch with {len(points)} points: {point_ids} - Concurrent requests: {current_concurrent}')
        
        # Record processing start time for accurate processing time measurement
        processing_start_time = time.time()
        
        # Simulate processing time - fixed 2-second processing for testing
        # In production, this would be replaced with actual data processing logic
        processing_time = 2.0
        time.sleep(processing_time)
        
        # Record processing end time
        processing_end_time = time.time()
        actual_processing_time = processing_end_time - processing_start_time
        
        # Simulate occasional failures (2% failure rate) for testing retry logic
        if random.random() < 0.02:
            response_time = time.time() - request_start_time
            response_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            # Update failed requests and response times (thread-safe)
            with metrics_lock:
                failed_requests += 1
                response_times.append(response_time)
                # Keep only last 1000 response times for memory efficiency
                if len(response_times) > 1000:
                    response_times.pop(0)
            
            logging.warning(f'[REQUEST FAILED] {response_timestamp} - Simulated failure for batch: {point_ids} - Total time: {response_time:.3f}s, Processing time: {actual_processing_time:.3f}s - Concurrent requests: {current_concurrent}')
            
            # Save metrics after failure to persist the failure count
            save_metrics()
            
            return jsonify({
                "success": False,
                "error": "Simulated processing failure",
                "concurrent_requests": current_concurrent
            }), 500
        
        # Log successful processing
        response_time = time.time() - request_start_time
        response_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # Update successful requests and response times (thread-safe)
        with metrics_lock:
            successful_requests += 1
            response_times.append(response_time)
            # Keep only last 1000 response times for memory efficiency
            if len(response_times) > 1000:
                response_times.pop(0)
            
            # Add detailed cycle information for comprehensive tracking
            cycle_info = {
                "timestamp": request_timestamp,           # When the request was received
                "points_processed": len(points),          # Number of points in this batch
                "processing_time": actual_processing_time, # Actual processing time
                "total_time": response_time,              # Total request time (including network)
                "concurrent_requests": current_concurrent # Concurrent requests at this time
            }
            cycles.append(cycle_info)
            # Keep only last 100 cycles for memory efficiency
            if len(cycles) > 100:
                cycles.pop(0)
        
        logging.info(f'[PROCESSING] Successfully processed point: {points[0]["id"]} in {actual_processing_time:.3f}s - Concurrent requests: {current_concurrent}')
        logging.info(f'[REQUEST SUCCESS] {response_timestamp} - Batch completed successfully: {point_ids} - Total time: {response_time:.3f}s, Processing time: {actual_processing_time:.3f}s - Concurrent requests: {current_concurrent}')
        
        # Save metrics after success to persist the success count
        save_metrics()
        
        return jsonify({
            "success": True,
            "processed_count": len(points),
            "processing_time": actual_processing_time,
            "total_time": response_time,
            "concurrent_requests": current_concurrent
        })
        
    except Exception as e:
        # Handle any unexpected errors during processing
        response_time = time.time() - request_start_time
        response_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        error_msg = f"Processing error: {str(e)}"
        
        # Update failed requests and response times (thread-safe)
        with metrics_lock:
            failed_requests += 1
            response_times.append(response_time)
            if len(response_times) > 1000:
                response_times.pop(0)
        
        logging.error(f'[REQUEST ERROR] {response_timestamp} - {error_msg} - Total time: {response_time:.3f}s - Concurrent requests: {current_concurrent}')
        
        # Save metrics after error to persist the error count
        save_metrics()
        
        return jsonify({
            "success": False,
            "error": error_msg,
            "concurrent_requests": current_concurrent
        }), 500
    
    finally:
        # Always decrement concurrent request counter, even if an error occurred
        with concurrent_lock:
            concurrent_requests -= 1
            final_concurrent = concurrent_requests
        end_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        logging.info(f'[REQUEST END] {end_timestamp} - Request completed - Remaining concurrent requests: {final_concurrent}')

@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint for monitoring system status.
    
    Returns basic health information including:
    - Server status (always "healthy" if responding)
    - Current timestamp
    - Number of concurrent requests
    
    Used by load balancers and monitoring systems.
    """
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
    """
    Detailed status endpoint with server metrics and capacity information.
    
    Returns comprehensive status including:
    - Current concurrent requests
    - Maximum thread capacity (50)
    - Utilization percentage
    - Server status (healthy/overloaded)
    
    Used for capacity planning and performance monitoring.
    """
    with concurrent_lock:
        current_concurrent = concurrent_requests
    
    return jsonify({
        "concurrent_requests": current_concurrent,
        "max_threads": 50,  # Matches Waitress server configuration
        "utilization_percent": round((current_concurrent / 50) * 100, 1),
        "timestamp": time.time(),
        "server_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        "status": "healthy" if current_concurrent <= 50 else "overloaded"
    })

@app.route('/metrics', methods=['GET'])
def get_metrics():
    """
    Comprehensive metrics endpoint for monitoring and analytics.
    
    Returns detailed metrics including:
    - Server uptime and performance statistics
    - Request counts (total, successful, failed)
    - Response time averages
    - Throughput metrics (points per minute)
    - Current system load
    
    This endpoint is used by monitoring tools like monitor_metrics.py
    and provides data for performance analysis and alerting.
    """
    with concurrent_lock:
        current_concurrent = concurrent_requests
    
    with metrics_lock:
        # Calculate average response time from stored response times
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        
        return jsonify({
            "uptime_hours": round(get_uptime_hours(), 2),           # Server uptime
            "total_points": successful_requests,                    # Total points processed
            "total_requests": total_requests,                       # Total requests received
            "successful_requests": successful_requests,             # Successful requests
            "failed_requests": failed_requests,                     # Failed requests
            "success_rate": round(get_success_rate(), 1),           # Success rate percentage
            "avg_response_time": round(avg_response_time, 3),       # Average response time
            "avg_points_per_minute": round(get_avg_points_per_minute(), 1),  # Throughput
            "current_concurrent": current_concurrent,               # Current concurrent requests
            "start_time": start_time.isoformat(),                   # Server start time
            "cycles_count": len(cycles),                            # Number of tracked cycles
            "timestamp": time.time(),                               # Current timestamp
            "server_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Human-readable time
        })

@app.route('/logs/level', methods=['POST'])
def set_log_level():
    """
    Dynamic log level control endpoint.
    
    Allows changing the server's log level at runtime without restarting.
    Useful for debugging and monitoring in production environments.
    
    Request format:
    {
        "level": "VERBOSE" | "INFO" | "METRICS_ONLY"
    }
    
    Log levels:
    - VERBOSE: DEBUG level - shows all log messages
    - INFO: INFO level - shows info, warning, and error messages
    - METRICS_ONLY: WARNING level - shows only warnings and errors
    """
    global current_log_level
    
    try:
        # Parse request data
        data = request.get_json()
        level = data.get('level', '').upper()
        
        # Map string levels to logging constants
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
        
        # Update logging level for all handlers
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
    """
    Reset all metrics counters to zero.
    
    This endpoint allows resetting all tracking metrics:
    - Request counters (total, successful, failed)
    - Response time history
    - Cycle information
    - Server start time (resets uptime)
    
    Useful for testing and performance benchmarking.
    After reset, all metrics start counting from zero.
    """
    global total_requests, successful_requests, failed_requests, response_times, cycles, start_time
    
    # Reset all metrics variables (thread-safe)
    with metrics_lock:
        total_requests = 0
        successful_requests = 0
        failed_requests = 0
        response_times = []
        cycles = []
        start_time = datetime.now()  # Reset start time for uptime calculation
    
    # Save reset metrics to file
    save_metrics()
    
    return jsonify({
        "success": True,
        "message": "Metrics reset successfully"
    })

# =============================================================================
# APPLICATION STARTUP
# =============================================================================

if __name__ == '__main__':
    # Initialize metrics file on startup to ensure it exists
    save_metrics()
    
    # Start Flask development server
    # Note: For production, use run_server.py with Waitress instead
    app.run(debug=True, host='0.0.0.0', port=8000)
