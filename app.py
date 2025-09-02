from flask import Flask, request, jsonify
import time
import random
import logging
from datetime import datetime
import threading

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

# Global counter for concurrent requests
concurrent_requests = 0
concurrent_lock = threading.Lock()

@app.route('/process', methods=['POST'])
def process_batch():
    """Process a batch of points with concurrent request tracking"""
    global concurrent_requests
    
    # Increment concurrent request counter
    with concurrent_lock:
        concurrent_requests += 1
        current_concurrent = concurrent_requests
    
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
        processing_time = 2.0  # Fixed 10-second processing time for testing
        time.sleep(processing_time)
        
        # Record processing end time
        processing_end_time = time.time()
        actual_processing_time = processing_end_time - processing_start_time
        
        # Simulate occasional failures (2% failure rate)
        if random.random() < 0.02:
            response_time = time.time() - request_start_time
            response_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            logging.warning(f'[REQUEST FAILED] {response_timestamp} - Simulated failure for batch: {point_ids} - Total time: {response_time:.3f}s, Processing time: {actual_processing_time:.3f}s - Concurrent requests: {current_concurrent}')
            return jsonify({
                "success": False,
                "error": "Simulated processing failure",
                "concurrent_requests": current_concurrent
            }), 500
        
        # Log successful processing
        response_time = time.time() - request_start_time
        response_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        logging.info(f'[PROCESSING] Successfully processed point: {points[0]["id"]} in {actual_processing_time:.3f}s - Concurrent requests: {current_concurrent}')
        logging.info(f'[REQUEST SUCCESS] {response_timestamp} - Batch completed successfully: {point_ids} - Total time: {response_time:.3f}s, Processing time: {actual_processing_time:.3f}s - Concurrent requests: {current_concurrent}')
        
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
        logging.error(f'[REQUEST ERROR] {response_timestamp} - {error_msg} - Total time: {response_time:.3f}s - Concurrent requests: {current_concurrent}')
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

# @app.route('/status', methods=['GET'])
# def get_status():
#     """Detailed status endpoint with server metrics"""
#     with concurrent_lock:
#         current_concurrent = concurrent_requests
    
#     return jsonify({
#         "concurrent_requests": current_concurrent,
#         "max_threads": 50,
#         "utilization_percent": round((current_concurrent / 10) * 100, 1),
#         "timestamp": time.time(),
#         "server_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
#         "status": "healthy" if current_concurrent <= 10 else "overloaded"
#     })

@app.route('/status', methods=['GET'])
def get_status():
    """Detailed status endpoint with server metrics"""
    with concurrent_lock:
        current_concurrent = concurrent_requests
    
    return jsonify({
        "concurrent_requests": current_concurrent,
        "max_threads": 50,  # Updated from 10 to 50
        "utilization_percent": round((current_concurrent / 50) * 100, 1),  # Updated from 10 to 50
        "timestamp": time.time(),
        "server_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        "status": "healthy" if current_concurrent <= 50 else "overloaded"  # Updated from 10 to 50
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)