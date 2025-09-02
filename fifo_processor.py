import asyncio
import aiohttp
from collections import deque
import time
import csv
import threading
import json

MICROSERVICE_URL = "http://localhost:8000/process"
# Flow control configuration
SERVER_THREADS = 50          # Match server capacity (from run_server.py)
MAX_CONCURRENT_REQUESTS = SERVER_THREADS  # Maximum concurrent requests at any time
REQUEST_TIMEOUT = 15         # Timeout for individual requests (longer than server processing time)
MAX_RETRIES = 3              # Maximum retry attempts for failed requests
RETRY_DELAY = 2              # Delay between retries in seconds
TOTAL_POINTS = 35000         # Process all points from CSV
INTERVAL = 900               # 15 minutes (15 * 60 seconds)

# Load points from CSV file
def load_points_from_csv(filename, limit=None):
    """Load points from CSV file into a deque"""
    points = deque()
    print(f"Loading points from {filename}...")
    
    with open(filename, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        
        for row in reader:
            if limit and count >= limit:
                break
                
            # Create point data structure
            point_data = {
                "id": row["id"],
                "value": int(row["value"]),
                "category": row["category"],
                "priority": row["priority"],
                "timestamp": float(row["timestamp"]),
                "metadata": row["metadata"]
            }
            
            points.append(point_data)
            count += 1
            
            if count % 1000 == 0:
                print(f"Loaded {count} points...")
    
    print(f"Loaded {len(points)} points from CSV")
    return points

# Load points from CSV
points = load_points_from_csv("points_data.csv", limit=TOTAL_POINTS)
processed_count = 0
failed_count = 0

async def call_microservice_single(session, point, attempt=1):
    """Make HTTP call to microservice for a single point with retry logic."""
    try:
        # Prepare data for a single point
        batch_data = {
            "points": [{
                "id": point["id"],
                "data": {
                    "value": point["value"],
                    "category": point["category"],
                    "priority": point["priority"],
                    "timestamp": point["timestamp"],
                    "metadata": point["metadata"]
                },
                "attempts": attempt
            }]
        }
        
        print(f"Sending request for point: {point['id']} (attempt {attempt})")
        
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.post(MICROSERVICE_URL, json=batch_data, timeout=timeout) as resp:
            result = await resp.json()
            success = result.get("success", False)
            processed_count = result.get("processed_count", 0)
            
            if success:
                print(f"Response for {point['id']}: success={success}, processed={processed_count}")
                return True, processed_count, point, None
            else:
                error_msg = result.get("error", "Unknown error")
                print(f"Response for {point['id']}: failed - {error_msg}")
                return False, 0, point, error_msg
            
    except asyncio.TimeoutError:
        error_msg = f"Request timeout after {REQUEST_TIMEOUT}s"
        print(f"Timeout for point {point['id']}: {error_msg}")
        return False, 0, point, error_msg
    except Exception as e:
        error_msg = f"Request error: {str(e)}"
        print(f"Error for point {point['id']}: {error_msg}")
        return False, 0, point, error_msg

async def process_single_with_semaphore(session, semaphore, point, retry_queue, stats):
    """Process a single point with semaphore-based flow control."""
    async with semaphore:  # Acquire semaphore slot (max MAX_CONCURRENT_REQUESTS)
        print(f"Processing point {point['id']} (slot acquired)")
        
        # Make the request
        success, count, point, error_msg = await call_microservice_single(session, point)
        
        if success:
            stats['processed'] += count
            stats['successful'] += 1
            print(f"Point {point['id']} processed successfully")
        else:
            # Check if we should retry
            if point.get("retry_count", 0) < MAX_RETRIES:
                point["retry_count"] = point.get("retry_count", 0) + 1
                retry_queue.append(point)
                print(f"Scheduling retry {point['retry_count']} for point {point['id']}")
            else:
                stats['failed'] += 1
                print(f"Max retries exceeded for point {point['id']}, giving up")
        
        # Semaphore is automatically released when exiting this context

async def process_all_with_request_level_flow_control():
    """Process all points with request-level flow control using semaphore."""
    global processed_count, failed_count
    processed_count = 0
    failed_count = 0
    
    print(f"Starting request-level flow control processing of {len(points)} points")
    print(f"Configuration: Max concurrent requests={MAX_CONCURRENT_REQUESTS}, Timeout={REQUEST_TIMEOUT}s")
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    # Create a copy of points to work with
    remaining_points = deque(points)
    retry_queue = deque()
    
    # Statistics tracking
    stats = {'processed': 0, 'successful': 0, 'failed': 0}
    
    async with aiohttp.ClientSession() as session:
        # Create initial tasks (up to MAX_CONCURRENT_REQUESTS)
        tasks = []
        
        # Start initial batch of requests
        for _ in range(min(MAX_CONCURRENT_REQUESTS, len(remaining_points))):
            if remaining_points:
                point = remaining_points.popleft()
                task = asyncio.create_task(
                    process_single_with_semaphore(session, semaphore, point, retry_queue, stats)
                )
                tasks.append(task)
        
        print(f"Started {len(tasks)} initial requests")
        
        # Process remaining points and retries
        while remaining_points or retry_queue or tasks:
            # Wait for at least one task to complete
            if tasks:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = list(pending)
                
                # Start new requests for completed slots
                new_requests_needed = MAX_CONCURRENT_REQUESTS - len(tasks)
                
                for _ in range(new_requests_needed):
                    # Prioritize retry queue
                    if retry_queue:
                        point = retry_queue.popleft()
                        print(f"Starting retry for point {point['id']}")
                    elif remaining_points:
                        point = remaining_points.popleft()
                        print(f"Starting new request for point {point['id']}")
                    else:
                        break  # No more points to process
                    
                    # Create new task
                    task = asyncio.create_task(
                        process_single_with_semaphore(session, semaphore, point, retry_queue, stats)
                    )
                    tasks.append(task)
                
                print(f"Active requests: {len(tasks)}, Remaining: {len(remaining_points)}, Retries: {len(retry_queue)}")
    
    # Update global counters
    processed_count = stats['processed']
    failed_count = stats['failed']
    
    print(f"Processing completed!")
    print(f"Final stats: {processed_count} successful, {failed_count} failed")
    success_rate = (processed_count / (processed_count + failed_count) * 100) if (processed_count + failed_count) > 0 else 0
    print(f"Success rate: {success_rate:.1f}%")

async def scheduler():
    """Run every 15 minutes or after completion, whichever is longer."""
    while True:
        start = time.time()
        print("Starting cycle...")
        await process_all_with_request_level_flow_control()
        elapsed = time.time() - start
        wait_time = max(0, INTERVAL - elapsed)
        print(f"Cycle finished in {elapsed:.2f}s. Waiting {wait_time:.2f}s...")
        await asyncio.sleep(wait_time)

if __name__ == "__main__":
    asyncio.run(scheduler())