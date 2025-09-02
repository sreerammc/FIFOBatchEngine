import asyncio
import aiohttp
from collections import deque
import time
import csv
import threading

MICROSERVICE_URL = "http://localhost:8000/process"
BATCH_SIZE = 10       # Number of individual requests to send concurrently
CONCURRENT_BATCHES = 5  # Number of concurrent batch workers
TOTAL_POINTS = 35000  # Process all points from CSV
INTERVAL = 900        # 15 minutes (15 * 60 seconds)

# Load points from CSV file
def load_points_from_csv(filename, limit=None):
    """Load points from CSV file into a deque"""
    points = deque()
    print(f"Loading points from {filename}...")
    
    with open(filename, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        
        for row in reader:
            if limit and count >= limit:
                break
                
            # Create point data structure
            point_data = {
                'id': row['id'],
                'value': int(row['value']),
                'category': row['category'],
                'priority': row['priority'],
                'timestamp': float(row['timestamp']),
                'metadata': row['metadata']
            }
            
            points.append(point_data)
            count += 1
            
            if count % 1000 == 0:
                print(f"Loaded {count} points...")
    
    print(f"✅ Loaded {len(points)} points from CSV")
    return points

# Load points from CSV
points = load_points_from_csv('points_data.csv', limit=TOTAL_POINTS)
processed_count = 0

async def call_microservice_single(session, point):
    """Make HTTP call to microservice for a single point."""
    try:
        # Prepare data for a single point
        batch_data = {
            "points": [{
                "id": point['id'],
                "data": {
                    "value": point['value'],
                    "category": point['category'],
                    "priority": point['priority'],
                    "timestamp": point['timestamp'],
                    "metadata": point['metadata']
                },
                "attempts": 0
            }]
        }
        
        print(f"�� Sending request for point: {point['id']}")
        
        async with session.post(MICROSERVICE_URL, json=batch_data, timeout=10) as resp:
            result = await resp.json()
            success = result.get("success", False)
            processed_count = result.get("processed_count", 0)
            
            print(f"✅ Response for {point['id']}: success={success}, processed={processed_count}")
            return success, processed_count, point
            
    except Exception as e:
        print(f"❌ Error for point {point['id']}: {e}")
        return False, 0, point

async def process_batch_worker_individual_responses(session, semaphore, worker_id):
    """Worker that processes individual responses as they come back."""
    global processed_count
    
    while points:
        # Get a batch of points from the queue
        batch_points = []
        for _ in range(BATCH_SIZE):
            if not points:
                break
            batch_points.append(points.popleft())
        
        if not batch_points:
            break
            
        async with semaphore:
            print(f"\n🔄 Worker {worker_id}: Starting batch of {len(batch_points)} points")
            
            # Create individual tasks for each point
            tasks = []
            for point in batch_points:
                task = asyncio.create_task(call_microservice_single(session, point))
                tasks.append(task)
            
            # Handle responses individually as they complete
            completed_count = 0
            for task in asyncio.as_completed(tasks):
                try:
                    success, count, point = await task
                    completed_count += 1
                    
                    # Handle individual response immediately
                    if success:
                        print(f"✅ Worker {worker_id}: Point {point['id']} processed successfully")
                        processed_count += count
                    else:
                        print(f"❌ Worker {worker_id}: Point {point['id']} failed")
                    
                    # Put point back to queue immediately after processing
                    points.append(point)
                    
                    print(f"📊 Worker {worker_id}: Completed {completed_count}/{len(batch_points)} points, total processed: {processed_count}")
                    
                except Exception as e:
                    print(f"💥 Worker {worker_id}: Task failed with exception: {e}")
                    completed_count += 1
            
            print(f"🏁 Worker {worker_id}: Batch completed, queue size: {len(points)}")

async def process_all():
    """Process all points with limited concurrency."""
    global processed_count
    processed_count = 0
    
    print(f"Starting to process {TOTAL_POINTS} points with {CONCURRENT_BATCHES} concurrent batch workers...")
    print(f"Each batch will send {BATCH_SIZE} individual requests with individual response handling")
    
    semaphore = asyncio.Semaphore(CONCURRENT_BATCHES)
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(process_batch_worker_individual_responses(session, semaphore, i))
                 for i in range(CONCURRENT_BATCHES)]
        await asyncio.gather(*tasks)

async def scheduler():
    """Run every 30 seconds or after completion, whichever is longer."""
    while True:
        start = time.time()
        print("Starting cycle...")
        await process_all()
        elapsed = time.time() - start
        wait_time = max(0, INTERVAL - elapsed)
        print(f"Cycle finished in {elapsed:.2f}s. Waiting {wait_time:.2f}s...")
        await asyncio.sleep(wait_time)

if __name__ == "__main__":
    asyncio.run(scheduler())
