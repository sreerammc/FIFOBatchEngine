# FIFO Batch Engine

A high-performance, flow-controlled FIFO batch processing system with request-level concurrency management.

## Features

- **Request-Level Flow Control**: Maintains exactly N concurrent requests at any time
- **High Performance**: 25+ points/second with 50 concurrent threads
- **Retry Logic**: Automatic retry for failed requests with configurable limits
- **Real-time Monitoring**: Built-in processing rate monitoring and logging
- **Scalable Architecture**: Easy to scale with multiple microservices

## Performance

- **Current Rate**: 25 points/second (50 threads, 2-second processing time)
- **Throughput**: 90,000 points/hour
- **Time for 35,000 points**: ~23 minutes

## Quick Start

### 1. Install Dependencies
`ash
pip install -r requirements.txt
`

### 2. Start the Server
`ash
python run_server.py
`

### 3. Run the Processor
`ash
python fifo_processor.py
`

## Configuration

### Server Configuration (run_server.py)
`python
serve(app, host='0.0.0.0', port=8000, threads=50)
`

### Client Configuration (fifo_processor.py)
`python
SERVER_THREADS = 50          # Match server capacity
MAX_CONCURRENT_REQUESTS = 50 # Maximum concurrent requests
REQUEST_TIMEOUT = 15         # Request timeout in seconds
MAX_RETRIES = 3              # Maximum retry attempts
TOTAL_POINTS = 35000         # Total points to process
`

## API Endpoints

### /process (POST)
Process a batch of points.

### /health (GET)
Health check with concurrent request count.

### /status (GET)
Detailed server status and metrics.

## Scaling

### Single Instance Performance
- **50 threads**: 25 points/second
- **100 threads**: 50 points/second (requires 16+ CPU cores, 32+ GB RAM)

### Multi-Instance Scaling
For 35,000 points in 15 minutes:
- **8 microservices** with 50 threads each
- **Total capacity**: 200 points/second
- **Processing time**: ~3 minutes

## Requirements

- Python 3.7+
- Flask
- aiohttp
- waitress
- asyncio

## License

MIT License
