from waitress import serve
from app import app

print('Starting Waitress server with 50 threads for high-performance processing...')
print('Server will handle up to 50 concurrent requests simultaneously')
print('Optimized for high-throughput FIFO batch processing')

serve(app, host='0.0.0.0', port=8000, threads=50)
