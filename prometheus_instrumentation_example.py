from prometheus_client import start_http_server, Summary, Counter
import random
import time

# Create a metric to track time spent and requests made.
counter = Counter('a_counter', 'This is just a counter')

# Decorate function with metric.

def process_request():
    print('Process function called')
    counter.inc()

if __name__ == '__main__':
    # Start up the server to expose the metrics.
    start_http_server(8000)
    print('Prometheus exporting on port 8000')
    
    # Do some work
    while True:
        process_request()
        time.sleep(1)