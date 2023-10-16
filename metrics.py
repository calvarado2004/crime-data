from prometheus_client import Counter, Gauge, Summary

REQUESTS_TOTAL = Counter('crime_data_requests_total', 'Total crime data requests')
REQUESTS_IN_PROGRESS = Gauge('crime_data_requests_in_progress', 'Number of crime data requests in progress')
REQUESTS_LATENCY = Summary('crime_data_requests_latency_seconds', 'Latency of crime data requests')
RABBITMQ_MESSAGES_SENT = Counter('rabbitmq_messages_sent', 'Total messages sent to RabbitMQ')