# Kafka Python Project

A Python-based Apache Kafka project demonstrating producer and consumer implementations with both basic JSON messaging and Avro schema serialization.

## Project Structure

```
kafka-python/
├── producer.py              # Basic JSON producer
├── consumer.py              # Basic JSON consumer
├── productor_pro.py         # Advanced Avro producer
├── consumidor_pro.py        # Advanced Avro consumer
├── productor_seguro.py      # Secure Avro producer with SASL auth
├── consumidor_seguro.py     # Secure Avro consumer with SASL auth
├── docker-compose.yml       # Docker services configuration
├── kafka_server_jaas.conf   # SASL/PLAIN authentication config
├── requirements.txt         # Python dependencies
├── test_producer.py         # Producer tests
├── test_consumer.py         # Consumer tests
└── README.md               # This file
```

## Prerequisites

- Docker & Docker Compose
- Python 3.8+
- Virtual environment (`venv`)

## Installation

### 1. Clone and Setup Virtual Environment

```bash
cd /Users/rafael/DevProjects/kafka-python
python -m venv .venv
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install confluent-kafka
```

### 3. Start Kafka Infrastructure

```bash
docker-compose up -d
```

This starts:
- **Kafka Broker** (9092)
- **Schema Registry** (8081)
- **Control Center** (9021) - Web UI for monitoring

> Note: Control Center must connect to the broker and Schema Registry using internal Docker hostnames (`broker:29092` and `schema-registry:8081`). The current `docker-compose.yml` includes those settings for the container.

## Usage

### Basic JSON Messaging

#### Producer
```bash
python producer.py
```
Sends timestamped messages to `mi-topico` topic.

#### Consumer
```bash
python consumer.py
```
Consumes messages from `mi-topico` topic in real-time.

### Advanced Avro Serialization

#### Producer (with Schema Registry)
```bash
python productor_pro.py
```
Sends structured user data serialized with Avro schema to `usuarios-eventos` topic.

#### Consumer (with Avro Deserialization)
```bash
python consumidor_pro.py
```
Consumes and deserializes Avro messages from `usuarios-eventos` topic.

### Secure Avro Messaging (SASL_PLAINTEXT)

#### Secure Producer
```bash
python productor_seguro.py
```
Sends Avro user events to `usuarios-eventos` using SASL/PLAIN authentication.

#### Secure Consumer
```bash
python consumidor_seguro.py
```
Consumes and deserializes Avro messages from `usuarios-eventos` using SASL/PLAIN authentication.

## Configuration Details

### Kafka Broker
- **Bootstrap Server**: `localhost:9092`
- **Protocol**: SASL_PLAINTEXT
- **Authentication**: SASL/PLAIN mechanism
- **KRaft Mode**: Yes (Controller + Broker combined)

### Schema Registry
- **URL**: `http://localhost:8081`
- **Purpose**: Manages Avro schemas for message serialization

### Authentication
Default credentials are configured in `kafka_server_jaas.conf`:
- Update credentials as needed for production use

## Troubleshooting

### "Invalid magic byte" Error
This occurs when:
- Producer sends non-Avro messages to a topic expecting Avro format
- **Solution**: Ensure producer uses `AvroSerializer` for `usuarios-eventos` topic

### Connection Refused
- Ensure Docker containers are running: `docker-compose ps`
- Check if services are healthy: `docker-compose logs`

### Schema Registry Issues
- Verify Schema Registry is healthy: `curl http://localhost:8081/subjects`
- Check broker connectivity in Schema Registry logs

## Topics

- **mi-topico**: JSON messages (basic producer/consumer)
- **usuarios-eventos**: Avro-serialized user events (advanced producer/consumer)

## Testing

Run the test suites:

```bash
python test_producer.py
python test_consumer.py
```

## Development

### Adding New Topics
Update the topic name in producer/consumer configuration:
```python
consumer.subscribe(['new-topic'])
producer.produce('new-topic', ...)
```

### Updating Avro Schema
Modify the schema in `productor_pro.py` and ensure Schema Registry is aware of the changes.

## Stopping Services

```bash
docker-compose down
```

To remove all data:
```bash
docker-compose down -v
```

## References

- [Confluent Kafka Python Client](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Avro Serialization](https://avro.apache.org/)
- [Schema Registry Guide](https://docs.confluent.io/platform/current/schema-registry/index.html)

## License

MIT
