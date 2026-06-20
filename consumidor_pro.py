from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

sr_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
avro_deserializer = AvroDeserializer(sr_client)

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'grupo-arquitectura-pro',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
consumer.subscribe(['usuarios-eventos'])

print("[Consumidor Pro] Esperando eventos binarios...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[Error] {msg.error()}")
            break

        usuario = avro_deserializer(msg.value(), None)
        print(f"[Consumidor Pro] Usuario recibido: {usuario}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()