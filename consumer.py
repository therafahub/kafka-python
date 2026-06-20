from confluent_kafka import Consumer, KafkaError
import json

# Configuración del cliente Confluent
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mi-grupo-consumidor',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(config)

# Suscribirse al tema
consumer.subscribe(['mi-topico'])

# Leer mensajes en tiempo real
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error al recibir mensaje: {msg.error()}")
                break

        print(f"Mensaje recibido: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
