from confluent_kafka import Consumer
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

TOPIC = 'usuarios-eventos'

schema_registry_conf = {
    'url': 'http://localhost:8081'
}

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'aplicacion',
    'sasl.password': 'usuario-password',
    'group.id': 'grupo-usuarios-seguro',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)
key_deserializer = StringDeserializer('utf_8')

consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC])

print(f"[Consumidor Seguro] Suscrito a '{TOPIC}' con SASL y Avro. Esperando evento...")

received = 0
try:
    while received < 1:
        msg = consumer.poll(5.0)
        if msg is None:
            print('[Consumidor Seguro] No llegó mensaje, reintentando...')
            continue
        if msg.error():
            print(f"[Consumidor Seguro] Error: {msg.error()}")
            break

        key = None
        if msg.key() is not None:
            key = key_deserializer(msg.key(), SerializationContext(TOPIC, MessageField.KEY))

        value = avro_deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))

        print('[Consumidor Seguro] Mensaje recibido:')
        print(f'  key={key}')
        print(f'  value={value}')
        print(f'  partition={msg.partition()} offset={msg.offset()}')
        received += 1
except KeyboardInterrupt:
    print('\n[Consumidor Seguro] Interrumpido por el usuario.')
finally:
    consumer.close()
    print('[Consumidor Seguro] Cerrado.')
