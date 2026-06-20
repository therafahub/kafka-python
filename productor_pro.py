from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# 1. Definir el esquema Avro
definicion_esquema = """
{
  "namespace": "com.empresa.telemetria",
  "type": "record",
  "name": "UsuarioCreado",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "nombre", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}
"""

# 2. Configurar Clientes
sr_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
avro_serializer = AvroSerializer(sr_client, definicion_esquema)

config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(config)

# 3. Enviar datos estructurados binarios
datos_usuario = {"id": 101, "nombre": "Rafael", "email": "rafael@email.com"}

print("[Productor Pro] Registrando esquema y enviando evento binario...")
producer.produce(
    topic='usuarios-eventos',
    key=StringSerializer('utf_8')("USR-101"),
    value=avro_serializer(datos_usuario, SerializationContext('usuarios-eventos', MessageField.VALUE)),
    callback=lambda err, msg: print(f"[Callback] Error: {err}" if err else f"[Callback] Enviado a {msg.topic()} [{msg.partition()}]")
)
producer.flush()