from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Esquema Avro compartido con productor_pro.py
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

# Configuración profesional con parámetros de seguridad obligatorios
config_segura = {
    'bootstrap.servers': 'localhost:9092',
    'security.protocol': 'SASL_PLAINTEXT', # Indica que viaja autenticado
    'sasl.mechanism': 'PLAIN',             # El tipo de cifrado de credenciales
    'sasl.username': 'aplicacion',         # Tu usuario definido en JAAS
    'sasl.password': 'usuario-password'    # Tu contraseña definida en JAAS
}

schema_registry_conf = {
    'url': 'http://localhost:8081'
}

sr_client = SchemaRegistryClient(schema_registry_conf)
avro_serializer = AvroSerializer(sr_client, definicion_esquema)
productor = Producer(config_segura)


def acuse(err, msg):
    if err:
        print(f"❌ Error de autenticación o envío: {err}")
    else:
        print(f"Acceso concedido. Mensaje escrito en la partición {msg.partition()}")

# Enviar datos estructurados binarios usando Avro
datos_usuario = {
    "id": 101,
    "nombre": "Rafael",
    "email": "rafael@email.com"
}

print("[Productor Seguro] Registrando esquema Avro y enviando evento binario...")
productor.produce(
    topic='usuarios-eventos',
    key=StringSerializer('utf_8')('KEY-1'),
    value=avro_serializer(datos_usuario, SerializationContext('usuarios-eventos', MessageField.VALUE)),
    callback=acuse
)

productor.flush()
