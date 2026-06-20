from confluent_kafka import Producer
import json
from datetime import datetime

# Configuración del cliente Confluent  
config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(config)

def callback_entrega(err, msg):
    if err is not None:
        print(f"Error al entregar mensaje: {err}")
    else:
        print(f"Mensaje enviado con éxito a {msg.topic()} [{msg.partition()}]")


# Enviar un mensaje
message = {"mensaje": "Hola desde Python"+str(datetime.now().strftime("%I:%M:%S %p"))}
producer.produce('mi-topico', value=json.dumps(message), callback=callback_entrega)

# Forzar el envío de mensajes pendientes
producer.flush()
