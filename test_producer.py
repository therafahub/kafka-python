from confluent_kafka import Producer
import json
import time

# Configuración del cliente Confluent  
config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(config)

def callback_entrega(err, msg):
    if err is not None:
        print(f"[ERROR] No se pudo entregar: {err}")
    else:
        print(f"[OK] Mensaje enviado a {msg.topic()} [partición {msg.partition()}, offset {msg.offset()}]")

# Enviar múltiples mensajes
for i in range(5):
    message = {"id": i, "mensaje": f"Mensaje de prueba #{i}"}
    print(f"[ENVIANDO] {message}")
    producer.produce('mi-topico', value=json.dumps(message), callback=callback_entrega)
    time.sleep(0.5)

# Forzar el envío de mensajes pendientes
print("[FLUSH] Esperando confirmación...")
producer.flush()
print("[COMPLETADO] Todos los mensajes fueron enviados")
