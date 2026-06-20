from confluent_kafka import Consumer, KafkaError
import json
import time

# Configuración del cliente Confluent
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mi-grupo-test',
    'auto.offset.reset': 'earliest',
    'session.timeout.ms': 6000
}
consumer = Consumer(config)

print("[INICIANDO] Consumer conectado a mi-topico")
consumer.subscribe(['mi-topico'])

# Leer mensajes en tiempo real
timeout_contador = 0
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            timeout_contador += 1
            if timeout_contador % 10 == 0:
                print(f"[ESPERANDO] {timeout_contador // 10}0 segundos sin mensajes...")
            continue
        
        timeout_contador = 0
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("[EOF] Fin de partición")
                continue
            else:
                print(f"[ERROR] {msg.error()}")
                break
        
        try:
            data = json.loads(msg.value().decode('utf-8'))
            print(f"[RECIBIDO] Offset={msg.offset()}, Partición={msg.partition()}, Data={data}")
        except:
            print(f"[RECIBIDO] Raw: {msg.value().decode('utf-8')}")
            
except KeyboardInterrupt:
    print("\n[INTERRUMPIDO] Consumer detenido por usuario")
finally:
    consumer.close()
    print("[CERRADO] Conexión finalizada")
