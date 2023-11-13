from kafka import KafkaProducer
import json
import random
import time

# Configurar el servidor Kafka y el topic
BOOTSTRAP_SERVER = '157.245.244.105:9092'
TOPIC = '20565'

# Configurar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generar_data():
    # Generar datos simulados
    temperatura = round(random.uniform(0, 100), 2)
    humedad = random.randint(0, 100)
    direccion_viento = random.choice(["N", "NW", "W", "SW", "S", "SE", "E", "NE"])

    return {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion_viento
    }

def main():
    try:
        while True:
            data = generar_data()
            key_bytes = 'sensor1'.encode('utf-8')
            producer.send(TOPIC, key=key_bytes, value=data)

            print(f"Mensaje enviado correctamente a {TOPIC} con clave {key_bytes} y datos {data}")

            # Dormir entre 15 y 30 segundos
            time.sleep(random.uniform(15, 30))

    except KeyboardInterrupt:
        # Maneja la interrupción de teclado (Ctrl+C)
        pass

    finally:
        producer.close()

if __name__ == "__main__":
    main()
