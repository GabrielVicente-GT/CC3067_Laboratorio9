from kafka import KafkaProducer
import json
import random
import time

BOOTSTRAP_SERVER = '157.245.244.105:9092'
TOPIC = '20565'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generar_data():

    temperatura = round(random.uniform(0, 100), 2)
    humedad = random.randint(0, 100)
    direccion_viento = random.choice(["N", "NW", "W", "SW", "S", "SE", "E", "NE"])

    return {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion_viento
    }

def direccion_a_numero(direccion_viento):
    direcciones = ["N", "NW", "W", "SW", "S", "SE", "E", "NE"]

    if direccion_viento not in direcciones:
        raise ValueError("Dirección del viento no válida")

    numero_asignado = direcciones.index(direccion_viento)
    
    return numero_asignado

def ENCODE(data):
    return {
        "t1": int(data["temperatura"]),
        "t2": int(str(round(data["temperatura"] - int(data["temperatura"]), 2))[-2:]),
        "h": data["humedad"],
        "d": direccion_a_numero(data["direccion_viento"])
    }

def main():
    try:
        while True:
            data = ENCODE(generar_data())
            key_bytes = 'sensor1'.encode('utf-8')
            producer.send(TOPIC, key=key_bytes, value=data)

            print(f"Mensaje enviado correctamente a {TOPIC} con clave {key_bytes} y datos {data}")

            time.sleep(random.uniform(15, 30))

    except KeyboardInterrupt:

        pass

    finally:
        producer.close()

if __name__ == "__main__":
    main()
