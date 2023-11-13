from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt

BOOTSTRAP_SERVER = '157.245.244.105:9092'
TOPIC = '20565'

temperaturas = []
humedades = []
direcciones = []

def procesar_mensaje(mensaje):
    payload = mensaje.value
    return payload


def numero_a_direccion(numero):
    direcciones = ["N", "NW", "W", "SW", "S", "SE", "E", "NE"]

    if not 0 <= numero < len(direcciones):
        raise ValueError("Número no válido para dirección del viento")

    direccion = direcciones[numero]
    
    return direccion
def DECODE(payload):
    return {
        "t": float(str(payload.get('t1', 0))+"." + str(payload.get('t2', 0))),
        "h": payload.get('h', 0),
        "d": numero_a_direccion(payload.get('d', 0))
    }

def actualizar_grafica(temperatura, humedad, direccion):
    temperaturas.append(temperatura)
    humedades.append(humedad)
    direcciones.append(direccion)

    plt.plot(temperaturas, label='Temperatura')
    plt.plot(humedades, label='Humedad')

    plt.plot([], [], label='Dirección: ' + direccion, color='white')

    plt.legend()
    plt.xlabel('Tiempo')
    plt.ylabel('Valores')
    plt.title('Temperatura, Humedad y Dirección en Tiempo Real')

    plt.pause(0.1)
    plt.clf()

def main():
    consumer = KafkaConsumer(
        TOPIC,
        group_id='foo2',
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    try:
        for mensaje in consumer:
            payload = procesar_mensaje(mensaje)
            payload = DECODE(payload)

            actualizar_grafica(payload["t"], payload["h"], payload["d"])

            print(f"Mensaje recibido: {payload}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
