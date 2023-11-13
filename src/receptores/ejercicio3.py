from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt


BOOTSTRAP_SERVER = '157.245.244.105:9092'
TOPIC = '20565'


temperaturas = []
humedades = []

def procesar_mensaje(mensaje):
    payload = mensaje.value
    return payload

def actualizar_grafica(temperatura, humedad):

    temperaturas.append(temperatura)
    humedades.append(humedad)

    plt.plot(temperaturas, label='Temperatura')
    plt.plot(humedades, label='Humedad')

    plt.legend()
    plt.xlabel('Tiempo')
    plt.ylabel('Valores')
    plt.title('Temperatura y Humedad en Tiempo Real')

    plt.pause(0.1)
    plt.clf() 

def main():

    consumer = KafkaConsumer(TOPIC,
                                group_id='foo2',
                                bootstrap_servers=BOOTSTRAP_SERVER,
                                value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    try:
        for mensaje in consumer:
            payload = procesar_mensaje(mensaje)

            temperatura = payload.get('temperatura', 0)
            humedad = payload.get('humedad', 0)

            actualizar_grafica(temperatura, humedad)

            print(f"Mensaje recibido: {payload}")

    except KeyboardInterrupt:

        pass

    finally:
        consumer.close()

if __name__ == "__main__":
    main()
