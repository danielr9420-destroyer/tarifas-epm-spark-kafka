# -*- coding: utf-8 -*-
"""
Productor Kafka - Tarea 3 Big Data (UNAD)
Simula lecturas de medidores de agua de hogares atendidos por EPM y las publica
en el topic 'lecturas_agua' cada segundo (adaptación del Anexo 3).

Ejecutar:  python3 kafka_producer.py
Detener:   Ctrl+C
"""
import json
import random
import time

from kafka import KafkaProducer

TOPIC = "lecturas_agua"

MUNICIPIOS = ["Medellín", "Bello", "Itaguí", "Envigado", "Sabaneta",
              "La Estrella", "Caldas", "Copacabana", "Girardota", "Barbosa"]

# Proporción aproximada de hogares por estrato (más hogares en estratos 1 a 3)
ESTRATOS = [1, 2, 3, 4, 5, 6]
PESOS_ESTRATO = [0.12, 0.33, 0.29, 0.12, 0.08, 0.06]


def generar_lectura():
    estrato = random.choices(ESTRATOS, weights=PESOS_ESTRATO)[0]
    # Consumo mensual en m3: normal alrededor de 12-15 m3; 5 % de los casos
    # simulan un consumo anómalo (posible fuga) para la detección de alertas.
    if random.random() < 0.05:
        consumo = random.uniform(40, 80)
    else:
        consumo = max(1.0, random.gauss(11 + estrato * 0.8, 4))
    return {
        "medidor_id": random.randint(100000, 999999),
        "municipio": random.choice(MUNICIPIOS),
        "estrato": estrato,
        "consumo_m3": round(consumo, 2),
        "timestamp": int(time.time()),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"),
    )
    print(f"Enviando lecturas al topic '{TOPIC}' (Ctrl+C para detener)...")
    try:
        while True:
            lectura = generar_lectura()
            producer.send(TOPIC, value=lectura)
            print(f"Sent: {lectura}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Productor detenido.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
