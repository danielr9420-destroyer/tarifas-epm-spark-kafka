# -*- coding: utf-8 -*-
"""
Consumidor Spark Structured Streaming - Tarea 3 Big Data (UNAD)
Lee las lecturas de agua del topic Kafka 'lecturas_agua', les aplica las tarifas
residenciales de EPM (Medellín, agosto 2026, obtenidas del análisis batch) y
calcula en tiempo real, por ventanas de 1 minuto:
  - número de lecturas, consumo promedio y valor facturado por estrato
  - alertas de consumo anómalo (> 40 m3, posible fuga)

Ejecutar (ajustar la versión 3.5.3 a la salida de `spark-submit --version`):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               DoubleType, LongType, StringType)

TOPIC = "lecturas_agua"
CONSUMO_BASICO_M3 = 13      # consumo de subsistencia (Res. CRA 750/2016)
UMBRAL_ALERTA_M3 = 40

# Tarifas residenciales Medellín (acueducto + alcantarillado), agosto 2026, COP.
# Fuente: resultado del procesamiento batch (tarifas_epm_batch.py).
TARIFAS = [
    # estrato, cargo fijo, $/m3 básico, $/m3 adicional
    (1, 6415.13, 3569.39, 8923.46),
    (2, 9622.69, 5354.07, 8923.46),
    (3, 14033.09, 7808.02, 8923.46),
    (4, 16037.82, 8923.46, 8923.46),
    (5, 24056.74, 13385.20, 13385.20),
    (6, 25660.51, 14277.53, 14277.53),
]

ESQUEMA = StructType([
    StructField("medidor_id", IntegerType()),
    StructField("municipio", StringType()),
    StructField("estrato", IntegerType()),
    StructField("consumo_m3", DoubleType()),
    StructField("timestamp", LongType()),
])


def preparar(spark, lecturas_json):
    """Convierte el JSON de Kafka en columnas y calcula el valor de cada lectura
    (unión streaming-estática con la tabla de tarifas)."""
    tarifas = spark.createDataFrame(
        TARIFAS, ["estrato", "cargo_fijo", "valor_m3_basico", "valor_m3_adicional"])

    lecturas = (lecturas_json
                .select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("d"))
                .select("d.*")
                .withColumn("event_time", F.col("timestamp").cast("timestamp")))

    basico = F.least(F.col("consumo_m3"), F.lit(CONSUMO_BASICO_M3))
    adicional = F.greatest(F.col("consumo_m3") - CONSUMO_BASICO_M3, F.lit(0))
    return (lecturas.join(F.broadcast(tarifas), "estrato", "left")
                    .withColumn("valor_factura",
                                F.round(F.col("cargo_fijo")
                                        + basico * F.col("valor_m3_basico")
                                        + adicional * F.col("valor_m3_adicional"), 0)))


def estadisticas_por_estrato(lecturas):
    return (lecturas
            .groupBy(F.window("event_time", "1 minute").alias("ventana"), "estrato")
            .agg(F.count("*").alias("lecturas"),
                 F.round(F.avg("consumo_m3"), 2).alias("consumo_prom_m3"),
                 F.round(F.sum("consumo_m3"), 1).alias("consumo_total_m3"),
                 F.round(F.avg("valor_factura"), 0).alias("factura_prom"),
                 F.sum(F.when(F.col("consumo_m3") > UMBRAL_ALERTA_M3, 1).otherwise(0))
                  .alias("alertas"))
            .select(F.date_format("ventana.start", "HH:mm").alias("inicio"),
                    F.date_format("ventana.end", "HH:mm").alias("fin"),
                    "estrato", "lecturas", "consumo_prom_m3", "consumo_total_m3",
                    "factura_prom", "alertas"))


def alertas(lecturas):
    return (lecturas.filter(F.col("consumo_m3") > UMBRAL_ALERTA_M3)
                    .select(F.date_format("event_time", "HH:mm:ss").alias("hora"),
                            "medidor_id", "municipio", "estrato", "consumo_m3",
                            "valor_factura"))


def main():
    spark = (SparkSession.builder
             .appName("LecturasAguaEPM_Streaming")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", TOPIC)
                .option("startingOffsets", "latest")
                .load())

    lecturas = preparar(spark, kafka_df)

    q1 = (estadisticas_por_estrato(lecturas)
          .orderBy(F.desc("inicio"), "estrato")
          .writeStream
          .outputMode("complete")
          .format("console")
          .option("truncate", False)
          .option("numRows", 30)
          .queryName("estadisticas_por_estrato")
          .trigger(processingTime="10 seconds")
          .start())

    q2 = (alertas(lecturas)
          .writeStream
          .outputMode("append")
          .format("console")
          .option("truncate", False)
          .queryName("alertas_consumo_anomalo")
          .trigger(processingTime="10 seconds")
          .start())

    print("Consumidor iniciado. Resultados cada 10 s. Ctrl+C para detener.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
