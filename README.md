# Análisis de tarifas de acueducto y alcantarillado de EPM con Apache Spark

**Curso:** Big Data (202016911) – Universidad Nacional Abierta y a Distancia (UNAD)
**Actividad:** Tarea 3 – Procesamiento de Datos con Apache Spark
**Autor:** Daniel Alberto Rodríguez Sandoval – Grupo 202016911_28

## Problema

¿Cómo han evolucionado las tarifas de acueducto y alcantarillado de EPM entre 2017 y 2026, y qué tan grandes son las diferencias entre estratos, sectores y municipios?

Las tarifas de agua impactan directamente el presupuesto de los hogares. En Colombia se aplica un esquema de subsidios (estratos 1 a 3) y contribuciones (estratos 5, 6 y sectores comercial e industrial). Este proyecto usa Apache Spark para limpiar, transformar y analizar el histórico de tarifas publicado por EPM. Además, simula en tiempo real lecturas de medidores con Apache Kafka y Spark Structured Streaming.

## Conjunto de datos

- **Nombre:** Tarifas de acueducto y aguas residuales para hogares – EPM
- **Fuente:** Portal de Datos Abiertos de Colombia (datos.gov.co)
- **Tamaño:** 24.179 registros, 14 columnas, enero 2017 – agosto 2026, 24 municipios (25 nombres en el archivo original, porque "Estrella" y "La Estrella" aparecen por separado)
- **Columnas:** Municipio, Sector, Estrato, Servicio, Cargo Fijo, Cargo por Consumo Menor/Mayor, Cargo por Consumo, Suspensión, Reinstalación, Reconexión, Corte, Año, Mes

## Estructura del repositorio

```
├── batch/
│   └── tarifas_epm_batch.py          # Procesamiento en batch (limpieza, EDA, gráficos)
├── streaming/
│   ├── kafka_producer.py             # Generador de lecturas de agua → Kafka
│   └── spark_streaming_consumer.py   # Consumidor Spark Structured Streaming
├── data/
│   └── tarifas_epm.csv               # Dataset original
├── output/                           # Resultados de la ejecución batch
│   ├── graficos/*.png
│   ├── *.csv
│   └── salida_consola.txt
└── requirements.txt
```

## 1. Procesamiento en batch

El script `batch/tarifas_epm_batch.py` usa DataFrames de Spark (y un ejemplo con RDDs) en cinco etapas.

| Etapa | Qué hace |
|---|---|
| Carga | Lee el CSV desde HDFS o desde un archivo local |
| Diagnóstico | Cuenta nulos, duplicados e inconsistencias de categorías |
| Limpieza | Elimina 370 duplicados y 2 nulos. Unifica "Estrella" con "La Estrella". Convierte valores `$10.531,25` a número. Corrige 3 errores de digitación detectados como atípicos (más de 10 veces la mediana del grupo). Excluye 18 registros con tarifa 0 de municipios sin servicio de alcantarillado. |
| Transformación | Crea fecha (año + mes), tipo de usuario, estrato numérico, valor por m³ unificado y factura estimada (cargo fijo + 13 m³ de consumo básico) |
| EDA | Estadísticas descriptivas, evolución anual, comparación por estrato (subsidios y contribuciones), ranking de municipios, comparación por sector, crecimiento por servicio, cargos de gestión, operaciones RDD (`map`, `reduceByKey`, `count`, `take`, `reduce`) |
| Resultados | 6 tablas CSV, dataset limpio en Parquet y 5 gráficos PNG |

### Ejecución en la máquina virtual (Hadoop + Spark)

```bash
# 1. Dependencias para los gráficos
pip install matplotlib pandas

# 2. Iniciar Hadoop y subir el dataset a HDFS
start-dfs.sh
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put data/tarifas_epm.csv /Tarea3/

# 3. Ejecutar el análisis
cd batch
spark-submit tarifas_epm_batch.py --input hdfs://localhost:9000/Tarea3/tarifas_epm.csv --output ../output
```

> Si su `core-site.xml` usa otro puerto para HDFS, ajuste `localhost:9000`. También se puede ejecutar sin HDFS usando `--input ../data/tarifas_epm.csv`.

### Principales hallazgos

- La factura estimada residencial (acueducto + alcantarillado, 13 m³) pasó de **$57.679 en 2017 a $133.838 en 2026**, un aumento de 132 %. El mayor salto ocurrió en **2022 (+21,2 %)**.
- El valor del m³ residencial creció **183 % en acueducto** y **149 % en alcantarillado** entre 2017 y 2026.
- En Medellín (agosto 2026), el estrato 1 paga **60 % menos** que el estrato 4 (referencia), y el estrato 6 paga **60 % más**. Esto evidencia el esquema de subsidios y contribuciones.
- Para el estrato 3, **Copacabana y La Estrella** tienen la factura estimada más alta ($132.043) y **Barbosa** la más baja ($103.520).
- El sector **comercial** tiene la factura promedio más alta (2025), seguido del industrial.
- Los cargos por **corte y reconexión** se duplicaron con creces desde 2017 y tuvieron su mayor alza en 2026.

## 2. Procesamiento en tiempo real (Kafka + Spark Streaming)

- `kafka_producer.py` publica cada segundo en el topic `lecturas_agua` una lectura simulada: medidor, municipio, estrato, consumo en m³ y timestamp. Alrededor del 5 % son consumos anómalos (más de 40 m³).
- `spark_streaming_consumer.py` lee el topic y une cada lectura con las tarifas de EPM obtenidas en el análisis batch para calcular su valor. Luego muestra en consola, cada 10 segundos:
  1. Estadísticas por **ventana de 1 minuto y estrato**: lecturas, consumo promedio y total, factura promedio y número de alertas.
  2. **Alertas** de consumo anómalo (posible fuga).

### Ejecución

```bash
# Terminal 1: ZooKeeper y Kafka (Kafka 3.9.x)
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &

# Crear el topic (solo la primera vez)
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 --topic lecturas_agua

# Terminal 1: productor
pip install kafka-python
python3 streaming/kafka_producer.py

# Terminal 2: consumidor (Scala 2.12 / Spark 3.5.3; verificar con spark-submit --version)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/spark_streaming_consumer.py
```

Las métricas de la ejecución se pueden ver en la interfaz web de Spark: `http://<IP-de-la-VM>:4040` (pestañas *Jobs*, *Stages* y *Structured Streaming*).

## Tecnologías

Apache Hadoop (HDFS) · Apache Spark 3.5 (PySpark, DataFrames, RDD, Structured Streaming) · Apache Kafka 3.9 · Python 3 · Matplotlib
