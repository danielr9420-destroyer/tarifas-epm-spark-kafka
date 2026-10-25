# -*- coding: utf-8 -*-
"""
Tarea 3 - Procesamiento de Datos con Apache Spark (Big Data - UNAD, 202016911)
Procesamiento en batch del dataset "Tarifas de acueducto y aguas residuales
para hogares - EPM" (datos.gov.co).

Problema: ¿Cómo han evolucionado las tarifas de acueducto y alcantarillado de EPM
entre 2017 y 2026 y qué tan grandes son las diferencias entre estratos, sectores
y municipios?

Etapas:
  1. Carga del CSV (local o HDFS)
  2. Diagnóstico de calidad (nulos, duplicados, inconsistencias)
  3. Limpieza y transformación (DataFrames)
  4. Análisis exploratorio (DataFrames + un ejemplo con RDDs)
  5. Visualización (gráficas PNG) y guardado de resultados

Uso:
  spark-submit tarifas_epm_batch.py --input hdfs://localhost:9000/Tarea3/tarifas_epm.csv --output resultados
  spark-submit tarifas_epm_batch.py --input ../data/tarifas_epm.csv --output ../output
"""

import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Consumo básico (de subsistencia) usado para estimar una factura típica.
# Resolución CRA 750 de 2016: 13 m3/mes para municipios entre 1.000 y 2.000 msnm,
# rango en el que se ubica el Valle de Aburrá.
CONSUMO_BASICO_M3 = 13

MESES = {
    "Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4, "Mayo": 5, "Junio": 6,
    "Julio": 7, "Agosto": 8, "Septiembre": 9, "Octubre": 10, "Noviembre": 11,
    "Diciembre": 12,
}

COLUMNAS_DINERO = {
    "Cargo Fijo": "cargo_fijo",
    "Cargo por Consumo Menor": "consumo_menor",
    "Cargo por Consumo Mayor": "consumo_mayor",
    "Cargo por Consumo": "consumo_no_residencial",
    "Suspensión": "suspension",
    "Reinstalación": "reinstalacion",
    "Reconexión": "reconexion",
    "Corte": "corte",
}


def titulo(texto):
    print("\n" + "=" * 78)
    print(" " + texto)
    print("=" * 78)


def a_numero(col_name):
    """'$10.531,25' -> 10531.25 (quita $, puntos de miles y usa punto decimal)."""
    limpio = F.regexp_replace(F.col(col_name), r"[$\s]", "")
    limpio = F.regexp_replace(limpio, r"\.", "")
    limpio = F.regexp_replace(limpio, ",", ".")
    return limpio.cast("double")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="../data/tarifas_epm.csv")
    parser.add_argument("--output", default="../output")
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)

    spark = (SparkSession.builder
             .appName("TarifasEPM_Batch")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------ 1. CARGA
    titulo("1. CARGA DEL CONJUNTO DE DATOS")
    df_raw = (spark.read
              .option("header", True)
              .option("encoding", "UTF-8")
              .option("inferSchema", False)
              .csv(args.input))
    total = df_raw.count()
    print(f"Registros: {total:,} | Columnas: {len(df_raw.columns)}")
    df_raw.printSchema()
    df_raw.show(5, truncate=False)

    # -------------------------------------------------------- 2. DIAGNÓSTICO
    titulo("2. DIAGNÓSTICO DE CALIDAD")
    print("Valores nulos por columna:")
    df_raw.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                   for c in df_raw.columns]).show(truncate=False)

    duplicados = total - df_raw.dropDuplicates().count()
    print(f"Filas duplicadas exactas: {duplicados}")

    print("Municipios (se observa 'Estrella' y 'La Estrella' como dos nombres):")
    df_raw.groupBy("Municipio").count().orderBy(F.desc("count")).show(30, truncate=False)

    print("Valores de Estrato (mezcla números y categorías no residenciales):")
    df_raw.groupBy("Sector", "Estrato").count().orderBy("Sector", "Estrato").show(30, truncate=False)

    # ----------------------------------------- 3. LIMPIEZA Y TRANSFORMACIÓN
    titulo("3. LIMPIEZA Y TRANSFORMACIÓN")
    df = df_raw.dropDuplicates()
    print(f"- Duplicados eliminados: {duplicados}")

    # Normalizar texto y municipio
    df = (df.withColumn("Municipio", F.trim(F.col("Municipio")))
            .withColumn("Municipio", F.when(F.col("Municipio") == "Estrella", "La Estrella")
                                      .otherwise(F.col("Municipio"))))
    print("- 'Estrella' unificado como 'La Estrella'")

    # Convertir columnas monetarias de texto a número
    # (se hace en un solo select: Spark no distingue mayúsculas en nombres de
    #  columna, por eso "Corte" y "corte" no pueden convivir)
    otras = [c for c in df.columns if c not in COLUMNAS_DINERO]
    df = df.select(*otras, *[a_numero(o).alias(n) for o, n in COLUMNAS_DINERO.items()])
    print("- Columnas monetarias convertidas de texto ('$10.531,25') a double")

    # Mes en texto -> número y fecha
    mapa_mes = F.create_map([F.lit(x) for kv in MESES.items() for x in kv])
    df = (df.withColumn("mes_num", mapa_mes[F.trim(F.col("Mes"))])
            .withColumn("anio", F.col("Año").cast(IntegerType()))
            .withColumn("fecha", F.make_date("anio", "mes_num", F.lit(1)))
            .withColumnRenamed("Municipio", "municipio")
            .withColumnRenamed("Sector", "sector")
            .withColumnRenamed("Servicio", "servicio")
            .withColumnRenamed("Estrato", "estrato")
            .drop("Año", "Mes"))
    print("- Mes convertido a número y creada la columna 'fecha'")

    # Nulos: 2 registros sin 'Cargo por Consumo' (Copacabana, Oficial, abril)
    nulos = df.filter(F.col("consumo_no_residencial").isNull()).count()
    df = df.filter(F.col("consumo_no_residencial").isNotNull())
    print(f"- Registros con cargo por consumo nulo eliminados: {nulos}")

    # Columnas derivadas
    df = (df.withColumn("tipo_usuario",
                        F.when(F.col("sector") == "Residencial", "Residencial")
                         .otherwise("No residencial"))
            .withColumn("estrato_num",
                        F.when(F.col("estrato").rlike("^[1-6]$"),
                               F.col("estrato").cast(IntegerType())))
            # Cargo por m3 unificado: residencial usa consumo básico (menor),
            # no residencial usa 'Cargo por Consumo'
            .withColumn("valor_m3",
                        F.when(F.col("tipo_usuario") == "Residencial", F.col("consumo_menor"))
                         .otherwise(F.col("consumo_no_residencial"))))

    # Valores atípicos: un cargo más de 10 veces la mediana de su grupo
    # (mismo municipio, sector, servicio y año) se considera error de digitación
    # (p. ej. $689.770 en lugar de $6.897,70) y se reemplaza por esa mediana.
    grupo = Window.partitionBy("municipio", "sector", "servicio", "anio")
    for c in ["cargo_fijo", "valor_m3"]:
        mediana = F.percentile_approx(c, 0.5).over(grupo)
        df = df.withColumn(f"{c}_mediana", mediana)
        atipicos = df.filter(F.col(c) > 10 * F.col(f"{c}_mediana"))
        print(f"- Atípicos en {c}: {atipicos.count()}")
        atipicos.select("municipio", "sector", "servicio", "fecha", c, f"{c}_mediana") \
                .show(truncate=False)
        df = (df.withColumn(c, F.when(F.col(c) > 10 * F.col(f"{c}_mediana"),
                                      F.col(f"{c}_mediana")).otherwise(F.col(c)))
                .drop(f"{c}_mediana"))

    # Tarifas en cero = municipios donde EPM no presta alcantarillado
    sin_servicio = df.filter((F.col("cargo_fijo") == 0) & (F.col("valor_m3") == 0))
    print(f"- Registros con tarifa 0 (servicio no prestado): {sin_servicio.count()}")
    sin_servicio.groupBy("municipio", "servicio").count().show(truncate=False)
    df = df.filter(~((F.col("cargo_fijo") == 0) & (F.col("valor_m3") == 0)))

    df = (df.withColumn("factura_estimada",
                        F.round(F.col("cargo_fijo") + CONSUMO_BASICO_M3 * F.col("valor_m3"), 2)))
    print(f"- Nuevas columnas: tipo_usuario, estrato_num, valor_m3, "
          f"factura_estimada (cargo fijo + {CONSUMO_BASICO_M3} m3)")

    df = df.cache()
    print(f"Registros después de la limpieza: {df.count():,}")
    df.select("municipio", "sector", "estrato", "servicio", "fecha", "cargo_fijo",
              "valor_m3", "factura_estimada").show(5, truncate=False)

    # ------------------------------------------------------------- 4. EDA
    titulo("4. ANÁLISIS EXPLORATORIO (EDA)")

    print("4.1 Estadísticas descriptivas de los cargos (COP):")
    df.select("cargo_fijo", "valor_m3", "factura_estimada", "reconexion", "corte") \
      .describe().show(truncate=False)

    # Factura del hogar = acueducto + alcantarillado para el mismo mes
    claves = ["municipio", "sector", "estrato", "estrato_num", "tipo_usuario",
              "anio", "mes_num", "fecha"]
    hogar = (df.groupBy(claves)
               .pivot("servicio", ["Acueducto", "Alcantarillado"])
               .agg(F.first("factura_estimada"))
               .withColumn("factura_total", F.col("Acueducto") + F.col("Alcantarillado")))

    print("4.2 Evolución anual de la factura estimada residencial (promedio, COP):")
    anual = (hogar.filter("tipo_usuario = 'Residencial'")
                  .groupBy("anio")
                  .agg(F.round(F.avg("Acueducto"), 0).alias("acueducto"),
                       F.round(F.avg("Alcantarillado"), 0).alias("alcantarillado"),
                       F.round(F.avg("factura_total"), 0).alias("factura_total")))
    w = Window.orderBy("anio")
    anual = (anual.withColumn("variacion_pct",
                              F.round((F.col("factura_total") / F.lag("factura_total").over(w) - 1) * 100, 2))
                  .orderBy("anio"))
    anual.show(truncate=False)

    print("4.3 Factura estimada por estrato - Medellín, último mes disponible:")
    ultima_fecha = df.agg(F.max("fecha")).first()[0]
    print(f"    Último mes disponible: {ultima_fecha}")
    estratos = (hogar.filter((F.col("municipio") == "Medellín") &
                             (F.col("fecha") == F.lit(ultima_fecha)) &
                             F.col("estrato_num").isNotNull())
                     .select("estrato_num", "Acueducto", "Alcantarillado", "factura_total"))
    ref = estratos.filter("estrato_num = 4").first()["factura_total"]
    estratos = (estratos.withColumn("vs_estrato_4_pct",
                                    F.round((F.col("factura_total") / F.lit(ref) - 1) * 100, 1))
                        .orderBy("estrato_num"))
    print("    (estrato 4 = tarifa de referencia; negativo = subsidio, positivo = contribución)")
    estratos.show(truncate=False)

    print("4.4 Ranking de municipios - estrato 3, último mes disponible:")
    municipios = (hogar.filter((F.col("fecha") == F.lit(ultima_fecha)) &
                               (F.col("estrato_num") == 3))
                       .select("municipio", F.round("factura_total", 0).alias("factura_total"))
                       .orderBy(F.desc("factura_total")))
    municipios.show(30, truncate=False)

    print("4.5 Factura promedio por sector no residencial vs residencial (último año completo):")
    ultimo_anio_completo = (df.groupBy("anio").agg(F.countDistinct("mes_num").alias("m"))
                              .filter("m = 12").agg(F.max("anio")).first()[0])
    por_sector = (hogar.filter(F.col("anio") == ultimo_anio_completo)
                       .groupBy("sector")
                       .agg(F.round(F.avg("factura_total"), 0).alias("factura_promedio"))
                       .orderBy(F.desc("factura_promedio")))
    print(f"    Año: {ultimo_anio_completo}")
    por_sector.show(truncate=False)

    print("4.6 Crecimiento 2017 -> último año por servicio (cargo fijo y valor m3, residencial):")
    anio_min = df.agg(F.min("anio")).first()[0]
    anio_max = df.agg(F.max("anio")).first()[0]
    base = (df.filter((F.col("tipo_usuario") == "Residencial") &
                      F.col("anio").isin(anio_min, anio_max))
              .groupBy("servicio")
              .pivot("anio", [anio_min, anio_max])
              .agg(F.avg("valor_m3")))
    crecimiento = base.select(
        "servicio",
        F.round(F.col(str(anio_min)), 2).alias(f"m3_{anio_min}"),
        F.round(F.col(str(anio_max)), 2).alias(f"m3_{anio_max}"),
        F.round((F.col(str(anio_max)) / F.col(str(anio_min)) - 1) * 100, 1).alias("crecimiento_pct"))
    crecimiento.show(truncate=False)

    print("4.7 Cargos por gestión (promedio anual, COP):")
    gestion = (df.groupBy("anio")
                 .agg(*[F.round(F.avg(c), 0).alias(c)
                        for c in ["suspension", "reinstalacion", "reconexion", "corte"]])
                 .orderBy("anio"))
    gestion.show(truncate=False)

    print("4.8 Ejemplo con RDDs (map + reduceByKey + acciones):")
    rdd = df.select("municipio", "cargo_fijo").rdd
    conteo = (rdd.map(lambda r: (r["municipio"], 1))            # transformación map
                 .reduceByKey(lambda a, b: a + b))               # transformación reduceByKey
    max_fijo = (rdd.map(lambda r: (r["municipio"], r["cargo_fijo"]))
                   .reduceByKey(max))
    print(f"    count(): {conteo.count()} municipios")             # acción
    print("    take(5) registros por municipio:", conteo.sortBy(lambda x: -x[1]).take(5))
    print("    take(5) cargo fijo máximo:", max_fijo.sortBy(lambda x: -x[1]).take(5))
    total_reg = rdd.map(lambda r: 1).reduce(lambda a, b: a + b)  # acción reduce
    print(f"    reduce(): {total_reg:,} registros en total")

    # ----------------------------------------------------- 5. RESULTADOS
    titulo("5. GUARDADO Y VISUALIZACIÓN")
    tablas = {
        "evolucion_anual": anual,
        "factura_por_estrato": estratos,
        "ranking_municipios": municipios,
        "factura_por_sector": por_sector,
        "crecimiento_servicio": crecimiento,
        "cargos_gestion": gestion,
    }
    pdfs = {nombre: t.toPandas() for nombre, t in tablas.items()}
    for nombre, pdf in pdfs.items():
        ruta = os.path.join(args.output, f"{nombre}.csv")
        pdf.to_csv(ruta, index=False, encoding="utf-8")
        print(f"- {ruta}")

    # Dataset limpio en Parquet (formato columnar para análisis posteriores)
    ruta_parquet = os.path.join(args.output, "tarifas_limpias.parquet")
    df.write.mode("overwrite").parquet(ruta_parquet)
    print(f"- {ruta_parquet}")

    generar_graficos(pdfs, args.output, ultima_fecha)

    spark.stop()


def generar_graficos(pdfs, salida, ultima_fecha):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib no está instalado: pip install matplotlib (se omiten gráficos)")
        return

    carpeta = os.path.join(salida, "graficos")
    os.makedirs(carpeta, exist_ok=True)
    azul, naranja, gris = "#1f5fa8", "#e8892b", "#8a8f98"
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})

    def miles(x, _):
        return f"${x:,.0f}".replace(",", ".")
    from matplotlib.ticker import FuncFormatter
    fmt = FuncFormatter(miles)

    # 1. Evolución anual
    a = pdfs["evolucion_anual"]
    fig, ax = plt.subplots(figsize=(9, 4.8))
    ax.plot(a["anio"], a["acueducto"], marker="o", color=azul, label="Acueducto")
    ax.plot(a["anio"], a["alcantarillado"], marker="o", color=naranja, label="Alcantarillado")
    ax.plot(a["anio"], a["factura_total"], marker="o", color="#333", lw=2.2, label="Total")
    ax.set_title(f"Factura estimada residencial ({CONSUMO_BASICO_M3} m³), promedio anual")
    ax.set_xlabel("Año"); ax.yaxis.set_major_formatter(fmt); ax.legend(frameon=False)
    ax.set_xticks(a["anio"])
    fig.tight_layout(); fig.savefig(os.path.join(carpeta, "1_evolucion_anual.png"), dpi=150); plt.close(fig)

    # 2. Por estrato
    e = pdfs["factura_por_estrato"]
    fig, ax = plt.subplots(figsize=(8, 4.8))
    colores = [azul if v < 0 else (gris if v == 0 else naranja) for v in e["vs_estrato_4_pct"]]
    barras = ax.bar(e["estrato_num"].astype(str), e["factura_total"], color=colores)
    for b, v in zip(barras, e["vs_estrato_4_pct"]):
        ax.annotate(f"{v:+.0f}%", (b.get_x() + b.get_width() / 2, b.get_height()),
                    ha="center", va="bottom", fontsize=9)
    ax.set_title(f"Medellín: factura estimada por estrato ({ultima_fecha:%m/%Y})\n"
                 "azul = subsidio, naranja = contribución (vs. estrato 4)")
    ax.set_xlabel("Estrato"); ax.yaxis.set_major_formatter(fmt)
    fig.tight_layout(); fig.savefig(os.path.join(carpeta, "2_factura_por_estrato.png"), dpi=150); plt.close(fig)

    # 3. Ranking municipios
    m = pdfs["ranking_municipios"].sort_values("factura_total")
    fig, ax = plt.subplots(figsize=(8, max(4, 0.32 * len(m) + 1)))
    ax.barh(m["municipio"], m["factura_total"], color=azul)
    ax.set_title(f"Factura estimada estrato 3 por municipio ({ultima_fecha:%m/%Y})")
    ax.xaxis.set_major_formatter(fmt)
    fig.tight_layout(); fig.savefig(os.path.join(carpeta, "3_ranking_municipios.png"), dpi=150); plt.close(fig)

    # 4. Por sector
    s = pdfs["factura_por_sector"]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    ax.bar(s["sector"], s["factura_promedio"], color=[naranja, azul, azul, gris][:len(s)])
    ax.set_title("Factura estimada promedio por sector (último año completo)")
    ax.yaxis.set_major_formatter(fmt)
    fig.tight_layout(); fig.savefig(os.path.join(carpeta, "4_factura_por_sector.png"), dpi=150); plt.close(fig)

    # 5. Cargos de gestión
    g = pdfs["cargos_gestion"]
    fig, ax = plt.subplots(figsize=(9, 4.8))
    for c, col in zip(["suspension", "reinstalacion", "reconexion", "corte"],
                      [azul, naranja, gris, "#333"]):
        ax.plot(g["anio"], g[c], marker="o", color=col, label=c.capitalize())
    ax.set_title("Cargos por gestión del servicio, promedio anual")
    ax.set_xticks(g["anio"]); ax.yaxis.set_major_formatter(fmt); ax.legend(frameon=False)
    fig.tight_layout(); fig.savefig(os.path.join(carpeta, "5_cargos_gestion.png"), dpi=150); plt.close(fig)

    print(f"- Gráficos guardados en {carpeta}")


if __name__ == "__main__":
    main()
