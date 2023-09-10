from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, concat, lit, coalesce
sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)

def limpia_columnas(df:DataFrame):
    for col in df.columns:
        df = (df.withColumnRenamed(col, col.strip().replace(" ", "_")))
    return df
def castea_df(df:DataFrame):
    df = (
        df
        .withColumn("datetime", to_timestamp(concat(col("Fecha"), lit("T"), col("Hora_UTC")), "dd/MM/yyyy'T'HH:mm"))
        .withColumn("Fecha",col("Fecha").cast("string"))
        .withColumn("Hora_UTC",col("Hora_UTC").cast("string"))
        .withColumn("Clase_de_Vuelo_(todos_los_vuelos)",col("Clase_de_Vuelo_(todos_los_vuelos)").cast("string"))
        .withColumn("Clasificación_Vuelo",col("Clasificación_Vuelo").cast("string"))
        .withColumn("Tipo_de_Movimiento",col("Tipo_de_Movimiento").cast("string"))
        .withColumn("Aeropuerto",col("Aeropuerto").cast("string"))
        .withColumn("Origen_/_Destino",col("Origen_/_Destino").cast("string"))
        .withColumn("Aerolinea_Nombre",col("Aerolinea_Nombre").cast("string"))
        .withColumn("Aeronave",col("Aeronave").cast("string"))
        .withColumn("Pasajeros",col("Pasajeros").cast("int"))
        .withColumn("Calidad_dato",col("Calidad_dato").cast("string")))
    return df

# Leemos los df desde hdfs
df_vuelos_2021 = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv"))

df_vuelos_2022 = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv"))

df_aeropuertos = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv"))

# Si los df de vuelos no tienen las mismas columnas hay un error
if len(df_vuelos_2021.columns)!=len(df_vuelos_2022.columns):
    raise Exception(f"Cantidad de columnas distintas: 2021->{len(df_vuelos_2021)}, 2022->{len(df_vuelos_2022)}")

# Aseguramos de on tener espacios en los nombres de las columnas
df_vuelos_2021 = limpia_columnas(df_vuelos_2021)
df_vuelos_2022 = limpia_columnas(df_vuelos_2022)
df_aeropuertos = limpia_columnas(df_aeropuertos)

#Casteamos los tipos de datos
df_vuelos_2021 = castea_df(df_vuelos_2021)
df_vuelos_2022 = castea_df(df_vuelos_2022)
df_aeropuertos = (
    df_aeropuertos
    .withColumn("uom_elev", col("uom_elev").cast("float"))
    .withColumn("distancia_ref", col("distancia_ref").cast("float"))
)

# Union de los dataframe de vuelos
df_vuelos = df_vuelos_2021.union(df_vuelos_2022)

# Dropeo columnas indeseadas
df_vuelos = df_vuelos.drop("Calidad_dato")
df_aeropuertos =(
    df_aeropuertos
    .drop("inhab")
    .drop("fir")
)

#Creo vistas para manipular con SQL
df_vuelos.createOrReplaceTempView('vuelos_view')

#Filtro por vuelos locales y saco nulls de la tabla vuelos, además dropeo la columna de clasificacion de vuelo
df_vuelos_filtrados = (
    spark.sql("""--sql
        SELECT * FROM vuelos_view
        WHERE 
            `Clasificación_Vuelo`='Doméstico' OR 
            `Clasificación_Vuelo`='Domestico'
        ;""")
    .drop("Clasificación_Vuelo")
    .withColumn("Pasajeros", coalesce(col("Pasajeros"), lit(0)))
    )
df_aeropuertos_coalesed = df_aeropuertos.withColumn("distancia_ref", coalesce(col("distancia_ref"), lit(0)))

# df_vuelos_filtrados.filter(df_vuelos_filtrados['Pasajeros'].isNull()).show() Código para verificar

df_vuelos_filtrados.createOrReplaceTempView('vuelos_transformed')
df_aeropuertos_coalesed.createOrReplaceTempView('aeropuertos_transformed')

# Inserto las vistas en las tablas de hive
hc.sql("INSERT INTO TABLE informe_anac.vuelos (SELECT * FROM vuelos_transformed);")

hc.sql("INSERT INTO TABLE informe_anac.aeropuertos (SELECT * FROM aeropuertos_transformed);")








