from pyspark.sql import DataFrame
from pyspark.sql.functions import col,round, lower,concat, lit, coalesce
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext

sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

def limpia_columnas(df:DataFrame):
    for col in df.columns:
        col_truncada = col[:20]
        df = (
            df.withColumnRenamed(col,col_truncada.strip()\
                .replace(" ", "_")\
                .replace(".","_")))
    return df



df_cardata = (
    spark.read
    .option("header","true")
    .option("sep", ",")
    .csv("hdfs://172.17.0.2:9000/ingest/CarRentalData.csv"))

df_cargeo = (
    spark.read
    .option("header","true")
    .option("sep", ";")
    .csv("hdfs://172.17.0.2:9000/ingest/CarRentalGeo.csv"))

df_cardata = limpia_columnas(df_cardata)
df_cargeo = limpia_columnas(df_cargeo)


df_cargeo = df_cargeo.select([col('Official_Name_State').alias('state_name'), col('United_States_Postal').alias('state_alias')])

# Casteamos y truncamos los datos
df_cardata: DataFrame = df_cardata.select([
    lower(col('fuelType')).alias('fuelType'),
    round(col('rating'),0).cast('int').alias('rating'),
    col('renterTripsTaken').cast('int'),
    col('reviewCount').cast('int'),
    col('location_City').alias('city'),
    col('location_state').alias('state_alias'),
    col('owner_id').cast('int'),
    col('rate_daily').cast('int'),
    col('vehicle_make').alias('make'),
    col('vehicle_model').alias('model'),
    col('vehicle_year').cast('int').alias('year')
    ])

# Elimino las filas que tienen rating nulo
df_cardata = df_cardata.na.drop(subset=['rating'])

# Hago el join para tener los nombres completos de los estados y elimino las filas de Texas
df_cardata.createOrReplaceTempView('cardata')
df_cargeo.createOrReplaceTempView('cargeo')

df_rental = spark.sql(
    """--sql
        SELECT 
            c.fuelType,
            c.rating,
            c.renterTripsTaken,
            c.reviewCount,
            c.city,
            g.state_name,
            c.owner_id,
            c.rate_daily,
            c.make,
            c.model,
            c.`year`
        FROM cardata c
        INNER JOIN cargeo g ON c.state_alias = g.state_alias
        WHERE g.state_name != 'Texas'
    """)
df_rental.createOrReplaceTempView('rental')

hc.sql("INSERT INTO TABLE car_rental_db.car_rental_analytics (SELECT * FROM rental);")