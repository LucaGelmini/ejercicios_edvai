from pyspark.sql import SparkSession

# Crear una instancia de SparkSession
spark = SparkSession.builder \
    .appName("Lectura de archivo Parquet") \
    .getOrCreate()
    
    # Leer el archivo Parquet
df = spark.read.parquet("/nifi/ingest/yellow_tripdata_2021-01.parquet")

# Mostrar el esquema del DataFrame
df.printSchema()

df.createOrReplaceTempView("yellowTrip")

# 1
spark.sql("""
    SELECT cast(VendorId as integer),
    cast(Tpep_pickup_datetime as date),
    cast(Total_amount as double)
    FROM yellowTrip
    WHERE Total_amount < 10
    """).show()

# 2
spark.sql("""
    SELECT cast(VendorId as integer),
    cast(Tpep_pickup_datetime as date),
    cast(Total_amount as double)
    FROM yellowTrip
    ORDER BY Total_amount DESC
    LIMIT 10
    """).show()

# 3
spark.sql("""
    SELECT cast(VendorId as integer),
    cast(Total_amount as double),
    cast(Trip_distance as integer)
    FROM yellowTrip
    WHERE Trip_distance > 10
    ORDER BY Total_amount ASC
    LIMIT 10
    """).show()

# 4
spark.sql("""
    SELECT cast(VendorId as integer),
    cast(Tpep_pickup_datetime as date),
    cast(Trip_distance as integer)
    FROM yellowTrip
    WHERE Passenger_count > 2 and Payment_type = 1
    ORDER BY Tpep_pickup_datetime ASC
    """).show()

# 5
spark.sql("""
    SELECT cast(VendorId as integer),
    cast(Tpep_pickup_datetime as date),
    cast(Trip_distance as integer),
    cast(Passenger_count as integer),
    cast(Tip_amount as float)
    FROM yellowTrip
    WHERE Trip_distance > 10
    ORDER BY Tip_amount DESC
    LIMIT 7
    """).show()

#6
spark.sql("""
    SELECT cast(RatecodeID as integer),
    sum(cast(Total_amount as double)) as Total_amount,
    mean(cast(Total_amount as double)) as mean
    FROM yellowTrip
    WHERE RatecodeID != 6
    GROUP BY RatecodeID
    """).show()
