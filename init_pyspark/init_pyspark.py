from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def init_pyspark(func):
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf=conf)
    spark = (
        SparkSession()
        .builder
        .appName("MyApp")
        .enableHiveSupport()
        .getOrCreate()
    )

    def wrapper(*args, **kwargs):
        print("\n=====INICIO=======\n")
        func(*args, **kwargs)
        print("\n=====FIN=======\n")
    return wrapper(sc,spark)