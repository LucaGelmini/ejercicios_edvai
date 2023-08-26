from pyspark import SparkContext, SparkConf

def init_pyspark(func):
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf=conf)
    def wrapper(*args, **kwargs):
        print("\n=====INICIO=======\n")
        func(*args, **kwargs)
        print("\n=====FIN=======\n")
    return wrapper(sc)