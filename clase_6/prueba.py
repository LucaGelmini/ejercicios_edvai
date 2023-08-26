from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf=conf)

    # rest of your code goes here
    print("hola from pyspark")
