from pyspark import SparkContext, SparkConf

def cartelitos(func):
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf=conf)
    def wrapper(*args, **kwargs):
        print("\n=====INICIO=======\n")
        func(*args, **kwargs)
        print("\n=====FIN=======\n")
    return wrapper(sc)

@cartelitos  
def main(SparkContext):
    print("hola from pyspark")
    print(SparkContext.version)
    
        



if __name__ == "__main__":
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf=conf)
    main()

    
    
