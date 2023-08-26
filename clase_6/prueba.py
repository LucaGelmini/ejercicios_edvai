from pyspark import SparkContext, SparkConf

def cartelitos(func):
    def wrapper(*args, **kwargs):
        print("\n=====INICIO=======\n")
        func(*args, **kwargs)
        print("\n=====FIN=======\n")
    return wrapper

@cartelitos  
def main():
    print("hola from pyspark")
        



if __name__ == "__main__":
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf=conf)

    
    
