from init_pyspark import init_pyspark

from pyspark import SparkContext

@init_pyspark  
def main(SparkContext: SparkContext):
    print("hola from pyspark")
    print(SparkContext.version)
    
    

if __name__ == "__main__":
    main()

    
    
