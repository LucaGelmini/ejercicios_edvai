from init_pyspark import init_pyspark

from pyspark import SparkContext, sql

@init_pyspark  
def main(SparkContext: SparkContext, Spark: sql.SparkSession):
    print("hola from pyspark")
    print(SparkContext.version)
    
    
    print("\nEjercicio 4\n")
    Spark.sql("SELECT * FROM passengers")
    
    
    

if __name__ == "__main__":
    main()

    
