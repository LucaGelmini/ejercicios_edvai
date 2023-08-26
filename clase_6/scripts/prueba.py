from init_pyspark import init_pyspark

@init_pyspark  
def main(SparkContext):
    print("hola from pyspark")
    print(SparkContext.version)
    

if __name__ == "__main__":
    main()

    
    
