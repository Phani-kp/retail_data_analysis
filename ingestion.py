from pyspark.sql import SparkSession

def load_data(file_path):
    spark = SparkSession.builder.appName("RetailDataIngestion").getOrCreate()
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    return data

if __name__ == "__main__":
    data = load_data("data/raw/online_retail.csv")
    data.show()
