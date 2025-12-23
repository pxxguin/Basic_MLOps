from pyspark.sql import SparkSession

# Start this session
spark = SparkSession.builder.appName('Test').getOrCreate()

# Define the .csv file
df = spark.read.csv(path='data/iris.csv',
                    header=True,
                    inferSchema=True)

# Show basic
df.show(5)
df.printSchema()