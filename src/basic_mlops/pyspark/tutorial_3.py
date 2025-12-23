from pyspark.sql import SparkSession

# Start the session
spark = SparkSession.builder.appName('Test').getOrCreate()

# Load .csv file
df = spark.read.csv(path='data/iris.csv',
                    header=True,
                    inferSchema=True)

# using groupby function
grouped_df = df.groupBy('species').avg()
grouped_df.show()