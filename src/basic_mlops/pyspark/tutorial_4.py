from pyspark.sql import SparkSession

# Start the session
spark = SparkSession.builder.appName('Test').getOrCreate()

# Load .csv file
df = spark.read.csv(path='data/iris.csv',
                    header=True,
                    inferSchema=True)

# Groupby and sort action(ascending=True)
ascending_true = df.groupBy('species').max('petal_length').sort(1, ascending=True)
ascending_true.show()

# Groupby and sort action(ascending=Fasle)
ascending_false = df.groupBy('species').max('petal_length').sort(1, ascending=False)
ascending_false.show()

# Print the result
result = ascending_true.collect()
print(result)

# Save the result
ascending_false.write.csv(path='data/iris_filtered.csv',
                          header=True)