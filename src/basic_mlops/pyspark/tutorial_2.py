from pyspark.sql import SparkSession

# Start the session
spark = SparkSession.builder.appName('Test').getOrCreate()

# Load the .csv file
df = spark.read.csv(path='data/iris.csv',
                    header=True,
                    inferSchema=True)

# Filter smaller than 3.5 in sepal_width
filtered_df = df.filter(df.sepal_width<3.5)
filtered_df.show()

# If I want to display just sepcies, I can just display selected columns
filtered_df = df.select('sepal_width', 'species').filter(df.sepal_width<3.5)
filtered_df.show(5)