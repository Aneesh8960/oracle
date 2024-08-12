from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read JSON File") \
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()

# Read JSON file into a DataFrame with various options
json_df = spark.read \
    .option("multiline", "true")\
    .option("mode", "PERMISSIVE")\
    .json(r"C:\Users\AneeshKumar\OneDrive - DGLiger Consulting Private Limited\Desktop\MyoneDrive\OneDrive - DGLiger Consulting Private Limited\ETL\config.json")

# Show the schema of the DataFrame
json_df.printSchema()

# Show the first few rows of the DataFrame
json_df.show()

# Don't forget to stop the SparkSession when you're done

