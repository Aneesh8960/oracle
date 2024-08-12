from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName('myapp').getOrCreate()

# Read Parquet files
df = spark.read.parquet(r'C:\Users\AneeshKumar\OneDrive - DGLiger Consulting Private Limited\Desktop\Pyspark\spark_files\customer_all\load_date=2024-07-21\customer_all_0_20240721.parquet',inferschema='true')
df.printSchema()

# Show the DataFrame content
df.show()
