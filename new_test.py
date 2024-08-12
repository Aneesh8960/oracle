from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder.appName("Example").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("event_date", StringType(), True),
    StructField("load_date", StringType(), True),
    StructField("other_column", StringType(), True)  # Add other columns as needed
])

# Create sample data
data = [
    ("2024-08-01", "2024-08-01", "Sample Data 1"),
    ("2024-08-02", "2024-08-01", "Sample Data 2"),
    ("2024-08-01", "2024-08-02", "Sample Data 3"),
    ("2024-08-03", "2024-08-01", "Sample Data 4"),
    ("2024-08-04", "2024-08-03", "Sample Data 5")
]

# Convert data to DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show()
