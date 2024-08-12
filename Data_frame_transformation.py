from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Create a SparkSession
if 'spark' not in locals():
    spark = SparkSession.builder \
        .appName("MyApp") \
        .master("local[*]") \
        .getOrCreate()

# Define CSV file path
csv_file_path = r"C:\Users\AneeshKumar\OneDrive - DGLiger Consulting Private Limited\Desktop\Pyspark\files\processinstance_2024-04-30.csv"

# Read the CSV file with all options specified
df = spark.read.csv(
    csv_file_path,
    header=True,                    # Treat the first row as header
    inferSchema=True,               # Infer schema automatically
    sep=",",                        # Specify the delimiter
    quote='"',                      # Specify the quote character
    escape="\\",                    # Specify the escape character
    nullValue="",                   # Specify the representation of null values
    mode="PERMISSIVE",              # Specify the mode for schema inference failures or missing columns
    ignoreLeadingWhiteSpace=False,  # Specify whether to ignore leading whitespaces
    ignoreTrailingWhiteSpace=False, # Specify whether to ignore trailing whitespaces
    encoding="UTF-8",               # Specify the encoding
    dateFormat="yyyy-MM-dd",        # Specify the date format
    timestampFormat="yyyy-MM-dd'T'HH:mm:ss.SSSXXX"  # Specify the timestamp format
)

# Show the DataFrame schema and some sample data
df.printSchema()
df.show()
#Creating temp View to use as Spark Sql
df_view=df.createOrReplaceTempView("Employee")
df.select(concat(col("Id"), lit("s"))).show()
df.select(col("Id")+5).show()
df_with_added_value = df.withColumn("Id_plus_5", col("Id") + 5)
df.select(expr("id as myid"),expr("concat(id,ProcessDefinitionId)")).show()
df = df.withColumn("Id", df["Id"].cast("int"))
df.select(col("Id")).show()
## Add 5 to the "Id" column
# df_with_added_value = df.withColumn("Id_plus_5", col("Id") + 5)
# df_with_added_value.select(col("Id_plus_5")).show()

#Spark SQL

print("Printing Spark Sq-------------")
spark.sql(
"""
select * from Employee where id ='04g2t0000009zA0AAI'
"""
).show()

new_df=df.select(col("Id").alias("new_id"))
new_df.show()


