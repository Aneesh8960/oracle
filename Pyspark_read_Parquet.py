from pyspark.sql import SparkSession
from pyspark.sql.functions import *

file_path = r"C:\Users\AneeshKumar\OneDrive - DGLiger Consulting Private Limited\Desktop\Pyspark\files\b01121918.dat_2023-09-12 09_31_14.parquet"

spark = SparkSession.builder \
    .appName("Myapp") \
    .getOrCreate()

df = spark.read.parquet(file_path)
prefix_pattern = r"tel:|sip:|tel:\+|\+"
df = df.withColumn("called_Party_Address", 
                                 when(col("called_Party_Address").startswith("sip"), 
                                      regexp_extract(col("called_Party_Address"), r'(\d+)', 1))
                                .otherwise(regexp_replace(col("called_Party_Address"), prefix_pattern, "")))
df = df.withColumn("called_Party_Address", 
                                              when((length(col("called_Party_Address")) == 12) & 
                                                   (col("called_Party_Address").startswith("27")), 
                                                   substring(col("called_Party_Address"), 3, 10))
                                              .otherwise(col("called_Party_Address")))                         
df.show()