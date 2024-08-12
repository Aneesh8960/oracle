from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .appName("Myapp") \
    .getOrCreate()
#df=spark.read.parquet(r"abc.parquet",inferschema='true')
df=spark.read.parquet(r"C:\Users\AneeshKumar\OneDrive - DGLiger Consulting Private Limited\Desktop\Pyspark\abc.parquet",inferschema='true')
df.printSchema()