from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName('Aneesh').getOrCreate()
df=spark.read.csv(r'C:\Users\AneeshKumar\OneDrive - DGLiger Consulting Private Limited\Desktop\Pyspark\simple-zipcodes.csv',inferSchema=True,header=True)
#rename column
# df=df.withColumnRenamed("City",'new_city').withColumnRenamed('Zipcode','zipcode').withColumnRenamed('State','state')
# df = df.select(
#     col("City").alias("new_city"),
 
#     col("Zipcode").alias("zipcode"),
#     col("State").alias("state")
# )
df.show()

df.select('city,zipcode,state').show()