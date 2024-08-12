import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
import pandas as pd

# Path to your Parquet file
path = r"C:\Users\AneeshKumar\Downloads\customer_all_0_20240716.parquet"

# Read DataFrame from Parquet file
df = pd.read_parquet(path)

# Display DataFrame and its schema
print("DataFrame:")
print(df)
print("\nDataFrame Schema:")
print(df.dtypes)

# Infer schema from DataFrame
schema = pa.schema([
    (col, pa.int64()) if pd.api.types.is_integer_dtype(dtype)
    else (col, pa.float64()) if pd.api.types.is_float_dtype(dtype)
    else (col, pa.timestamp('us')) if pd.api.types.is_datetime64_any_dtype(dtype)
    else (col, pa.string())  # Handle other data types as needed
    for col, dtype in df.dtypes.items()
])
print("\nInferred Schema for PyArrow Table:")
print(schema)

arrays = []
for col in df.columns:
    if pd.api.types.is_integer_dtype(df[col]):
        arrays.append(pa.array(df[col], type=pa.int64()))
    elif pd.api.types.is_float_dtype(df[col]):
        arrays.append(pa.array(df[col], type=pa.float64()))
    elif pd.api.types.is_datetime64_any_dtype(df[col]):
        arrays.append(pa.array(df[col], type=pa.timestamp('us')))
    else:
        arrays.append(pa.array(df[col].astype(str)))
table = pa.Table.from_arrays(arrays, schema=schema)

# Write the table to a Parquet file with Snappy compression
pq.write_table(table, 'abc.parquet', compression='SNAPPY')
print(f"\nSchema used for writing to abc.parquet:")
print(schema)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadParquetExample") \
    .getOrCreate()

# Read Parquet file into DataFrame
df_spark = spark.read.parquet("abc.parquet")

# Show DataFrame schema and content
print("\nSpark DataFrame Schema:")
df_spark.printSchema()
print("\nSpark DataFrame Content:")
df_spark.show(truncate=False)
