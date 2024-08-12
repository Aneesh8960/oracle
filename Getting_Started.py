from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read CSV File") \
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
df.show(100000,False)