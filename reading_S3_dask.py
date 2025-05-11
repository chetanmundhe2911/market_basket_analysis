# Sample Code (Reading Data from S3 using Dask)

import dask.dataframe as dd

# Read Parquet or CSV files from S3 using Dask
df = dd.read_parquet('s3://your-bucket-name/path/to/your-data/*.parquet', 
                     engine='pyarrow')  # or you can use CSV, JSON etc.

# Perform Dask operations (e.g., basic analysis)
df.head()  # Dask will compute only when needed
df.describe().compute()  # Trigger computation for summary statistics

# Dask can also handle larger-than-memory datasets using out-of-core computation

# 2. PyArrow + Pandas (for efficient file reading)
import pyarrow.parquet as pq
import s3fs

# Use s3fs to read data from S3
fs = s3fs.S3FileSystem(anon=False)

# Read Parquet file from S3 directly
table = pq.read_table('s3://your-bucket-name/path/to/your-data/*.parquet', filesystem=fs)

# Convert to Pandas DataFrame
df = table.to_pandas()

# Perform your analysis
print(df.describe())


import pyarrow.parquet as pq
import s3fs

# Use s3fs to read data from S3
fs = s3fs.S3FileSystem(anon=False)

# Read Parquet file from S3 directly
table = pq.read_table('s3://your-bucket-name/path/to/your-data/*.parquet', filesystem=fs)

# Convert to Pandas DataFrame
df = table.to_pandas()

# Perform your analysis
print(df.describe())

import modin.pandas as mpd

# Read CSV/Parquet data from S3 using Modin
df = mpd.read_parquet('s3://your-bucket-name/path/to/your-data/*.parquet')

# Perform analysis (similar to Pandas API)
df.head()
df.describe()


import modin.pandas as mpd

# Read CSV/Parquet data from S3 using Modin
df = mpd.read_parquet('s3://your-bucket-name/path/to/your-data/*.parquet')

# Perform analysis (similar to Pandas API)
df.head()
df.describe()

# Sample Code (Using AWS Data Wrangler to Read from S3)

import awswrangler as wr

# Read Parquet file from S3
df = wr.s3.read_parquet('s3://your-bucket-name/path/to/your-data/*.parquet')

# Perform data analysis
print(df.describe())

# 5. Apache Spark with PySpark (for very large-scale data processing)
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("LargeDataAnalysis").getOrCreate()

# Read Parquet file from S3
df = spark.read.parquet("s3://your-bucket-name/path/to/your-data/*.parquet")

# Perform transformations or actions on the DataFrame
df.describe().show()

#..........
