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
