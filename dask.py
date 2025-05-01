import dask.dataframe as dd
from dask.distributed import Client
import os

# Connect to a local Dask cluster (modify if you're using a remote scheduler)
client = Client()  # Or Client("scheduler-address:port") if remote
print(client)

# S3 paths
input_path = "s3://your-input-bucket/path/to/data/"
output_path = "s3://your-output-bucket/path/to/partitioned-data/"

# Load data from S3
# Adjust `blocksize` for CSV/JSON if needed; Parquet handles it efficiently
df = dd.read_parquet(input_path, storage_options={"anon": False})

# Optional: Inspect the dataset
print("Initial Dask dataframe:")
print(df)

# Partitioning column (change this to your actual column name)
partition_column = 'your_partition_column'  # e.g. 'date' or 'region'

# Repartition if needed (e.g., 2000 partitions, or by row count)
df = df.repartition(npartitions=2000)

# Write back to S3 as partitioned Parquet
df.to_parquet(
    output_path,
    engine="pyarrow",  # or "fastparquet"
    write_index=False,
    partition_on=[partition_column],
    storage_options={"anon": False},
    overwrite=True
)

print(f"Data successfully written to {output_path}")
