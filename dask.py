import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

# --- Configuration ---
S3_INPUT_PATH = "s3://your-input-bucket/path/to/data/"
S3_OUTPUT_PATH = "s3://your-output-bucket/path/to/partitioned-data/"
PARTITION_COLUMN = "your_partition_column"  # Replace with a real column like 'date', 'region', etc.

# --- Cluster Configuration ---these based on your instance's hardware
N_WORKERS = 12
THREADS_PER_WORKER = 4
MEMORY_PER_WORKER = "30GB"  # Adjust ba

# Launch local Dask cluster
cluster = LocalCluster(
    n_workers=N_WORKERS,
    threads_per_worker=THREADS_PER_WORKER,
    memory_limit=MEMORY_PER_WORKER,
    dashboard_address=":8787",  # Makes dashboard accessible on port 8787
)
client = Client(cluster)

print("Dask Cluster is running:")
print(client)

# --- Load the Data from S3 ---
df = dd.read_parquet(
    S3_INPUT_PATH,
    engine="pyarrow",  # or "fastparquet"
    storage_options={"anon": False}
)

print(f"Loaded dataset with {df.npartitions} partitions")

# --- Repartition (optional but recommended) ---
# Choose based on memory/compute — too few = OOM, too many = overhead
df = df.repartition(npartitions=2000)

# Optional: Persist in memory before writing
df = df.persist()

# --- Write back to S3 in partitioned P
    S3_OUTPUT_PATH,
    engine="pyarrow",
    partition_on=[PARTITION_COLUMN],
    write_index=False,
    storage_options={"anon": False},
    overwrite=True
)

print(f"✅ Data written to {S3_OUTPUT_PATH}")


#  Full Code to Load 1TB from S3 with Dask

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

# --- Configuration ---
S3_INPUT_PATH = "s3://your-input-bucket/path/to/data/"
N_WORKERS = 12
THREADS_PER_WORKER = 4
MEMORY_PER_WORKER = "30GB"

# --- Start Dask Cluster ---
cluster = LocalCluster(
    n_workers=N_WORKERS,
    threads_per_worker=THREADS_PER_WORKER,
    memory_limit=MEMORY_PER_WORKER,
    dashboard_address=":8787"
)
client = Client(cluster)

print("✅ Dask cluster started")
print(client)

# --- Step 1: Lazy Load Parquet Files from S3 ---
df = dd.read_parquet(
    S3_INPUT_PATH,
    engine="pyarrow",  # or "fastparquet"
    storage_options={"anon": False}  # Remove if using IAM role
)

# --- Step 2: Print metadata only (does NOT load data) ---
print("📊 Schema:")
print(df.dtypes)
print("🧱 Partitions:", df.npartitions)

# --- Step 3: Peek safely (small sample, won’t OOM) ---
print("🔍 Sample rows:")
print(df.head(5))  # Just reads first few rows from first partition

# --- Optional Step 4: Persist if you're planning transformations ---
# Repartition first if existing partitions are too small or large
df = df.repartition(npartitions=2000)
df = df.persist()

print("✅ Data is now in memory (partially or fully based on worker capacity)")


#-----------------------------------
#...........-------------
