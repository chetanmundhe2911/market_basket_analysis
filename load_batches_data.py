import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import os

# --- Configuration ---
S3_INPUT_PATH = "s3://your-input-bucket/path/to/data/"
S3_OUTPUT_PATH = "s3://your-output-bucket/path/to/partitioned-daywise-data/"
DATE_COLUMN = "event_date"  # replace with your actual date column name
PARTITION_COLUMN = "day"

# Cluster Setup (tune based on your system)
N_WORKERS = 12
THREADS_PER_WORKER = 4
MEMORY_PER_WORKER = "30GB"

# --- Start Local Dask Cluster ---
cluster = LocalCluster(
    n_workers=N_WORKERS,
    threads_per_worker=THREADS_PER_WORKER,
    memory_limit=MEMORY_PER_WORKER,
    dashboard_address=":8787"
)
client = Client(cluster)

print("✅ Dask cluster started")
print(client)

# --- Step 1: Read Parquet Files from S3 ---
df = dd.read_parquet(
    S3_INPUT_PATH,
    engine="pyarrow",
    storage_options={"anon": False}  # Assumes AWS credentials or IAM role are in place
)

print(f"📥 Loaded {df.npartitions} partitions from {S3_INPUT_PATH}")
print("🔎 Sample schema:\n", df.dtypes)

# --- Step 2: Convert Date to Day (string format 'YYYY-MM-DD') ---
df[PARTITION_COLUMN] = dd.to_datetime(df[DATE_COLUMN]).dt.strftime('%Y-%m-%d')

# --- Step 3: Repartition to Optimize Write (optional but recommended) ---
df = df.repartition(npartitions=2000)
df = df.persist()

print("✅ Data repartitioned and persisted")

# --- Step 4: Write to S3 Partitioned by Day ---
df.to_parquet(
    S3_OUTPUT_PATH,
    engine="pyarrow",
    partition_on=[PARTITION_COLUMN],
    write_index=False,
    storage_options={"anon": False},
    overwrite=True
)

print(f"✅ Successfully written day-partitioned data to {S3_OUTPUT_PATH}")
