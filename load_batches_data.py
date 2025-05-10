import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import os

# --- Configuration ---
S3_INPUT_PATH = "s3://your-input-bucket/path/to/data/"
S3_OUTPUT_PATH = "s3://your-output-bucket/path/to/partitioned-daywise-data/"
DATE_COLUMN = "event_date"  # Replace with your actual date column
PARTITION_COLUMN = "day"

# --- Cluster Configuration ---
# Allow overrides via environment variables (useful for EMR or ECS)
N_WORKERS = int(os.getenv("DASK_WORKERS", 24))
THREADS_PER_WORKER = int(os.getenv("DASK_THREADS", 4))
MEMORY_PER_WORKER = os.getenv("DASK_MEMORY", "30GB")

# --- Start Local Dask Cluster ---
cluster = LocalCluster(
    n_workers=N_WORKERS,
    threads_per_worker=THREADS_PER_WORKER,
    memory_limit=MEMORY_PER_WORKER,
    dashboard_address=":8787"
)
client = Client(cluster)

print("✅ Dask cluster started at http://localhost:8787 (or corresponding EC2 IP)")
print(client)

# --- Step 1: Read Parquet Files from S3 ---
try:
    df = dd.read_parquet(
        S3_INPUT_PATH,
        engine="pyarrow",
        storage_options={"anon": False}
    )
except Exception as e:
    raise RuntimeError(f"❌ Failed to load data from S3: {e}")

print(f"📥 Loaded {df.npartitions} partitions from {S3_INPUT_PATH}")
print("🔍 Schema:\n", df.dtypes)

# --- Step 2: Sample First Few Rows (safe peek) ---
try:
    print("🧪 Sample rows:\n", df[[DATE_COLUMN]].head(5))
except Exception as e:
    print(f"⚠️ Could not read sample rows: {e}")

# --- Step 3: Convert Date Column to Day Format ---
try:
    df[PARTITION_COLUMN] = dd.to_datetime(df[DATE_COLUMN], errors='coerce').dt.strftime('%Y-%m-%d')
except Exception as e:
    raise RuntimeError(f"❌ Failed to convert date column: {e}")

print(f"📆 Transformed '{DATE_COLUMN}' → '{PARTITION_COLUMN}'")

# --- Step 4: Optional Repartitioning (Adaptive) ---
try:
    # Estimate partition count (goal: ~100MB per partition)
    approx_rows = df.shape[0].compute()
    rows_per_partition = 2_000_000  # Adjust for your row size
    target_parts = max(100, approx_rows // rows_per_partition)
    print(f"📊 Repartitioning to ~{target_parts} partitions...")
    df = df.repartition(npartitions=target_parts)
    df = df.persist()
    print("✅ Data repartitioned and persisted")
except Exception as e:
    raise RuntimeError(f"❌ Failed during repartitioning: {e}")

# --- Step 5: Write Back to S3 Partitioned by Day ---
try:
    df.to_parquet(
        S3_OUTPUT_PATH,
        engine="pyarrow",
        partition_on=[PARTITION_COLUMN],
        write_index=False,
        storage_options={"anon": False},
        overwrite=True
    )
    print(f"✅ Successfully written day-partitioned data to {S3_OUTPUT_PATH}")
except Exception as e:
    raise RuntimeError(f"❌ Failed to write data to S3: {e}")
