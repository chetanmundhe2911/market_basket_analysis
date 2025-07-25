import mlrun
import os

# Step 1: Create or load MLRun project
project = mlrun.get_or_create_project("basket-project", context=".")

# Step 2: Register the function from the script
clean_fn = project.set_function(
    "clean_data.py",     # The file you wrote
    name="cleaner",      # Name for MLRun
    kind="job",          # Batch job
    image="mlrun/mlrun"  # MLRun base image (built-in)
)

# Step 3: Run the function with input data
artifact_path = os.path.abspath("artifacts")  # Set an absolute path for artifacts
run = clean_fn.run(
    name="clean-online-retail",
    handler="clean_online_retail",
    inputs={"raw_data": "data/OnlineRetail.xlsx"},  # Use your actual CSV filename
    artifact_path=artifact_path                   # Set the absolute artifact path
)
