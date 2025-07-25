import mlrun
import os

# Local project (optional)
project = mlrun.get_or_create_project("basket-project", context=".")

# Use local function kind
clean_fn = mlrun.code_to_function(
    name="cleaner",
    kind="local",            # 🔥 LOCAL EXECUTION
    handler="clean_online_retail",
    filename="clean_data.py"
)

# Run function with raw input
artifact_path = os.path.abspath("artifacts")
run = clean_fn.run(
    name="clean-online-retail",
    inputs={"raw_data": "data/OnlineRetail.csv"},
    artifact_path=artifact_path
)
