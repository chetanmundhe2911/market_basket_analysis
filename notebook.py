# Install necessary libraries before running:
# pip install pyspark mlflow pandas scikit-learn matplotlib

# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
from pyspark.ml.fpm import FPGrowth
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Step 1: Start a PySpark Session
# This initializes the Spark session we will use for all operations.
spark = SparkSession.builder \
    .appName("TransactionsFPGrowthAnalysis") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

print("Spark Session Created Successfully!")

# Step 2: Generate Transactional Data
# Create sample transaction dataframes using Pandas and combine into one dataset.
# This mimics multiple tables with different transaction types.
# You can replace or modify the data below as needed.



# Create Pandas DataFrames
df_table1 = pd.DataFrame(table1_data)
df_table2 = pd.DataFrame(table2_data)
df_table3 = pd.DataFrame(table3_data)

# Combine all DataFrames into 
transactions_df = pd.concat([df_table1, df_table2, df_table3], ignore_index=True)

# Save combined transaction data (optional)
# transactions_df.to_csv("transactions.csv", index=False)

# Display initial transaction data
print("Initial Transaction Data (Pandas DataFrame):")
print(transactions_df.head())

# Step 3: Convert the Pandas DataFrame to a PySpark DataFrame
spark_transactions_df = spark.createDataFrame(transactions_df)

# Show Spark DataFrame
print("Initial Data in Spark DataFrame:")
spark_transactions_df.show(truncate=False)

# Step 4: Group Transactions for FP-Growth
# Aggregate related transaction records into baskets.
grouped_transactions_df = (
    spark_transactions_df
    .groupBy("DS_TRXN_TYPE")
    .agg(collect_list("DS_RESN_TYPE").alias("items"))
)

# Display grouped transaction data
print("Grouped Transactions (Ready for FP-Growth):")
grouped_transactions_df.show(truncate=False)

# Step 5: Run FP-Growth
# Initialize FP-Growth Model
fp_growth = FPGrowth(itemsCol="items", minSupport=0.1, minConfidence=0.3)

# Fit the FP-Growth model to the grouped transaction data
fp_model = fp_growth.fit(grouped_transactions_df)

# Retrieve and display frequent itemsets
frequent_itemsets = fp_model.freqItemsets
print("Frequent Itemsets:")
frequent_itemsets.show(truncate=False)

# Retrieve and display association rules
association_rules = fp_model.associationRules
print("Association Rules:")
association_rules.show(truncate=False)

# Step 6: Convert Data to Pandas for Visualization
frequent_itemsets_df = frequent_itemsets.toPandas()
association_rules_df = association_rules.toPandas()

# Step 7: Visualize Frequent Itemsets
# Create barplot for frequent itemsets
plt.figure(figsize=(10, 6))
sns.barplot(data=frequent_itemsets_df, x="freq", y="items", orient="h")
plt.title("Frequent Itemsets")
plt.xlabel("Frequency")
plt.ylabel("Itemsets")
plt.show()

# Step 8: Visualize Associations
# Create scatter plot for association rules
plt.figure(figsize=(10, 6))
sns.scatterplot(data=association_rules_df, x="support", y="confidence", size="lift", hue="lift", alpha=0.7)
plt.title("Association Rules")
plt.xlabel("Support")
plt.ylabel("Confidence")
plt.legend(title="Lift", bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.show()

# Step 9:
# Stop the Spark session once all processing is done---
spark.stop()
print("Spark session stopped.")
