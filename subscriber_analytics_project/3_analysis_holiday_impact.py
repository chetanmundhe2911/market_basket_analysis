import mlrun
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Get offline features
df = mlrun.feature_store.get_offline_features([
    "subscriber_daily_usage.call_count",
    "subscriber_daily_usage.msg_count",
    "subscriber_daily_usage.data_mb",
    "subscriber_daily_usage.is_school_holiday",
    "subscriber_daily_usage.source"
]).to_dataframe()

# Aggregate stats
summary = df.groupby(["source", "is_school_holiday"])[
    ["call_count", "msg_count", "data_mb"]
].mean().reset_index()

print(summary)

# Plotting
sns.set_style("whitegrid")
sns.barplot(data=summary, x="is_school_holiday", y="call_count", hue="source")
plt.title("Avg Call Count - Holiday vs Non-Holiday (Set A vs Set B)")
plt.xlabel("Is School Holiday?")
plt.ylabel("Average Call Count")
plt.show()
