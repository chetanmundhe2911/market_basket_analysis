import pandas as pd
import mlrun
from mlrun.feature_store import FeatureSet, Entity

# Load datasets
df_a = pd.read_csv("dataset_a.csv")
df_b = pd.read_csv("dataset_b.csv")
df = pd.concat([df_a, df_b])

# Load holiday calendar
holiday_dates = pd.read_csv("kenya_school_holidays.csv")["date"]
holiday_dates = pd.to_datetime(holiday_dates)

# Transformation function
def add_flags(df):
    df["date"] = pd.to_datetime(df["date"])
    df["is_school_holiday"] = df["date"].isin(holiday_dates)
    return df

# Apply transformation
df_transformed = add_flags(df)

# Define feature set
fs = FeatureSet(name="subscriber_daily_usage", entities=[Entity("nr_sbsc")])

# Ingest data into feature store
mlrun.feature_store.ingest(featureset=fs, source=df_transformed, as_df=True)

