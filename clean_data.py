import mlrun
import pandas as pd

def clean_online_retail(context, raw_data: mlrun.DataItem):
    # Load CSV
    df = raw_data.as_df()

    # Drop rows with missing values
    df_clean = df.dropna()

    # Filter out canceled orders (InvoiceNo starts with 'C')
    df_clean = df_clean[~df_clean['InvoiceNo'].astype(str).str.startswith('C')]

    # Log how many rows were cleaned
    context.log_result("original_rows", len(df))
    context.log_result("cleaned_rows", len(df_clean))

    # Save cleaned dataset
    cleaned_path = "cleaned_retail.csv"
    df_clean.to_csv(cleaned_path, index=False)

    # Log as artifact
    context.log_artifact("cleaned_dataset", local_path=cleaned_path)
