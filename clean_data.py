import mlrun
import pandas as pd
import datetime

def clean_online_retail(context, raw_data: mlrun.DataItem):
    # Load raw data
    df = raw_data.as_df()

    # Drop rows with missing values
    df.dropna(inplace=True)

    # Drop canceled orders (InvoiceNo starting with 'C')
    df = df[~df['InvoiceNo'].astype(str).str.startswith('C')]

    # Strip whitespaces from descriptions
    df['Description'] = df['Description'].str.strip()

    # Generate timestamped filename
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"cleaned_data_{timestamp}.csv"

    # ✅ Save cleaned data to CSV
    df.to_csv(output_filename, index=False)

    # ✅ Log the artifact after saving
    context.log_artifact(output_filename, local_path=output_filename)
