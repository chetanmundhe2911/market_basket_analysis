import pandas as pd
import polars as pl

# Load your datasets
mtandao_df = pd.read_csv('mtandao.csv')  # Ensure correct file path
soc_df = pd.read_csv('soc.csv')  # Ensure correct file path

# Define keys and references for joining (adjust according to your data structure)
kpi_df = pd.DataFrame()  # Create this dataframe according to your KPI structure
ref_df = pd.DataFrame()  # Create this dataframe according to your reference structure
ref2_df = pd.DataFrame()  # Another reference dataframe

# Define keys used for joining
keys = ['recordid', 'datatimestamp']  # Example keys, adjust as needed
keys2 = ['cid', 'rnc']  # Example keys, adjust as needed
keys3 = ['last_sai_cgi_ecgi']  # Adjust if you use ref3

# Apply cleanup to mtandao_df using CGI processing
mtandao_cleaned = cleanup(mtandao_df, kpi_df, ref_df, ref2_df, keys, keys2, meta=False)

# Apply cleanup to soc_df considering it contains CGI data
soc_cleaned = cleanup(soc_df, kpi_df, ref_df, ref2_df, keys, keys2, keys3, meta=False)

print(mtandao_cleaned.head())
print(soc_cleaned.head())

# /........---
