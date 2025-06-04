import dask.dataframe as dd

# Step 1: Read the CSV (this is lazy, nothing is loaded yet)
df = dd.read_csv('your_file.csv')

# Step 2: Apply some transformations (also lazy)
filtered_df = df[df['column'] > 100]
grouped_df = filtered_df.groupby('category_column').mean()

# Step 3: No computation has happened yet!
print(grouped_df)  # Just prints a lazy object

# Step 4: Trigger computation
result = grouped_df.compute()

# Now 'result' is a Pandas DataFrame with the computed output
print(result.head())
