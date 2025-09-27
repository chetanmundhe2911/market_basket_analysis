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

#---------------------------------------------------------------------
import dask.dataframe as dd
import pandas as pd
import numpy as np

# Step 1: Create a sample Pandas DataFrame
pdf = pd.DataFrame({
    'id': np.arange(1, 11),
    'value': np.random.randint(1, 100, size=10),
    'category': ['A', 'B'] * 5
})

# Step 2: Convert to Dask DataFrame (lazy)
ddf = dd.from_pandas(pdf, npartitions=2)

# Step 3: Lazy transformations
# Only builds the computation graph, doesn't run yet
filtered = ddf[ddf['value'] > 50]
grouped = filtered.groupby('category')['value'].mean()

# Step 4: This just prints the Dask graph representation
print("Lazy Result (not yet computed):")
print(grouped)

# Step 5: Trigger computation
result = grouped.compute()

print("\nComputed Result:")
print(result)


#----------------------------------------------

