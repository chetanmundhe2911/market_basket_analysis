import h3
import pandas as pd

# Sample coordinates
lat, lon = 37.775938728915946, -122.41795063018799

# Convert lat/lon to H3 index at resolution 9
h3_index = h3.geo_to_h3(lat, lon, 9)

# Get hexagon boundary (for plotting)
boundary = h3.h3_to_geo_boundary(h3_index)

print(f"H3 Index: {h3_index}")


# Suppose you have a DataFrame of lat/lon points
df = pd.DataFrame({
    'lat': [37.77, 37.78],
    'lon': [-122.41, -122.42]
})

# Assign H3 index
df['h3_index'] = df.apply(lambda row: h3.geo_to_h3(row['lat'], row['lon'], 9), axis=1)

# Group by H3 index
grouped = df.groupby('h3_index').size().reset_index(name='count')

#------------------------WITH DASK AND PANDAS-----------------Sample Dataset in Dask--------------------------------------
import dask.dataframe as dd
import pandas as pd
import h3

# Sample data (can be replaced by reading from Parquet/CSV/etc.)
data = pd.DataFrame({
    'lat': [37.775, 37.776, 37.770, 37.770],
    'lon': [-122.419, -122.419, -122.421, -122.420],
    'value': [10, 20, 30, 40]
})

# Convert to Dask DataFrame
ddf = dd.from_pandas(data, npartitions=2)

#---------------------------------------------------------

def assign_h3_index(df, resolution=9):
    df['h3_index'] = df.apply(
        lambda row: h3.geo_to_h3(row['lat'], row['lon'], resolution),
        axis=1
    )
    return df

# Apply H3 encoding on each partition
ddf = ddf.map_partitions(assign_h3_index)

#------------------------------------------------------------------
# Group by H3 index and aggregate values
agg_df = ddf.groupby('h3_index')['value'].sum().compute()



