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

#-------------------(Optional) Convert H3 Cell to Polygon for Plotting-----------------------------------------

from shapely.geometry import Polygon
import geopandas as gpd

# Convert H3 index to polygon
def h3_to_polygon(h3_index):
    boundary = h3.h3_to_geo_boundary(h3_index, geo_json=True)
    return Polygon(boundary)

agg_df = agg_df.reset_index()
agg_df['geometry'] = agg_df['h3_index'].apply(h3_to_polygon)

gdf = gpd.GeoDataFrame(agg_df, geometry='geometry', crs='EPSG:4326')

-----------------------------For every subscriber's (lat, lon), find the nearest school (among 4,700 schools) using the Haversine distance in a scalable way with Dask.----------------------------------
import dask.dataframe as dd
import pandas as pd

# Assume df_schools is small, so convert it to pandas
df_schools = pd.read_csv('schools.csv')  # or any other loading method
df_schools['key'] = 1  # dummy key for cross join

# Your large Dask subscriber data
df_subs = dd.read_parquet('subscriber.parquet')  # or any source
df_subs['key'] = 1

# Do cross join
df_cross = df_subs.merge(df_schools, on='key', suffixes=('_sub', '_school'))

df_cross['distance_km'] = haversine_np(
    df_cross['lat_sub'], df_cross['lon_sub'],
    df_cross['school_lat'], df_cross['school_lon']
)


# Each imsi+timestamp will have multiple rows (one per school). Find the one with min distance.
min_idx = df_cross.groupby(['imsi', 'timestamp'])['distance_km'].idxmin()
# ------------------------------------------------------------------------------------------------------
# Fetch those rows
df_nearest = df_cross.loc[min_idx].compute()


| imsi       | timestamp  | lat\_sub | lon\_sub | nearest\_school\_id | distance\_km |
| ---------- | ---------- | -------- | -------- | ------------------- | ------------ |
| 1234567890 | 2025-06-14 | 18.45    | 77.54    | SCL\_1023           | 0.85 km      |

