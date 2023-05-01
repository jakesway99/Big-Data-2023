import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import sys

crime_df_pd = pd.read_csv(sys.argv[1])
geo_json_path = "NTA_json.geojson"
neighborhoods = gpd.read_file(geo_json_path)
neighborhood_income_mapping = pd.read_csv("neighborhood_income_data/ntacode_incomes.csv")

# add geometry column to the df, convert to geopandas
crime_df_pd["geometry"] = crime_df_pd.apply(lambda row: Point(row.longitude, row.latitude), axis=1)
shooting_geo_df = gpd.GeoDataFrame(crime_df_pd, geometry="geometry")

# need CRS to match
shooting_geo_df.crs = neighborhoods.crs
crimes_with_neighborhood = gpd.sjoin(shooting_geo_df, neighborhoods, predicate="within")

# drop and rename columns
crimes_with_neighborhood = crimes_with_neighborhood.drop(
    columns=["index_right", "shape_area", "county_fips", "shape_leng"])
crimes_with_neighborhood = crimes_with_neighborhood.rename(columns={"ntaname": "neighborhood"})

crime_nta_income = pd.merge(crimes_with_neighborhood, neighborhood_income_mapping, on="ntacode")

# exclude -1 and blank values. -1 values are places like airport, park/cemetery, etc
crime_nta_income = crime_nta_income[(crime_nta_income['income'] != -1) & (crime_nta_income['income'].notnull())]
print(crime_nta_income[["incident_key", "boro_name", "neighborhood", "income"]])
