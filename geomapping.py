import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import sys

crime_df_pd = pd.read_csv(sys.argv[1])
output_name = sys.argv[2]
geo_json_path = "NTA_json.geojson"
neighborhoods = gpd.read_file(geo_json_path)

# add geometry column to the df, convert to geopandas
crime_df_pd["geometry"] = crime_df_pd.apply(lambda row: Point(row.longitude, row.latitude), axis=1)
shooting_geo_df = gpd.GeoDataFrame(crime_df_pd, geometry="geometry")

# need CRS to match
shooting_geo_df.crs = neighborhoods.crs
crimes_with_neighborhood = gpd.sjoin(shooting_geo_df, neighborhoods, predicate="within")

# drop and rename columns
crimes_with_neighborhood = crimes_with_neighborhood .drop(columns=["index_right", "shape_area", "county_fips", "shape_leng"])
crimes_with_neighborhood = crimes_with_neighborhood .rename(columns={"ntaname": "neighborhood"})

neighborhood_income_mapping = pd.read_csv("ntacode_incomes.csv")

crime_nta_income = pd.merge(crimes_with_neighborhood, neighborhood_income_mapping, on="ntacode")

# exclude -1 and blank values. -1 values are places like airport, park/cemetery, etc
crime_nta_income = crime_nta_income[(crime_nta_income['income'] != -1) & (crime_nta_income['income'].notnull())]
crime_nta_income.to_csv(f"{output_name}_nta_income.csv", index=False)


