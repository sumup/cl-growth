import timeit
import urllib.request, json
import warnings
from collections import defaultdict
from shapely.geometry import Point, Polygon
from tqdm import tqdm
import geopandas as gpd
import numpy as np
from os import path, chdir
chdir(path.join('cl-upload-offline-data'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh



# Geodata URL in JSON format -> https://www.ine.cl/herramientas/portal-de-mapas/geodatos-abiertos (Censo 2017 Distrito Censal: Población, viviendas por área y densidad)
geodata_ine_url = 'https://services5.arcgis.com/hUyD8u3TeZLKPe4T/arcgis/rest/services/Comuna_Densid_Superficie/FeatureServer/0/query?where=1%3D1&outFields=OBJECTID,REGION,NOM_REGION,PROVINCIA,NOM_PROVIN,NOM_COMUNA,COMUNA&outSR=4326&f=json'

# Read JSON from URL and parse as dictionary
with urllib.request.urlopen(geodata_ine_url) as url:
    geodata = json.loads(url.read().decode())

# Create an empty geopandas GeoDataFrame
geodata_ine = gpd.GeoDataFrame(columns=['commune_id', 'commune_name', 'district_id', 'district_name', 'region_id', 'region_name', 'geometry'], geometry=None, crs='EPSG:4326')

# Populate GeoDataFrame with attributes and geometry (Polygon) for every commune
for i in tqdm(range(0, len(geodata['features']))):

    # Insert attributes
    geodata_ine.loc[i, 'commune_id'] = geodata['features'][i]['attributes']['COMUNA']
    geodata_ine.loc[i, 'commune_name'] = geodata['features'][i]['attributes']['NOM_COMUNA']
    geodata_ine.loc[i, 'district_id'] = geodata['features'][i]['attributes']['PROVINCIA']
    geodata_ine.loc[i, 'district_name'] = geodata['features'][i]['attributes']['NOM_PROVIN']
    geodata_ine.loc[i, 'region_id'] = geodata['features'][i]['attributes']['REGION']
    geodata_ine.loc[i, 'region_name'] = geodata['features'][i]['attributes']['NOM_REGION']

    # Obtain coordinates from rings in geometry and transform to Shapely Polygon Format (from list-list to list-tuple)
    coordinates = [tuple(j) for j in geodata['features'][i]['geometry']['rings'][0]]
    
    # Create Shapely polygon and insert it in GeoDataFrame geometry column
    poly = Polygon(coordinates)
    geodata_ine.loc[i, 'geometry'] = poly

query_name = 'transactions_coordinates'
transaction_coordinates = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    )

transactions_gdf = gpd.GeoDataFrame(transaction_coordinates, geometry=gpd.points_from_xy(transaction_coordinates.lon,transaction_coordinates.lat))
transactions_gdf.columns = transactions_gdf.columns.str.lower()
transactions_gdf['commune_id'] = np.nan
transactions_gdf['is_high_risk'] = False
transactions_gdf['is_outside_boundaries'] = False
transactions_gdf['is_null_island'] = False
transactions_gdf['osm_id'] = np.nan 


for i in tqdm(range(0, len(geodata_ine))):
    pip = transactions_gdf.within(geodata_ine.loc[i, 'geometry'])
    # Creating a new gdf and keep only the intersecting records
    transactions_gdf.loc[pip, 'commune_id'] = geodata_ine.loc[i, 'commune_id']



transactions_gdf = transactions_gdf.drop(columns=['geometry']).merge(
    right=geodata_ine,
    on='commune_id',
    how='left'
    )

# Flag outside_boundaries and null_island transactions
transactions_gdf.loc[(transactions_gdf['lat'] != 0) & (transactions_gdf['lon'] != 0) & (transactions_gdf['commune_id'].isna()), 'is_outside_boundaries'] = True
transactions_gdf.loc[(transactions_gdf['lat'] == 0) & (transactions_gdf['lon'] == 0) & (transactions_gdf['commune_id'].isna()), 'is_null_island'] = True

# Prepare data to upload to Snowflake
transactions_geodata = transactions_gdf.loc[:, ['transaction_id', 'commune_id', 'commune_name', 'district_id', 'district_name', 'region_id', 'region_name', 'is_outside_boundaries', 'is_null_island']]
transactions_geodata.columns = transactions_geodata.columns.str.upper()


query_name = 'all_tx_locations'
delete_locations = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    )

dwh().pandas_to_dwh(
    dataframe=transactions_geodata,
    schema_name='analyst_acquisition_cl',
    table_name='partners_transactions_locations',
    if_exists='append'
)
