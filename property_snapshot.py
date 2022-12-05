import streamlit as st
import gspread
import dotenv
import os
import json
import pandas as pd
import numpy as np
import geopandas as gpd
import logging
import sys
import datetime
import requests
import shapely
import googlemaps
import psycopg2
import gdown
from sqlalchemy import create_engine
# import matplotlib.pyplot as plt
# import contextily as cx

dotenv.load_dotenv()


@st.cache(allow_output_mutation=True)
def load_parcels():
    url = os.getenv('PARCELS_URL')
    gdown.download(url, output='tcad_parcels.parquet', quiet=True)  # GDrive download of parcels
    parcels = gpd.read_parquet('./tcad_parcels.parquet')
    parcels.rename(columns={'PID_10':'parcel_id', 'PROP_ID':'property_id'}, inplace=True)
    return parcels


@st.cache
def geocode_addr(addr):
    gmaps_key = os.getenv('GMAPS_API_KEY')
    gmaps = googlemaps.Client(key=gmaps_key)

    try:
        result = gmaps.geocode(addr)
    except Exception as error:
        return f'   Geocoder raised Exception:{error}', None, None
    if not result:
        return 'NO RESULT', None, None
    accuracy = result[0]['geometry']['location_type']
    coord = result[0]['geometry']['location']
    cmpnts = result[0]['address_components']
    county = [c['long_name'] for c in cmpnts if 'administrative_area_level_2' in c['types']]
    
    return accuracy, coord['lat'], coord['lng']


@st.cache
def sjoin_on_coord(lat, lng):
    dburl = os.getenv('SNAPSHOT_DATABASE_URL')
    if dburl.startswith("postgres:"):
        dburl = "postgresql" + dburl[len("postgres"):]
    conn = create_engine(dburl)
    sql = f"""
        SELECT * FROM property_snapshot WHERE
        ST_Within( ST_SetSRID(ST_Point({lng}, {lat}), 4326), geometry );
    """
    df = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col='geometry')      
    
    # addy_coords = gpd.points_from_xy([lng], [lat])
    # df = gpd.GeoDataFrame(geometry=addy_coords, crs='EPSG:4326')
    # df = gpd.sjoin(df, parcels, how='left', predicate='within')
    if df.empty:
        return None
    else:
        return df['property_id'].values


@st.cache
def get_evictions(pid):
    conn = psycopg2.connect(os.getenv('EVICTIONS_DATABASE_URL'))
    data = pd.read_sql_query(
        f"""
        SELECT * FROM spatial_joined_data WHERE property_id='{pid}'
        """,
        conn
    )
    return data['case_number']


@st.cache
def get_property_data(pid):
    conn = psycopg2.connect(os.getenv('SNAPSHOT_DATABASE_URL'))
    data = pd.read_sql_query(
        f"""
        SELECT * FROM property_snapshot WHERE property_id='{pid}'
        """,
        conn
    )
    return data


def streamlit_app():
    st.title('BASTA Property Snapshot')
    st.caption('**Instructions:** Type in an address and see all of the related info we have on that property')
    st.header('Input')
    address = st.text_input('Address to search')

    st.header('Results')
    if not address:
        return
    accuracy, lat, lng = geocode_addr(address)
    df = pd.DataFrame([[lat, lng]], columns=['lat', 'lon'])
    st.write(f"Accuracy of geocode result: {accuracy}. Coordinates: {lat}, {lng}")

    if not (lat and lng):
        return
    
    st.write('Here\'s a map of your coordinate. Is it what you expected?')
    st.map(df)
    propid = sjoin_on_coord(lat, lng)
    if not propid:
        return
    
    if len(propid) > 1:
        st.write(f'Found more than one property: {propid[:]}')
    else:
        propid = propid[0]
    st.write(f'Found TCAD parcel with property id: {propid}')
    propdat = get_property_data(propid)
    st.subheader('Property Info')
    if propdat.empty:
        st.write('We couldn\'t locate data for that parcel, sorry!')
    else:
        st.write(f"Parcel ID: {propdat['parcel_id'].values}")
        st.write(f"Property ID: {propdat['property_id'].values}")
        st.write(f"TCAD's parcel address: {propdat['parcel_address'].values}")
        st.write(f"Owner (Sept. 2022): {propdat['owner_sep_2022'].values}")
        st.write(f"Owner address: {propdat['owner_address'].values}")
        st.write(f"DBA (Sept. 2022): {propdat['dba_sep_2022'].values}")
        
        st.subheader('Housing Subsidies')
        st.write(f"CARES Act protections? (as of Jul 2022): {propdat['cares_act_july_2022'].values}")
        st.write(f"Federal housing subsidies? (as of Jul 2022): {propdat['nhpd_july_2022'].values}")
        st.write(f"Accepted Housing Choice Vouchers (Section 8)?: {propdat['housing_choice_vouchers'].values}")


    evdf = get_evictions(propid)
    st.subheader('Evictions')
    if evdf.empty:
        st.write('We do not have records (since 2014) of evictions at this property')
    else:
        st.write(f'There have been **{len(evdf)}** evictions at this property since 2014')
        st.write(f'Here are the case numbers for those evictions')
        st.write(evdf.tolist())
        

if __name__ == "__main__":
    streamlit_app()
    print('DEPLOYED')