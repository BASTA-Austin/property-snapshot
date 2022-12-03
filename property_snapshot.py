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
# import matplotlib.pyplot as plt
# import contextily as cx

dotenv.load_dotenv()


@st.cache(allow_output_mutation=True)
def load_parcels():
    url = 'https://drive.google.com/uc?id=1ifjaYmsKYha_vr2DxsDN58swHU7lF_k9&confirm=t'
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
def sjoin_on_coord(lat, lng, parcels):
    addy_coords = gpd.points_from_xy([lng], [lat])
    df = gpd.GeoDataFrame(geometry=addy_coords, crs='EPSG:4326')
    df = gpd.sjoin(df, parcels, how='left', predicate='within')
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
    st.write('**Instructions:** Type in an address and see all of the related info we have on that property')
    st.header('Input')
    address = st.text_input('Address to search')
    
    st.header('App status:')
    data_load_state = st.text('Loading TCAD parcels...')

    parcels = load_parcels()
    data_load_state.text('TCAD parcels loaded')

    st.header('Results')
    accuracy, lat, lng = geocode_addr(address)
    df = pd.DataFrame([[lat, lng]], columns=['lat', 'lon'])
    st.write(f"Accuracy of geocode result: {accuracy}. Coordinates: {lat}, {lng}")

    if not (lat and lng):
        return
    
    st.write('Here\'s a map of your coordinate. Is it what you expected?')
    st.map(df)
    propid = sjoin_on_coord(lat, lng, parcels)
    if not propid:
        return
    
    if len(propid) > 1:
        st.write(f'Found more than one property: {propid[:]}')
    else:
        propid = propid[0]
    st.write(f'Found TCAD parcel with property id: {propid}')
    propdat = get_property_data(propid)
    st.subheader('Property Info')
    if not propdat.empty:
        st.write(propdat)

    evdf = get_evictions(propid)
    st.subheader('Evictions')
    if not evdf.empty:
        st.write(f'There have been {len(evdf)} evictions at this property')
        st.write(f'Here are the case numbers for those evictions')
        st.write(evdf.tolist())
    else:
        st.write('We do not have records (since 2014) of evictions at this property')
        


if __name__ == "__main__":
    streamlit_app()
    print('DEPLOYED')