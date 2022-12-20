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
        SELECT property_id FROM property_snapshot WHERE
        ST_Within( ST_SetSRID(ST_Point({lng}, {lat}), 4326), geometry );
    """ 
    df = pd.read_sql_query(sql, conn)
    # df = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col='geometry')
    if df.empty:
        return None
    else:
        return df['property_id'].values


@st.cache
def get_evictions(pids, start, end):
    conn = psycopg2.connect(os.getenv('EVICTIONS_DATABASE_URL'))
    data = pd.read_sql_query(
        f"""
        SELECT cd.case_number, sjd.property_id FROM spatial_joined_data AS sjd
        LEFT JOIN case_detail AS cd ON cd.case_number=sjd.case_number 
        WHERE property_id IN ({','.join([repr(x) for x in pids])})
        AND TO_DATE(cd.date_filed, 'MM/DD/YYYY') >= '{start.strftime("%Y-%m-%d")}'::date
        AND TO_DATE(cd.date_filed, 'MM/DD/YYYY') <= '{end.strftime("%Y-%m-%d")}'::date
        """,
        conn
    )
    return data['case_number']


@st.cache
def get_property_data(pid):
    conn = psycopg2.connect(os.getenv('SNAPSHOT_DATABASE_URL'))
    data = pd.read_sql_query(
        f"""
        SELECT * FROM property_snapshot WHERE property_id='{pid}';
        """,
        conn
    )
    return data


@st.cache
def find_by_owner_add(propdf, pid):
    owneradd = propdf['Owner address'].values[0]
    conn = psycopg2.connect(os.getenv('SNAPSHOT_DATABASE_URL'))
    data = pd.read_sql_query(
        f"""
        SELECT * FROM property_snapshot WHERE owner_address='{owneradd}'
        AND property_id NOT IN ('{pid}');
        """,
        conn
    )
    return data


def streamlit_app():
    st.set_page_config(layout="centered")
    st.title('BASTA Property Snapshot')
    
    st.sidebar.title('About')
    st.sidebar.info(
        """
        This app lets you search properties in Travis county.\n
        It shows relevant information like the existence of housing subsidies, 
        Travis CAD owner information, and whether there were evictions at the property.\n
        It was developed by [BASTA Austin](https://bastaaustin.org)
        """
    )

    st.sidebar.title('Contact')
    st.sidebar.info(
        """
        Peishi Cheng\n
        Data Analyst at BASTA Austin\n
        pcheng@bastaaustin.org
        """
    )
    
    st.info('**Instructions:** Type in an address and see all of the related info we have on that property')
    address = st.text_input('Address to search')

    st.header('Results')
    if not address:
        return
    accuracy, lat, lng = geocode_addr(address)
    df = pd.DataFrame([[lat, lng]], columns=['lat', 'lon'])
    st.write(f"Accuracy of geocode result: **`{accuracy}`**")
    if not (lat and lng):
        return
    st.write(f"Coordinates: **`{lat:.5f}`**, **`{lng:.5f}`**")

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
    tcadlink = f'https://stage.travis.prodigycad.com/property-detail/{propid}'
    st.success(f'_Found TCAD parcel_\n\nProperty ID: **{propid}**\n\nTCAD page link: [{tcadlink}]({tcadlink})')
    propdat = get_property_data(propid)
    st.subheader(f'Property Info    ')
    if propdat.empty:
        st.write('We couldn\'t locate data for that parcel, sorry!')
    else:
        propdat.rename(
            columns={
                'parcel_id': 'Parcel ID',
                'property_id': 'Property ID',
                'parcel_address': 'TCAD parcel address',
                'owner_sep_2022': 'Owner (as of Sept. 2022)',
                'owner_address': 'Owner address',
                'dba_sep_2022': 'DBA (as of Sept. 2022)',
                'cares_act_july_2022': 'CARES Act protections? (known Jul 2022)',
                'cares_act_id': 'NLIHC CARES Act database ID',
                'nhpd_july_2022': 'Federal housing subsidies? (known Jul 2022)',
                'nhpd_id':  'National Housing Preservation Database ID',
                'housing_choice_vouchers': 'Accepted Housing Choice Vouchers (Section 8)?'
            },
            inplace=True
        )
        st.write(propdat[['Parcel ID', 'Property ID', 'TCAD parcel address', 'Owner (as of Sept. 2022)',
            'Owner address', 'DBA (as of Sept. 2022)']].transpose())

        st.subheader('Housing Subsidies')
        st.write(propdat[['CARES Act protections? (known Jul 2022)', 'NLIHC CARES Act database ID',
            'Federal housing subsidies? (known Jul 2022)', 'National Housing Preservation Database ID',
            'Accepted Housing Choice Vouchers (Section 8)?']].transpose())

        st.subheader('Other properties with same owner address:')
        relatedprops = find_by_owner_add(propdat, propid)
        if relatedprops.empty:
            st.write('There were no other properties with the _exact_ same owner address.')
        else:
            st.write(relatedprops[['parcel_id', 'property_id', 'parcel_address', 'owner_sep_2022',
                'owner_address', 'dba_sep_2022']])

    st.subheader('Evictions')
    with st.container():
        c1, c2 = st.columns(2)
        with c1:
            startdate = st.date_input('Evictions start date', value=datetime.date(2014, 1, 1))
        with c2:
            enddate = st.date_input('Evictions end date', value=datetime.date.today())
    evdf = get_evictions([propid], startdate, enddate)
    if evdf.empty:
        st.write('We do not have records (since 2014) of evictions at _this property_')
    else:
        st.write(f'There have been **{len(evdf)}** evictions at _this property_ in the specified date range')
        with st.expander('Here are the case numbers for those evictions'):
            st.write(evdf)
    if relatedprops.empty:
        pass
    else:
        evdf = get_evictions(relatedprops['property_id'].tolist(), startdate, enddate)
        if evdf.empty:
            st.write('We do not have records (since 2014) of evictions at _other properties with the same owner address_')
        else:
            st.write(f'There have been **{len(evdf)}** evictions at _all other properties_ with the same owner address in the specified date range')
            with st.expander('Here are the case numbers for those evictions'):
                st.write(evdf)

if __name__ == "__main__":
    streamlit_app()