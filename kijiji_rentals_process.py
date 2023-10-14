#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 17:09:49 2023

Description:
    Takes raw kijiji ad info from kijiji_rentals_scraper.py and applies filters
    and processing to produce more structured data output to .csv.
    
    Additional columns appended to data:
        "Longitude", "Latitude", 
        "Sublet", 
        "StudentPreferred", "FemalePreferred", "MalePreferred", "OtherPreferred", 
    

@author: eric
"""

import os
import datetime
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tqdm
import time
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
#from timezonefinder import TimezoneFinder
import argparse


describe_help = 'python kijiji_rentals_process.py --file ads.csv'
parser = argparse.ArgumentParser(description=describe_help)
# User defined options
parser.add_argument('-r', '--raw_file', help='File (.csv) of ads info to process', type=str, default="ads.csv")
parser.add_argument('-o', '--output_file', help='File (.csv) for processed ad data', type=str, default="ads_cleaned.csv")
parser.add_argument('--city', help='City to default to for long/lat coordinates if ad location is unsearchable', type=str, default="")
parser.add_argument('--country', help='Country to default to for long/lat coordinates if ad location is unsearchable', type=str, default="Canada")
args = parser.parse_args()

def get_coordinates(df):
    geolocator = Nominatim(user_agent="kijiji", timeout=3)
    geocode = RateLimiter(geolocator.geocode,
                          max_retries=3, 
                          min_delay_seconds=1, 
                          error_wait_seconds=2,
                          swallow_exceptions=True)
    
    # Iterate over locations
    for i in tqdm.tqdm(df.index):
        # Ignore if coordinates already exist
        if np.isnan(df.loc[i, 'Longitude']) and np.isnan(df.loc[i, 'Latitude']):
            try:
                loc = geolocator.geocode(df['Location'][i])
                time.sleep(1)
                # Try again without postal code if nothing returned
                if loc == None:
                    loc = geolocator.geocode(','.join(df['Location'][i].split(',')[:-1]))
                    time.sleep(1)
                # If still no coordinates, use default city + country
                if loc == None:
                    loc = geolocator.geocode(args.city.capitalize() + ', ' + args.country.capitalize())
                    time.sleep(1)
                df.loc[i, 'Longitude'] = loc.longitude
                df.loc[i, 'Latitude'] = loc.latitude
                
            except Exception as e:
                print(e)
                print('Could not retrieve coordinates %s'%df['Location'][i])
                continue
        
    return df


def anonymize_values(series):
    mapping = dict(zip(series.unique(), range(0, len(series.unique()))))
    series = series.map(mapping).astype('uint32')
    return series
    
# pd.DataFrame(columns=["Title", "Price", "Location", "Description", "PostingDate", "Poster", "AdURL", "AdId", "ScrapeDate", "Longitude", "Latitude"])

if __name__ == '__main__':
    
    # Load raw data file
    if os.path.isfile(args.raw_file):
        print("Loading raw file %s..."%args.raw_file)
        df_raw = pd.read_csv(args.raw_file)
        df = df_raw.copy()
    else:
        print("%s could not be found..."%args.raw_file)
        exit()
    
    # Load previous data file if exists
    if os.path.isfile(args.output_file):
        print("Loading cleaned %s..."%args.output_file)
        df_cleaned = pd.read_csv(args.output_file)
    else:
        print("Writing cleaned data to %s"%args.output_file)
    
    
    # Categorize rental ad by listing group
    urls_expanded = df['AdURL'].str.split('/', expand=True)
    df['RentalCategory'] = urls_expanded[3]
    category_mapping = {'v-apartments-condos': 'Apartments-Condos',
                        'v-commercial-office-space': 'Commercial-Office-Space',
                        'v-room-rental-roommate': 'Room-Rental-Roommate',
                        'v-short-term-rental': 'Short-Term-Rental',
                        'v-storage-parking': 'Storage-Parking'}
    df['RentalCategory'] = df['RentalCategory'].map(category_mapping)
    
    # Add columns to distinguish rental type
    df['Commercial'] = (df['RentalCategory'] == 'Commercial-Office-Space').astype('uint8')
    df['Residential'] = (df['RentalCategory'].isin(['Apartments-Condos', 'Room-Rental-Roommate', 'Short-Term-Rental'])).astype('uint8')
    
    # Assign Poster with anonymized IDs
    df['Poster'] = anonymize_values(df['Poster'])
    
    # Assign AdURL with anonymized IDs
    df['AdURL'] = anonymize_values(df['AdURL'])
    
    # Assign AdId with anonymized IDs
    df['AdId'] = anonymize_values(df['AdId'])
        
    # Format Price
    df['Price'] = df['Price'].str.replace('$', '', regex=False)
    df['Price'] = df['Price'].str.replace(',', '', regex=False)
    df['Price'].replace(to_replace='Please Contact', value=np.nan, inplace=True)
    df['Price'].replace(to_replace='Free', value=np.nan, inplace=True)
    df['Price'].replace(to_replace='Swap/Trade', value=np.nan, inplace=True)
    df['Price'] = df['Price'].astype('float32')
    
    # Format PostingDate
    df['PostingDate'] = pd.to_datetime(df['PostingDate'], format='%Y-%m-%dT%H:%M:%S', utc=True)
    
    # Modify binary values
    for c in df.columns[df.dtypes == 'object']:
        if 'Yes' in df[c].unique() or 'No' in df[c].unique():
            df[c].replace(to_replace='Not-Available', value='No', inplace=True)
            df[c] = df[c].map({'Yes': 1, 'No': 0}, na_action='ignore').astype('float16')
    
    # Format Move-In-Date
    if 'Move-In-Date' in df.columns:
        df['Move-In-Date'] = pd.to_datetime(df['Move-In-Date'], format='%B-%d,-%Y', utc=True)
    
    # Format number of Parking-Included
    if 'Parking-Included' in df.columns:
        df['Parking-Included'].replace(to_replace='Not-Available', value='0', inplace=True)
        df['Parking-Included'].replace(to_replace='No', value='0', inplace=True)
        df['Parking-Included'] = df['Parking-Included'].str.extract(r'(\d+)')
        df['Parking-Included'] = df['Parking-Included'].astype('float16')
        
    
    # Format Location coordinates for map visualizations
    df['Longitude'] = np.nan
    df['Latitude'] = np.nan
    print("Grab a coffee, this will take some time!\n")
    while df['Longitude'].isna().any() and df['Latitude'].isna().any():
        print("Querying coordinates...")
        df = get_coordinates(df)
    df['Longitude'] = df['Longitude'].astype('float32')
    df['Latitude'] = df['Latitude'].astype('float32')
    
    '''
    # ------------------- Difficult Part ------------------
    # Extract specific rental info from title and description text
    df['Title'] = df['Title'].str.lower()
    df['Description'] = df['Description'].str.lower()
    text = (df['Title'] + df['Description']).str.replace('\n', '')
    
    # Keywords to search
    girls = ['girl', 'female', 'woman', 'women', 'lady', 'ladies']
    boys = ['boy', 'male', 'man', 'men', 'guy', 'dude']
    sublet = ['sublet']
    students = ['student']
    preference = ["prefer", "only"]
    
    
    # Use regex
    text.str.extract(r'(\S+)')
    
    
    # Find Preferene-Male
    df['Preference-Male'] = ( text.str.contains('|'.join(boys)) & (~text.str.contains('|'.join(girls))) )
    
    # Find Preference-Female
    df['Preference-Feale'] = ( (text.str.contains('|'.join(girls))) )
    
    # Find Sublets
    df['Sublet'] = ( (text.str.contains('|'.join(sublet))) )
    
    # Find Students
    df['Students'] = ( (text.str.contains('|'.join(students))) )
    
    # Find Preference-Other
    df['Preference'] = ( (text.str.contains('|'.join(preference))) )
    
    text[text.str.contains('|'.join(girls))]
    text[( text.str.contains('|'.join(boys)) ) & (~text.str.contains('|'.join(girls)))]
    '''
    
    # Write final data to file
    print("Writing to file...")
    df.to_csv(args.output_file, sep=',', header=True, index=False)
    print("Done!")
    