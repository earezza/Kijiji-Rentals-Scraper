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

'''
beds_map = {'1 + Den': '2', '2 + Den': '3', 
           '3 + Den': '4', '4 + Den': '5', 
           '5 + Den': '6', '6 + Den': '7',
           '7 + Den': '8', 'Bachelor/Studio': '1',
           '1': '1', '2': '2', '3': '3', '4': '4', '5': '5', '6': '6', '7': '7', '8': '8'
           }

number_map = {'zero': '0', 'one': '1', 'two': '2', 'three': '3',
              'four': '4', 'five': '5', 'six': '6', 'seven': '7',
              'eight': '8', 'nine': '9', 'ten': '10'}

price_not_listed = ['Please Contact', 'Swap / Trade', 'Free']


def get_num_from_ambiguities(df, column, pattern, limit=10):
    df_temp = df.copy()
    colsearch = df_temp[column].str.lower()
    # Get number before/after pattern
    temp = colsearch.str.split(pattern, expand=True) # number typically in 0th column (e.g. 1bd)
    temp = temp[0]
    temp_list = temp.str.split()
    # Get number (would be in last word/number in 0 column)
    vals = pd.Series([ temp_list.loc[i][-1] if len(temp_list.loc[i]) > 0 else np.nan for i in temp_list.index.values ])
    vals.index = colsearch.index
    vals = vals.dropna()
    vals = vals[vals.str.isdigit()].astype(int)
    vals = vals[vals < limit]
    
    return vals

def get_ads_with_pattern(df, column, pattern):
    df_temp = df.copy()
    colsearch = df_temp[column].str.lower()
    
    return df_temp[colsearch.str.contains(pattern)]
'''


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
    series = series.map(mapping).astype('int32')
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
    for c in df.columns:
        print(c)
        if 'Yes' in df[c].values or 'No' in df[c].values:
            print('\tModifying...\n')
            df[c] = df[c].map({'Yes': 1, 'No': 0}, na_action='ignore').astype('Int16')
    
    # Format Move-In-Date
    if 'Move-In-Date' in df.columns:
        df['Move-In-Date'] = pd.to_datetime(df['Move-In-Date'], format='%B-%d,-%Y', utc=True)
    
    # Format number of Parking-Included
    if 'Parking-Included' in df.columns:
        df['Parking-Included'].replace(to_replace='Not-Available', value='0', inplace=True)
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
    
    
    
    # Extract specific rental info from description
    df['Description'] = df['Description'].str.lower()
    
    
    
    
    
    
    
    '''
    # Fill in number of rooms from raw data and searching ad title + description 
    print("Filling in number of rooms info...")
    bed_strings = ['bd', 'bed', 'room', 'bdr', 'bedr', 'bedrm', 'bdrm', 'bdroom', 'bedroom']
    no_beds = df[df['Bedrooms'].isna()]
    
    # Look in title and description for info
    queries = []
    for p in bed_strings:
        queries.append(get_num_from_ambiguities(no_beds, 'Title', p, limit=8))
        queries.append(get_num_from_ambiguities(no_beds, 'Description', p, limit=8))
    
    # Consolidate queries to select most represented number of rooms for each ad
    num_beds = pd.concat(queries, axis=1)
    num_beds = num_beds.mode(axis=1, dropna=True)
    num_beds_unambiguous = num_beds[num_beds.notna().sum(axis=1) == 1][0].astype(int)
    #num_beds_ambiguous = num_beds[num_beds.notna().sum(axis=1) > 1]
    
    # Add column to df to include number of rooms found by original df, or by searching title and description
    df['NumberOfRooms'] = df['Bedrooms'].map(beds_map)
    df['NumberOfRooms'].loc[num_beds_unambiguous.index] = num_beds_unambiguous
    df['NumberOfRooms'] = df['NumberOfRooms'].fillna(0).astype(int)
    df['NumberOfRooms'] = df['NumberOfRooms'].replace(0, np.nan)
    
    # Get ads without prices and then remove
    df_no_price = df[df['Price'].isin(price_not_listed)]
    
    print("Removing ads with no price listed...")
    # Indicate ads with no price
    has_price = df[~df['Price'].isin(price_not_listed)]
    df['PriceAvailable'] = df['Price']
    df['PriceAvailable'] = df.index.isin(has_price.index)
    
    
    # Use average for ads with multiple prices
    df['Price'] = df['Price'].str.replace('Please Contact', '')
    df['Price'] = df['Price'].str.replace('Swap / Trade', '')
    df['Price'] = df['Price'].str.replace('Free', '')
    prices = df['Price'].str.split(expand=True)
    prices = prices.astype(float)
    prices = prices.mean(axis=1)
    df['RentPrice'] = df['Price']
    df['RentPrice'].loc[prices.index] = prices
    df['RentPrice'] = df['RentPrice'].astype(float)
    
    sublets = pd.DataFrame()
    sublets = pd.concat([sublets, get_ads_with_pattern(df, 'Title', 'sublet')])
    sublets = pd.concat([sublets, get_ads_with_pattern(df, 'Description', 'sublet')])
    sublets.drop_duplicates(inplace=True)
    df['Sublet'] = df.index.isin(sublets.index)
    
    print("Collecting student-only rentals...")
    student_strings = ['student only', 'students only', 'student-only', 'students-only', 'all student', 'all students',
                   'for student', 'for students', 'student only', 'students only', 'student-only', 'students-only', 'all student', 'all students',
                   'student preferred', 'students preferred', 'student preferred', 'students preferred']
    
    # Look in title and description for info
    students = pd.DataFrame()
    for p in student_strings:
        students = pd.concat([students, get_ads_with_pattern(df, 'Title', p)])
        students.drop_duplicates(inplace=True)
        students = pd.concat([students, get_ads_with_pattern(df, 'Description', p)])
        students.drop_duplicates(inplace=True)
        
    df['StudentOnly'] = df.index.isin(students.index)
    
    print("Collecting female-only rentals...")
    girl_strings = ['for female', 'for females', 'female only', 'females only', 'female-only', 'females-only', 'all female', 'all females',
                   'for girl', 'for girls', 'girl only', 'girls only', 'girl-only', 'girls-only', 'all girl', 'all girls',
                   'female preferred', 'females preferred', 'girl preferred', 'girls preferred',
                   'lady', 'ladies']
    
    # Look in title and description for info
    girls = pd.DataFrame()
    for p in girl_strings:
        girls = pd.concat([girls, get_ads_with_pattern(df, 'Title', p)])
        girls.drop_duplicates(inplace=True)
        girls = pd.concat([girls, get_ads_with_pattern(df, 'Description', p)])
        girls.drop_duplicates(inplace=True)
        
    df['FemaleOnly'] = df.index.isin(girls.index)
        
    print("Collecting male-only rentals...")
    guy_strings = ['for male', 'for males', ' male only', ' males only', 'male-only', 'males-only', 'all male', 'all males',
                   'for boy', 'for boys', 'boy only', 'boys only', 'boy-only', 'boys-only', 'all boy', 'all boys',
                   ' male preferred', ' males preferred', 'boy preferred', 'boys preferred',
                   ' man', ' men']
    
    # Look in title and description for info
    guys = pd.DataFrame()
    for p in guy_strings:
        guys = pd.concat([guys, get_ads_with_pattern(df, 'Title', p)])
        guys.drop_duplicates(inplace=True)
        guys = pd.concat([guys, get_ads_with_pattern(df, 'Description', p)])
        guys.drop_duplicates(inplace=True)
        
    df['MaleOnly'] = df.index.isin(guys.index)
    
    df['PricePerRoom'] = df['RentPrice'] / df['NumberOfRooms']
    '''
    print("Writing to file...")
    df.to_csv(args.output_file, sep=',', header=True, index=False)
    print("Done!")
    