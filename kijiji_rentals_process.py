#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 17:09:49 2023

Description:
    Takes raw kijiji ad info from kijiji_rentals_scraper.py and applies filters
    and processing to produce more structured data output to .csv.
    
    Useful columns appended to data:
        "NumberOfRooms", "PriceAvailable", "RentPrice", "FemaleOnly", "MaleOnly", "Sublet", "PricePerRoom"
    

@author: eric
"""

import os
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tqdm
import argparse


describe_help = 'python kijiji_rentals_process.py --file ads.csv'
parser = argparse.ArgumentParser(description=describe_help)
# User defined options
parser.add_argument('-f', '--file', help='File (.csv) of ads info to filter', type=str, default="ads.csv")
args = parser.parse_args()


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
    

if __name__ == '__main__':
    
    # Load previously saved file
    if os.path.isfile(args.file):
        print("Loading %s..."%args.file)
        df_raw = pd.read_csv(args.file)
        df = df_raw.copy()
    else:
        print("%s could not be found..."%args.file)
        exit()
    
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
    
    print("Collecting female-only rentals...")
    girl_strings = ['for female', 'for females', 'female only', 'females only', 'female-only', 'females-only', 'all female', 'all females',
                   'for girl', 'for girls', 'girl only', 'girls only', 'girl-only', 'girls-only', 'all girl', 'all girls']
    
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
                   'for boy', 'for boys', 'boy only', 'boys only', 'boy-only', 'boys-only', 'all boy', 'all boys']
    
    # Look in title and description for info
    guys = pd.DataFrame()
    for p in guy_strings:
        guys = pd.concat([guys, get_ads_with_pattern(df, 'Title', p)])
        guys.drop_duplicates(inplace=True)
        guys = pd.concat([guys, get_ads_with_pattern(df, 'Description', p)])
        guys.drop_duplicates(inplace=True)
        
    df['MaleOnly'] = df.index.isin(guys.index)
    
    df['PricePerRoom'] = df['RentPrice'] / df['NumberOfRooms']
    
    print("Writing to file...")
    df.to_csv(args.file.replace('.csv', '_cleaned.csv'), sep=',', header=True, index=False)
    print("Done!")
    