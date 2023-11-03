#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  2 17:35:28 2023

@author: eric
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse


describe_help = 'python kijiji_rentals_clean.py --input_file ads_processed.csv --output_file ads_cleaned.csv'
parser = argparse.ArgumentParser(description=describe_help)
# User defined options
parser.add_argument('-i', '--input_file', help='File (.csv) of processed ads to be clean', type=str, default="ads_processed.csv")
parser.add_argument('-o', '--output_file', help='File (.csv) output', type=str, default='')
args = parser.parse_args()

if args.output_file == '':
    args.output_file = args.input_file.replace('_processed.csv', '').split('_')[0] + '_cleaned.csv'

my_dtypes = {'Price': 'float64',
 'Location': 'object',
 'PostingDate': 'object',
 'Poster': 'int64',
 'AdId': 'int64',
 'ScrapeDate': 'object',
 'UnitType': 'object',
 'Bedrooms': 'object',
 'Bathrooms': 'object',
 'Appliances-Laundry-(In-Unit)': 'boolean',
 'Appliances-Dishwasher': 'boolean',
 'Appliances-Fridge-/-Freezer': 'boolean',
 'Personal-Outdoor-Space-Balcony': 'boolean',
 'Amenities-Gym': 'boolean',
 'Amenities-Bicycle-Parking': 'boolean',
 'Amenities-Storage-Space': 'boolean',
 'Amenities-Elevator-in-Building': 'boolean',
 'Parking-Included': 'float64',
 'Agreement-Type': 'object',
 'Move-In-Date': 'object',
 'Pet-Friendly': 'object',
 'Size-(sqft)': 'float64',
 'Furnished': 'boolean',
 'Air-Conditioning': 'boolean',
 'Smoking-Permitted': 'object',
 'Utilities-Included-Hydro': 'boolean',
 'Utilities-Included-Heat': 'boolean',
 'Utilities-Included-Water': 'boolean',
 'Wi-Fi-and-More-Internet': 'boolean',
 'Wi-Fi-and-More-Cable-/-TV': 'boolean',
 'Amenities-Pool': 'boolean',
 'Elevator-Accessibility-Features-Wheelchair-accessible': 'boolean',
 'Barrier-free-Entrances-and-Ramps': 'boolean',
 'Visual-Aids': 'boolean',
 'Accessible-Washrooms-in-Suite': 'boolean',
 'Appliances-Laundry-(In-Building)': 'boolean',
 'Personal-Outdoor-Space-Yard': 'boolean',
 'Amenities-Concierge': 'boolean',
 'Amenities-24-Hour-Security': 'boolean',
 'More-Info': 'object',
 'Elevator-Accessibility-Features-Braille-Labels': 'boolean',
 'City': 'object',
 'RentalCategory': 'object',
 'Commercial': 'boolean',
 'Residential': 'boolean',
 'PostingDateDaysInAdvance': 'float64',
 'Preference-Male': 'bool',
 'Preference-Female': 'bool',
 'Male': 'bool',
 'Female': 'bool',
 'Sublet': 'bool',
 'Students': 'bool',
 'Preference-Any': 'bool',
 'NumberBedrooms': 'float64',
 'NumberBathrooms': 'float64',
 'PricePerBedroom': 'float64',
 'PricePerSqFt': 'float64',
 'Elevator-Accessibility-Features-Audio-Prompts': 'boolean'}


if __name__ == '__main__':
    

    # Load data file
    if os.path.isfile(args.input_file):
        print("Loading raw file %s..."%args.input_file)
        df_raw = pd.read_csv(args.input_file, dtype=my_dtypes)
        df = df_raw.copy()
    else:
        print("%s could not be found..."%args.input_file)
        exit()
        
    
    # Filter out extremes and missing prices
    df = df[df['Price'] <= max(df['Price'].quantile(q=0.99), 10000)]
    df = df.dropna(subset=['Price'])
    df = df.dropna(subset=['PricePerBedroom'])
    df = df[~np.isinf(df['PricePerBedroom'])]
    df = df.reset_index(drop=True)
    
    # Filter out missing values for important features
    df = df.dropna(subset=['UnitType'])
    df = df[df['UnitType'] != 'False']
    
    df = df.dropna(subset=['Agreement-Type'])
    df = df[df['Agreement-Type'] != 'False']
    
    df = df.dropna(subset=['City'])
    df = df.dropna(subset=['RentalCategory'])
    
    df = df.dropna(subset=['PricePerSqFt'])
    
    # Remove any duplicate liistings
    df = df.drop_duplicates(subset=['AdId', 'Poster', 'City', 'Price'], keep='first')
    df = df.reset_index(drop=True)
    
    print("Writing to file...")
    df.to_csv(args.output_file, sep=',', header=True, index=False)