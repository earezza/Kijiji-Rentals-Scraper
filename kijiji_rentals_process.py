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
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
#from timezonefinder import TimezoneFinder
import argparse


describe_help = 'python kijiji_rentals_process.py --raw_file ads.csv --output_file ads_cleaned.csv'
parser = argparse.ArgumentParser(description=describe_help)
# User defined options
parser.add_argument('-r', '--raw_file', help='File (.csv) of ads info to process', type=str, default="ads.csv")
parser.add_argument('-o', '--output_file', help='File (.csv) for processed ad data', type=str, default='')
parser.add_argument('--city', help='City to default to for long/lat coordinates if ad location is unsearchable', type=str, default="")
parser.add_argument('--country', help='Country to default to for long/lat coordinates if ad location is unsearchable', type=str, default="Canada")
parser.add_argument('--lat_long', help='Flag if want to get latitudes/longitudes (takes long time), will keep "Location" column either way.', action='store_true', default=False)
args = parser.parse_args()

if args.output_file == '':
    args.output_file = args.raw_file.replace('.csv', '').split('_')[0] + '_processed.csv'

my_dtypes = {'Price': 'float64',
 'Location': 'object',
 'PostingDate': 'object',
 'Poster': 'object',
 'AdId': 'object',
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


fr_en_translation = {
    'Services-inclus-Électricité': 'Utilities-Included-Hydro',
    'Services-inclus-Chauffage': 'Utilities-Included-Heat',
    'Services-inclus-Eau': 'Utilities-Included-Water',
    'Wi-Fi-et-plus-Internet': 'Wi-Fi-and-More-Internet',
    'Électroménagers-Réfrigérateur-/-congélateur': 'Appliances-Fridge-/-Freezer',
    'Espace-extérieur-privé-Balcon': 'Personal-Outdoor-Space-Balcony',
    "Commodités-dans-l'immeuble-Salle-de-sport": 'Amenities-Gym',
    "Commodités-dans-l'immeuble-Piscine": 'Amenities-Pool',
    "Commodités-dans-l'immeuble-Stationnement-pour-vélos": 'Amenities-Bicycle-Parking',
    "Commodités-dans-l'immeuble-Ascenseur": 'Amenities-Elevator-in-Building',
    'Stationnement-inclus': 'Parking-Included',
    'Durée-du-bail': 'Agreement-Type',
    "Date-d'emménagement": 'Move-In-Date',
    'Animaux-acceptés': 'Pet-Friendly',
    'Taille-(pieds-carrés)': 'Size-(sqft)',
    'Meublé': 'Furnished',
    'Air-conditionné': 'Air-Conditioning',
    'Fumeurs-acceptés': 'Smoking-Permitted',
    "Électroménagers-Buanderie-(dans-l'appartement)": 'Appliances-Laundry-(In-Unit)',
    'Électroménagers-Lave-vaisselle': 'Appliances-Dishwasher',
    "Commodités-dans-l'immeuble-Concierge": 'Amenities-Concierge',
    "Commodités-dans-l'immeuble-Sécurité-24-heures-sur-24": 'Amenities-24-Hour-Security',
    "Commodités-dans-l'immeuble-espace-de-stockage": 'Amenities-Storage-Space',
    "Entrées-et-rampes-d'accès-sans-obstacle": 'Barrier-free-Entrances-and-Ramps',
    'Aides-visuelles': 'Visual-Aids',
    'Toilettes-accessibles': 'Accessible-Washrooms-in-Suite',
    'Ascenseurs-accessibles-Accessible-en-fauteuil-roulant': 'Elevator-Accessibility-Features-Wheelchair-accessible',
    'Chambres-à-coucher': 'Bedrooms',
    'Salles-de-bain': 'Bathrooms',
    'Autre-Info': 'More-Info',
    "Électroménagers-Buanderie-(dans-l'immeuble)": 'Appliances-Laundry-(In-Building)',
    'Espace-extérieur-privé-Jardin': 'Personal-Outdoor-Space-Yard',
    'Ascenseurs-accessibles-Affichage-en-braille': 'Elevator-Accessibility-Features-Braille-Labels',
    'Ascenseurs-accessibles-Messages-sonores': 'Elevator-Accessibility-Features-Audio-Prompts',
    'Wi-Fi-et-plus-Câble-/-télé': 'Wi-Fi-and-More-Cable-/-TV'
    }


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


if __name__ == '__main__':
    
    # Main df columns are pd.DataFrame(columns=["Title", "Price", "Location", "Description", "PostingDate", "Poster", "AdURL", "AdId", "ScrapeDate"])
    
    # Load raw data file
    if os.path.isfile(args.raw_file):
        print("Loading raw file %s..."%args.raw_file)
        df_raw = pd.read_csv(args.raw_file)
        df = df_raw.copy()
    else:
        print("%s could not be found..."%args.raw_file)
        exit()

    # Apply translation mapping
    for key, value in fr_en_translation.items():
        if key in df.columns and value in df.columns:
            df[value] = df[value].combine_first(df[key])
            df.drop(columns=key, inplace=True)
    df.rename(mapper=fr_en_translation, axis=1, inplace=True)
    
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
    df['Commercial'] = (df['RentalCategory'] == 'Commercial-Office-Space').astype('boolean')
    df['Residential'] = (df['RentalCategory'].isin(['Apartments-Condos', 'Room-Rental-Roommate', 'Short-Term-Rental'])).astype('boolean')
    #df['RentalType'] = np.where(df['RentalCategory'] == 'Commercial-Office-Space', 'Commercial', 'Residential')
    
    
    # Assign Poster with anonymized IDs
    #df['Poster'] = anonymize_values(df['Poster'])
    
    # Assign AdURL with anonymized IDs
    #df['AdURL'] = anonymize_values(df['AdURL'])
    
    # Assign AdId with anonymized IDs
    #df['AdId'] = anonymize_values(df['AdId'])
    
    # Extract specific rental info from title and description text
    text = (df['Title'].str.lower() + df['Description'].str.lower() + urls_expanded[5].str.lower()).str.replace('\n', '')
    df.drop(columns=['Title', 'Description', 'AdURL'], inplace=True)
    #df['Text'] = text
    
    # Format Price
    df['Price'] = df['Price'].fillna(text.str.extract(r'((\$)(\d+))', re.DOTALL, expand=False)[0])
    df['Price'] = df['Price'].str.replace('$', '', regex=False)
    df['Price'] = df['Price'].str.replace(',', '', regex=False)
    df['Price'] = df['Price'].str.replace(' ', '', regex=False)
    df['Price'] = df['Price'].str.replace("\xa0", "", regex=False)
    # Translate
    df['Price'] = df['Price'].str.replace("Surdemande", "PleaseContact", regex=False)
    df['Price'] = df['Price'].str.replace('Échange', "Swap/Trade", regex=False)
    df['Price'] = df['Price'].str.replace('Gratuit', "Free", regex=False)
    # Replace
    df['Price'].replace(to_replace='PleaseContact', value=np.nan, inplace=True)
    df['Price'].replace(to_replace='Free', value=np.nan, inplace=True)
    df['Price'].replace(to_replace='Swap/Trade', value=np.nan, inplace=True)
    df['Price'] = df['Price'].astype('float32')
    
    
    # Format PostingDate
    df['PostingDate'] = pd.to_datetime(df['PostingDate'], format='%Y-%m-%dT%H:%M:%S', utc=True)
    
    # Modify/translate values
    value_mapping = {
        'Not-Available': False,
        'Not Available': False,
        'Non-disponible': False,
        'Non disponible': False,
        'Yes': True,
        'Oui': True,
        'No': False,
        'Non': False,
        "Seulement-à-l'extérieur": 'Outdoors-only',
        'Stationnement': 'Parking',
        'Entreposage': 'Storage',
        'Limité': 'Limited',
        'Au-mois': 'Month-to-month',
        '1-an': '1-Year',
        '½': '1/2',
        'Studio': 'Bachelor/Studio',
        'Appartement': 'Apartment',
        'Maison': 'House',
        'Maison en rangée': 'Townhouse',
        'Sous-sol': 'Basement'
               }
    
    for c in df.columns[df.dtypes == 'object']:
        df[c] = df[c].map(value_mapping).fillna(df[c])
        
    # Format number of Parking-Included
    if 'Parking-Included' in df.columns:
        df['Parking-Included'] = df['Parking-Included'].str.extract(r'(\d+)')
        df['Parking-Included'] = df['Parking-Included'].astype('float16')

    # Format Move-In-Date
    fr_month_mapping = {
        'janvier': 'January',
        'février': 'February',
        'mars': 'March',
        'avril': 'April',
        'mai': 'May',
        'juin': 'June',
        'juillet': 'July',
        'août': 'August',
        'septembre': 'September',
        'octobre': 'October',
        'novembre': 'November',
        'décembre': 'December'
        }
    
    if 'Move-In-Date' in df.columns:
        for key, value in fr_month_mapping.items():
            df['Move-In-Date'] = df['Move-In-Date'].str.replace(key, value)
        en_movein = pd.to_datetime(df['Move-In-Date'], format='%B-%d,-%Y', utc=True, errors='coerce').dt.date
        fr_movein = pd.to_datetime(df['Move-In-Date'], format='%d-%B-%Y', utc=True, errors='coerce').dt.date
        df['Move-In-Date'] = en_movein.combine_first(fr_movein)
        df['Move-In-Date'] = pd.to_datetime(df['Move-In-Date'])
    
        df['PostingDateDaysInAdvance'] = (df['Move-In-Date'].dt.date - df['PostingDate'].dt.date).dt.days.astype('Int32')
    
    # Format size
    df['Size-(sqft)'] = df['Size-(sqft)'].str.replace(',', '', regex=False)
    df['Size-(sqft)'] = df['Size-(sqft)'].str.replace(' ', '', regex=False)
    df['Size-(sqft)'] = df['Size-(sqft)'].str.replace("\xa0", "", regex=False).astype('float32')
    
    
    # Keywords to regex search (including possible typos)
    girls = ['girl', 'girls', 'female', 'females', 'woman', 'women', 'lady', 'ladies',
             'femme', 'filles', 'fille', 'femmes']
    boys = ['boy', 'boys', 'male', 'males', 'man', 'men', 'guy', 'guys',
            'garçon', 'garçons', 'homme', 'hommes']
    sublet = ['sublet', 'sublets', 'subletting' 'subleting', 'sous-louer', 'sous-location']
    students = ['student', 'students', 'étudiant', 'étudiants']
    preference = ["prefer", 'preference', 'prefered', 'preferred', 'préférer', 'préférence', 'préféré', 'exclusively', 'exclusivement']


    # Find Preference-Male
    df['Preference-Male'] = text.str.extract(r'((\b[^fe]%s\b).(\bonly\b))'%('|'.join(boys)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Preference-Male'] = df['Preference-Male'] | text.str.extract(r'((\bonly\b).(\b[^fe]%s\b))'%('|'.join(boys)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Preference-Male'] = df['Preference-Male'] | text.str.extract(r'((\bfor\b).(\b[^fe]%s\b))'%('|'.join(boys)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Preference-Male'] = df['Preference-Male'] | text.str.extract(r'((\bpour\b).(\b[^fe]%s\b))'%('|'.join(boys)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Preference-Male'] = df['Preference-Male'] | text.str.extract(r'((\b[^fe]%s\b).(\b%s\b))'%('|'.join(boys), '|'.join(preference)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Preference-Male'] = df['Preference-Male'] | text.str.extract(r'((\bp%s\b).(\b[^fe]%s\b))'%('|'.join(preference), '|'.join(boys)), re.DOTALL, expand=False)[0].notnull().astype('bool')          
    df['Preference-Male'] = df['Preference-Male'] | text.str.extract(r'((\ball\b).(\b[^fe]%s\b))'%('|'.join(boys)), re.DOTALL, expand=False)[0].notnull().astype('bool') 
    df['Preference-Male'] = df['Preference-Male'] | text.str.extract(r'((\bno\b)\s(\b%s\b))'%('|'.join(girls)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    
    # Find Preference-Female
    df['Preference-Female'] = text.str.extract(r'((\b%s\b).(\bonly\b))'%('|'.join(girls)), expand=False)[0].notnull().astype('bool')
    df['Preference-Female'] = df['Preference-Female'] | text.str.extract(r'((\bonly\b).(\b%s\b))'%('|'.join(girls)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Preference-Female'] = df['Preference-Female'] | text.str.extract(r'((\bfor\b).(\b%s\b))'%('|'.join(girls)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Preference-Female'] = df['Preference-Female'] | text.str.extract(r'((\bpour\b).(\b%s\b))'%('|'.join(girls)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Preference-Female'] = df['Preference-Female'] | text.str.extract(r'((\b%s\b).(\b%s\b))'%('|'.join(girls), '|'.join(preference)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Preference-Female'] = df['Preference-Female'] | text.str.extract(r'((\b%s\b).(\b%s\b))'%('|'.join(preference), '|'.join(girls)), re.DOTALL, expand=False)[0].notnull().astype('bool')          
    df['Preference-Female'] = df['Preference-Female'] | text.str.extract(r'((\ball\b).(\b%s\b))'%('|'.join(girls)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Preference-Female'] = df['Preference-Female'] | text.str.extract(r'((\bno\b)\s(\b%s\b))'%('|'.join(boys)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    
    # Any keywords contained
    df['Male'] = text.str.extract(r'(\b(%s)\b)'%('|'.join(boys)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    df['Female'] = text.str.extract(r'(\b(%s)\b)'%('|'.join(girls)), re.DOTALL, expand=False)[0].notnull().astype('bool')
    
    # Set False if any ads identified with both male+female
    no_gender_pref = df[(df['Preference-Male'] == True) & (df['Preference-Female'] == True)]
    df.loc[no_gender_pref.index, ['Preference-Male', 'Preference-Female']] = False
    
    # Find Sublets
    df['Sublet'] = text.str.extract(r'(\b%s\b)'%('|'.join(sublet)), re.DOTALL, expand=False).notnull().astype('bool')
    
    # Find Students
    df['Students'] = text.str.extract(r'(\b%s\b)'%('|'.join(students)), re.DOTALL, expand=False).notnull().astype('bool')
    
    # Find Preference-Other
    df['Preference-Any'] = text.str.extract(r'(\b%s\b)'%('|'.join(preference)), re.DOTALL, expand=False).notnull().astype('bool')
    
    
    # Format bedrooms
    beds = ['bed', 'beds', 'bedroom', 'bedrooms', 'chambre', 'chambres']
    baths = ['bath', 'baths', 'bathroom', 'bathrooms', 'salle de bains', 'bains']
    num_map = {'1.0': '1', '2.0': '2', '3.0': '3', '4.0': '4', '5.0': '5', '6.0': '6', '7.0': '7', '8.0': '8', '9.0': '9',
               'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5', 'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'ten': '10'}
    nums = ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten']
    
    # Infer number of beds from text and fill nan
    df['Bedrooms'] = df['Bedrooms'].map(num_map).fillna(df['Bedrooms'])
    df['Bedrooms'] = df['Bedrooms'].fillna(text.str.extract(r'((\b\d\b).(%s))'%('|'.join(beds)), re.DOTALL, expand=False)[1])
    df['Bedrooms'] = df['Bedrooms'].fillna(text.str.extract(r'((\b%s\b).(%s))'%('|'.join(nums), '|'.join(beds)), re.DOTALL, expand=False)[1])
    df['Bedrooms'] = df['Bedrooms'].map(num_map).fillna(df['Bedrooms'])
    df['NumberBedrooms'] = df['Bedrooms'].astype('str')
    df['NumberBedrooms'] = df['NumberBedrooms'].str.replace('Bachelor/Studio', '1')
    df['NumberBedrooms'] = df['NumberBedrooms'].str.replace('Den', '1')
    df['NumberBedrooms'] = df['NumberBedrooms'].fillna(df['Bedrooms'].map(num_map).astype('Int16')).astype(str)
    df['NumberBedrooms'] = (df['NumberBedrooms'].str.extract(r'(\d)')[0].astype('Int16') + df['NumberBedrooms'].str.extract(r'(\d)*(\+)')[1].notnull().astype('Int16'))
    df['NumberBedrooms'] = df['NumberBedrooms'].astype('Int16')
    
    df['Bathrooms'] = df['Bathrooms'].fillna(text.str.extract(r'((\b\d\b).(%s))'%('|'.join(baths)), re.DOTALL, expand=False)[1])
    df['Bathrooms'] = df['Bathrooms'].fillna(text.str.extract(r'((\b%s\b).(%s))'%('|'.join(nums), '|'.join(baths)), re.DOTALL, expand=False)[1])
    df['Bathrooms'] = df['Bathrooms'].map(num_map).fillna(df['Bathrooms'])
    df['NumberBathrooms'] = (df['Bathrooms'].astype('str').str.extract(r'(\d)\.(\d)')[0] + '.' + df['Bathrooms'].astype('str').str.extract(r'(\d)\.(\d)')[1]).astype('float16')
    df['NumberBathrooms'] = df['NumberBathrooms'].fillna(df['Bathrooms'].astype(str).str.extract(r'(\d)').astype('float16')[0])
    df['NumberBathrooms'] = df['NumberBathrooms'].astype('float16')
    
    df['PricePerBedroom'] = (df['Price'] / df['NumberBedrooms']).round(decimals=0)
    
    df['PricePerSqFt'] = (df['Price'] / df['Size-(sqft)']).round(decimals=0)
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['AdId', 'Poster', 'City', 'Price'], keep='first', ignore_index=True)
    
    # Convert datatypes
    #df['Poster'] = df['Poster'].astype(str)
    #df['AdId'] = df['AdId'].astype(str)
    #df = df.convert_dtypes()
    cols = [ c for c in df.columns if c in my_dtypes.keys() ]
    for c in cols:
        df[c] = df[c].astype(my_dtypes[c])
    
    
    print("Writing to file...")
    df.to_csv(args.output_file, sep=',', header=True, index=False, columns=cols)
    
    if args.lat_long:
        print("Continuing to get longitude/latitude info...")
        # Format Location coordinates for map visualizations
        df['Longitude'] = np.nan
        df['Latitude'] = np.nan
        print("Grab a coffee, this will take some time!\n")
        while df['Longitude'].isna().any() and df['Latitude'].isna().any():
            print("Querying coordinates...")
            df = get_coordinates(df)
        df['Longitude'] = df['Longitude'].astype('float32')
        df['Latitude'] = df['Latitude'].astype('float32')

        # Write final data to file
        print("Writing to file...")
        df = df.drop_duplicates(subset=['AdId', 'Poster', 'City', 'Price'], keep='first', ignore_index=True)
        df.to_csv(args.output_file, sep=',', header=True, index=False)
    
    print("Done!")
    
    '''
to_boolean =  ['Appliances-Laundry-(In-Unit)', 'Appliances-Dishwasher', 'Appliances-Fridge-/-Freezer',
    'Personal-Outdoor-Space-Balcony', 'Amenities-Gym', 'Amenities-Bicycle-Parking', 
    'Amenities-Storage-Space', 'Amenities-Elevator-in-Building', 'Furnished',
    'Air-Conditioning', 'Utilities-Included-Hydro', 'Utilities-Included-Heat',
    'Utilities-Included-Water', 'Wi-Fi-and-More-Internet', 'Wi-Fi-and-More-Cable-/-TV',
    'Amenities-Pool', 'Elevator-Accessibility-Features-Wheelchair-accessible',
    'Barrier-free-Entrances-and-Ramps', 'Visual-Aids', 'Accessible-Washrooms-in-Suite',
    'Appliances-Laundry-(In-Building)', 'Personal-Outdoor-Space-Yard', 'Amenities-Concierge',
    'Amenities-24-Hour-Security', 'Elevator-Accessibility-Features-Braille-Labels',
    'Elevator-Accessibility-Features-Audio-Prompts']
'''
    
