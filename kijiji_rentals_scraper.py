#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 12 18:25:34 2023

Description:
    Queries all kijiji rental ads and saves raw info to .csv file with a timestamp.
    
    Columns include:
        "Title", "Price", "Date", "Location", "Description", "NearestIntersection", "Bedrooms", "Link"
    
    Subsequent running of the script will add the new listings to the provided file.

@author: eric
"""

import os
import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
import tqdm
import argparse


describe_help = 'python kijiji_rentals_scraper.py --file ads.csv'
parser = argparse.ArgumentParser(description=describe_help)
# User defined options
parser.add_argument('-f', '--file', help='File (.csv) to update results, will create if nonexisting', type=str, default="ads.csv")
args = parser.parse_args()


def collect_ad_info(ads, df_new, timestamp):
    
    for a in tqdm.tqdm(ads):

        # Ad info
        title = ' '.join(a.find("div", {"class": "title"}).get_text().split())
        # Skip wanted ads
        if 'Wanted: ' in title:
            continue
        price = ' '.join(a.find("div", {"class": "price"}).get_text().split()).replace('$','').replace(',','')
        date = '%s-%s-%s '%(timestamp.day, timestamp.month, timestamp.year) #+ ' '.join(a.find("span", {"class": "date-posted"}).get_text().split())
        date_posted = ' '.join(a.find("span", {"class": "date-posted"}).get_text().split())
        location = ' '.join(a.find("div", {"class": "location"}).get_text().split()).replace(date_posted, '')
        description = ' '.join(a.find("div", {"class": "description"}).get_text().split())
        #info = ' '.join(a.find("div", {"class": "rental-info"}).get_text().split())
        try:
            nearest_intersection = ' '.join(a.find("span", {"class": "nearest-intersection"}).get_text().split())
        except:
            nearest_intersection = ''
        try:
            bedrooms = ' '.join(a.find("span", {"class": "bedrooms"}).get_text().split()).split(': ')[-1]
        except:
            bedrooms = ''
        link = 'https://www.kijiji.ca' + a.find(href=True)['href']
        
        data = np.array([title, price, date, location, description, nearest_intersection, bedrooms, link])
        
        df_new.loc[len(df_new)] = data
            
        #df_new['Price'] = df_new['Price'].astype(float)
    
    return df_new


if __name__ == '__main__':
    
    # Load previously saved file
    if os.path.isfile(args.file):
        print("Loading %s..."%args.file)
        df_old = pd.read_csv(args.file)
    else:
        print("Creating save file %s..."%args.file)
        df_old = pd.DataFrame(columns=["Title", "Price", "Date", "Location", "Description", "NearestIntersection", "Bedrooms", "Link"])
        
    print("Fetching kijiji.ca...")
    # Kijiji homepage
    HOME_URL = 'https://www.kijiji.ca'
    page = requests.get(HOME_URL)
    soup = BeautifulSoup(page.content, "html.parser")
    
    # Get url extension for rental listings - page 1
    rentals_links = [i for i in soup.find_all(href=True) if 'for-rent' in i['href']]
    page_url = HOME_URL + rentals_links[0]['href']
    
    print("Fetching rental listings...")
    # Get rental listings
    page = requests.get(page_url)
    soup = BeautifulSoup(page.content, "html.parser")
    
    ads = soup.find_all("div", {"class": "search-item top-feature"})
    ads = ads + soup.find_all("div", {"class": "search-item regular-ad"})
    
    
    # DataFrame to save rental info
    df_new = pd.DataFrame(columns=["Title", "Price", "Date", "Location", "Description", "NearestIntersection", "Bedrooms", "Link"])
    timestamp = datetime.datetime.now()
    
    done = False
    while not done:
        
        try:
            page_number = soup.find("span", {"class": "selected"}).get_text()
            print("Collecting ad info page %s..."%page_number)
            
            df_new = collect_ad_info(ads, df_new, timestamp)
        except:
            with open('htmlerror.txt', 'w') as f:
                f.write(str(soup))
            print('Check htmlerror.txt and/or run again...')
            exit()
        
        # To get next page ads
        try:
            page_url = HOME_URL + soup.find("a", {"title": "Next"})['href']
            # Get rental listings
            page = requests.get(page_url)
            soup = BeautifulSoup(page.content, "html.parser")
            ads = soup.find_all("div", {"class": "search-item top-feature"})
            ads = ads + soup.find_all("div", {"class": "search-item regular-ad"})
        except:
            print("No more pages...or run again if failed...")
            with open('htmlerror.txt', 'w') as f:
                f.write(str(soup))
            done = True
    
    print("Updating file, removing duplicate listings...")
    # Add updates to old data
    df = pd.concat([df_old, df_new], ignore_index=True)
    # Remove duplicates
    df.drop_duplicates(subset=['Link'], ignore_index=True, inplace=True)
    df.drop_duplicates(subset=['Title'], ignore_index=True, inplace=True)
    
    print("Writing to file...")
    df.to_csv(args.file, sep=',', header=True, index=False)
    print("Done!")

    