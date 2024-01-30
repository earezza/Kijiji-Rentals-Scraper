#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 12 18:25:34 2023

Description:
    Queries all kijiji rental ads and saves raw info to .csv file with a timestamp.
    
    Columns include:
        "Title", "Price", "Location", "Description", "PostingDate", "Poster", "AdURL", "ScrapeDate"
    
    Subsequent running of the script will add the new listings to the provided file.


version: 2 (updated for Kijiji website changes from v1 compatibility)


TODO:
    Make run parallel with multiprocessing since it takes ~2.5 hours on first run (at 88 pages, 40 ads/page)

@author: eric
"""

import os
import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import re
import tqdm
import argparse


describe_help = 'python kijiji_rentals_scraper.py --file ads.csv --city ottawa'
parser = argparse.ArgumentParser(description=describe_help)
# User defined options
parser.add_argument('-f', '--file', help='File (.csv) to update results, will create if nonexisting', type=str, default="ads.csv")
parser.add_argument('-c', '--city', help='City to search ads', type=str, default="")
args = parser.parse_args()


def collect_ads_info(ad_links, df_new):
    
    row = df_new.shape[0]
    for ad in tqdm.tqdm(ad_links):
        
        # ignore links to images
        if 'http' in ad['href'] and 'imageNumber=' in ad['href']:
            continue

        ad_url = HOME_URL[:-1] + ad['href']
        
        try:
            session = requests.Session()
            retry = Retry(connect=3, backoff_factor=0.5)
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            ad_page = requests.get(ad_url, timeout=120)
            ad_soup = BeautifulSoup(ad_page.content, "html.parser")
            #time.sleep(3)
        except Exception as e:
            print(e)
            print("Unable to get: %s...\n"%ad_url)
            continue
        
        try:
            title = '_'.join(np.unique(ad_soup.find_all('h1', re.compile("title*"))))
            price = '_'.join(np.unique(ad_soup.find_all('span', re.compile("currentPrice*"))))
            location = '_'.join(np.unique(ad_soup.find_all('span', re.compile("address"))))
            ad_id = '_'.join(np.unique(ad_soup.find_all('a', re.compile("adId*"))))
            try:
                ad_post_date = ad_soup.find('div', re.compile("datePosted*")).time["datetime"]
            except:
                ad_post_date = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%zZ").replace('+0', '')
            ad_poster_profile = HOME_URL[:-1] + ad_soup.find('a', re.compile("avatarLink*"))["href"]
            
            descriptions = ad_soup.find_all('p')
            description = ''
            for d in descriptions[:-1]:
                description = description + "\n" + d.get_text()
            
            # Add listing info to df
            df_new.loc[row, "Title"] = title
            df_new.loc[row, "Price"] = price
            df_new.loc[row, "Location"] = location
            df_new.loc[row, "Description"] = description
            df_new.loc[row, "PostingDate"] = ad_post_date
            df_new.loc[row, "Poster"] = ad_poster_profile
            df_new.loc[row, "AdURL"] = ad_url
            df_new.loc[row, 'AdId'] = ad_id
            df_new.loc[row, "ScrapeDate"] = scrape_date
        except Exception as e:
            print(e)
            print("Unable to parse data from: %s...\n"%ad_url)
            row += 1
            continue
        
        try:
            title_attributes = ad_soup.find('div', re.compile("titleAttributes*"))
            if title_attributes != None:
                title_details = title_attributes.find_all('li')
                for detail in title_details:
                    if 'unittype' in detail.find('use')['xlink:href']:
                        df_new.loc[row, "UnitType"] = detail.get_text()
                    elif 'numberbedrooms' in detail.find('use')['xlink:href']:
                        df_new.loc[row, "Bedrooms"] = pd.Series(detail.get_text()).str.extract(r'(\d+)')[0][0] # detail.get_text().split()[-1]
                    elif 'numberbathrooms' in detail.find('use')['xlink:href']:
                        df_new.loc[row, "Bathrooms"] = pd.Series(detail.get_text()).str.extract(r'(\d+)')[0][0] # detail.get_text().split()[-1]
                    elif 'furnished' in detail.find('use')['xlink:href']:
                        df_new.loc[row, "Furnished"] = detail.get_text().split()[-1]
                    elif 'petsallowed' in detail.find('use')['xlink:href']:
                        df_new.loc[row, "PetFriendly"] = detail.get_text().split()[-1]
            else:
                title_attributes = ad_soup.find('div', re.compile("attributeList*"))
                if title_attributes != None:
                    title_details = title_attributes.find_all('ul')
                    for detail_list in title_details:
                        details = detail_list.find_all('li')
                        for detail in details:
                            df_new.loc[row, '-'.join(detail.find('dt').get_text().split())] = detail.find('dd').get_text()

        except Exception as e:
            print(e)
            print("Unable to get unit details from: %s...\n"%ad_url)
        
        try:
            ad_attributes = {}
            
            # Get rental attributes if ad has attribute cards
            attribute_groups = ad_soup.find_all('li', re.compile("attributeGroupContainer*"))
            for attributes in attribute_groups:
                attribute = attributes.find('div')
                attribute_type = attribute.find('h4').get_text().replace(' ', '-')
                
                for a in attribute.find_all('li'):
                    if a.get_text() != '':
                        if a.find('use') == None:
                            ad_attributes[attribute_type + '-' + a.get_text().replace(' ', '-')] = 'Yes'
                        elif a.find('use') != None:
                            if 'yes' in a.find('use')['xlink:href']:
                                ad_attributes[attribute_type + '-' + a.get_text().replace(' ', '-')] = 'Yes'
                            elif 'no' in a.find('use')['xlink:href']:
                                ad_attributes[attribute_type + '-' + a.get_text().replace(' ', '-')] = 'No'
    
            # Repeat for rental attributes with two-line attributes      
            attribute_twolines = ad_soup.find_all('li', re.compile("twoLinesAttribute*"))
            for attribute in attribute_twolines:
                attribute_type = attribute.find("dt").get_text().replace(' ', '-')
                attribute_details = attribute.find("dd").get_text().replace(' ', '-')
                ad_attributes[ attribute_type ] = attribute_details
                
            # Add listing features to df
            for feature, value in ad_attributes.items():
                df_new.loc[row, feature] = value
        except Exception as e:
            print(e)
            print("Unable to get ad features for: %s...\n"%ad_url)
            row += 1
            continue
        
        row += 1
    
    return df_new

def write_data(df_old, df_new, filename):
    
    print("Updating file, removing duplicate listings...")
    # Add updates to old data
    df = pd.concat([df_old, df_new], ignore_index=True)
    # Remove duplicates
    df.drop_duplicates(subset=['AdURL'], ignore_index=True, inplace=True)
    df.drop_duplicates(subset=['AdId'], ignore_index=True, inplace=True)
    df.drop_duplicates(subset=['Title', 'Location', 'Poster', 'Description'], ignore_index=True, inplace=True)
    
    cities = df['AdURL'].str.split('/', expand=True)
    df['City'] = cities[4]
    
    print("Writing to file...")
    df.to_csv(filename, sep=',', header=True, index=False)
    
    return df


if __name__ == '__main__':
    
    scrape_date = "%s-%s-%s"%(datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day)
    
    # Load previously saved file
    if os.path.isfile(args.file):
        print("Loading %s..."%args.file)
        df_old = pd.read_csv(args.file)
    else:
        print("Creating save file %s..."%args.file)
        #df_old = pd.DataFrame(columns=["Title", "Price", "Date", "Location", "Description", "NearestIntersection", "Bedrooms", "Link"])
        df_old = pd.DataFrame(columns=["Title", "Price", "Location", "Description", "PostingDate", "Poster", "AdURL", "AdId", "ScrapeDate"])
        
        
    print("Fetching kijiji.ca...")
    # Kijiji homepage
    HOME_URL = 'https://www.kijiji.ca/'
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    page = requests.get(HOME_URL + args.city, timeout=120)
    soup = BeautifulSoup(page.content, "html.parser")
    
    # Get url extension for rental listings - page 1
    try:
        rentals_links = [i for i in soup.find_all(href=True) if 'for-rent' in i['href']]
        rentals_url = HOME_URL[:-1] + rentals_links[0]['href']
    except IndexError:
        print("Check URL of city and input --city with URL after %s"%HOME_URL)
        exit()
    
    # DataFrame to save rental info
    df_new = pd.DataFrame(columns=["Title", "Price", "Location", "Description", "PostingDate", "Poster", "AdURL", "AdId", "ScrapeDate"])
    
    # Start collecting ads page-by-page
    print("Fetching rental listings...")
    done = False
    page_number = 1
    print("Grab a coffee, this will take some time!\n")
    while not done:
        
        try:
            print("Page %s\n"%page_number)
            session = requests.Session()
            retry = Retry(connect=3, backoff_factor=0.5)
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            page = requests.get(rentals_url, timeout=120)
            soup = BeautifulSoup(page.content, "html.parser")
            
            # Get list of ads
            ad_list = soup.find_all('ul', {'data-testid': 'srp-search-list'})
            
            # ad_list contains 2 elements, first is "featured" ads, second is all relevant ads
            for element in range(1, len(ad_list)):
                ads = soup.find_all('ul', {'data-testid': 'srp-search-list'})[element]
                ad_links = ads.find_all("a", href=True)
                ad_links = [ ad for ad in ad_links if 'http' not in ad['href'] and 'imageNumber=' not in ad['href'] ]
                
                # Remove any if already exist in old data
                ad_links = [ ad for ad in ad_links if str(HOME_URL[:-1] + ad['href']) not in df_old['AdURL'].values ]
                
                df_new = collect_ads_info(ad_links, df_new)
                
            # To get next page ads
            try:
                next_html = soup.find('li', {'data-testid': 'pagination-next-link'})
                if next_html != None:
                    rentals_url = next_html.find('a', href=True)['href']
                else:
                    done = True
            except:
                print("No more pages...or run again if failed...")
                with open('htmlerror.txt', 'w') as f:
                    f.write(str(soup))
                done = True
            
            page_number += 1
            
        except KeyboardInterrupt:
            done = True
        
        except ConnectionError as e:
            print(e)
            
        finally:
            # Write to file in case errors or ending session
            df = write_data(df_old, df_new, args.file)
    
    # Write to file
    df = write_data(df_old, df_new, args.file)
    print("Done!")

    
