# Kijiji-Rentals-Scraper  

Collect all kijiji rentals ads and saves them to file.  
The "-c" parameter should be the URL extension for the desired city when searching Kijiji.   
Subsequent runs using the same input file will append new listings and remove duplicates:  

> python kijiji_rentals_scraper.py -f ads.csv -c h-ottawa/1700185    

Process raw info:  

> python kijiji_rentals_process.py -r ads.csv -o ads_processed.csv  
  
Clean processed data and anonymize to be used for analyzing:  
  
> python kijiji_rentals_clean.py -i ads_processed.csv -o ads_cleaned.csv
