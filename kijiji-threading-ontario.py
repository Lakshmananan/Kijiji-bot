# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a scraper for Kijiji classifieds.
"""
print('00 - Starting up')
import threading
import time as timer
from selenium import webdriver
import requests
from bs4 import BeautifulSoup
import pandas as pd
print('01 - Packages loaded')

driver = webdriver.Chrome(executable_path=r"C:\Users\laksh\Desktop\Scrapers\Kijiji-bot\chromedriver.exe")
print('02 - Chrome Launched')

driver.get('https://www.kijiji.ca/b-motorcycles/ontario/c30l9004?ad=offering')
assert "Kijiji" in driver.title
print('03 - Kijiji Loaded')

ele = driver.find_element_by_xpath('//*[@id="mainPageContent"]/div[3]/div[3]/div[1]/div/div[2]')
word_list = ele.text.split();
count = word_list[-2]
count = int(count.replace(",", ""))
print(f'04 - Number of ads found: {count} ')
page_count = int(count / 40) + (count % 40 > 0)
print(f'05 - Number of pages found: {page_count} ')


pages = []
for page in range(1, page_count + 1):
    pages.append(f'https://www.kijiji.ca/b-motorcycles/ontario/page-{page}/c30l9004?ad=offering')

print('06 - Shutting down Selenium')
driver.close()


print('07 - Started concating all ads from pages')
pages_count = len(pages)
links = []

def fetch_pages(page):
    global pages_count
    site = requests.get(page)
    soup = BeautifulSoup(site.text, 'html.parser')
    
    for a in soup.find_all('a', href=True):
        links.append(a['href'])
    pages_count = pages_count - 1
    print(f'08 - Done parsing a page. {pages_count} remaining')

threads = [threading.Thread(target=fetch_pages, args=(page,)) for page in pages]
for t in threads:
    t.start()

for t in threads:
    t.join()
    
links = list(filter(lambda k: '/v-' in k, links))
print('09 - ads filtered') 

print('10 - Dataset initiated') 

counter = len(links)
data = []

def fetch_links(link):
    global counter
    url = 'https://www.kijiji.ca' + link
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    cats = []
    for dt in soup.find_all('dt'):
        cats.append(dt.text)
    
    vals = []
    for dd in soup.find_all('dd'):
        vals.append(dd.text)
    
    posted = []
    for time in soup.find_all('time'):
        posted.append(time.text)
    
    location = []
    for loc in soup.findAll("span", {"class": "address-3617944557"}):
        location.append(loc.text)
        
    price = []
    for cost in soup.findAll("span", {"class": "currentPrice-2842943473"}):
        price.append(cost.text)
    
    d = {'Category': cats, 'Values':vals}
    df = pd.DataFrame(d)
    
    make, model, colour, engine, kilo, year = 0,0,0,0,0,0
    if len(location) == 0:
        location = ['Missing']
    if len(price) == 0:
        price = ['Missing']
    if len(posted) == 0:
        posted = ['Missing']
        
    for i in range(len(df)):
        if df.iloc[i].Category == "Make":
            make = df.iloc[i].Values
        elif df.iloc[i].Category == "Model":
            model = df.iloc[i].Values
        elif df.iloc[i].Category == "Colour":
            colour = df.iloc[i].Values
        elif df.iloc[i].Category == "Engine Displacement (cc)":
            engine = df.iloc[i].Values
        elif df.iloc[i].Category == "Kilometers":
            kilo = df.iloc[i].Values
        elif df.iloc[i].Category == "Year":
            year = df.iloc[i].Values
            
    listing = {'Url': url,
               'Make':make,
               'Model':model,
               'Colour':colour,
               'Engine Displacement':engine,
               'Kilometers':kilo,
               'Years':year,
               'Location':location[0],
               'Price':price[0],
               'Date':posted[0]}
    
    counter = counter -1
    data.append(listing)
    print(f'11 - Done another listing {counter} remaining')

threads = [threading.Thread(target=fetch_links, args=(link,)) for link in links]
for t in threads:
    t.start()
    timer.sleep(0.01)

for t in threads:
    t.join()

print('12 - Done :D')
data = pd.DataFrame(data)
data.to_csv('Ontario_motorcylces.csv')    


