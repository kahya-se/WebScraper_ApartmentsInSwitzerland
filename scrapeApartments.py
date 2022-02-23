#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 12:30:35 2021
Refactored on Sun Feb 20 17:30:00 2022

Version: 22-02

@author: kahya-se

Note:
    - Names of classes and functions might have changed. 

Changes (compared to the version of Nov 2021):
    - (Very) Verbose descriptions
    - "Replacing headless selenium with requests", except for Comparis (...)
    - Multi-threading
    - Methods to run Nominatim locally
    - Less messy code; yet more __init__ functions - related to ducks (https://en.wikipedia.org/wiki/Duck_typing)
    - Some more auxiliary functions

Add-ons which would be great:
    - Initiating and closing a local Nominatim instance from the script
    - More pages to scrape from
    - More means to commute (e.g. bike)

"""
__version__ = "22-02"


import os, sys 
from selenium import webdriver
import urllib.parse
import requests
import re
import concurrent.futures
#import subprocess

import numpy as np
import pandas as pd
import geopandas as gpd
import json
import xmltodict

from geopy.geocoders import Nominatim
from shapely.geometry import Point

import time
from datetime import timedelta, date

##################################################################################
#
#  Module 1: The Scraper
#
##################################################################################

class Scraper():
    """
    Module for scraping websites (immoscout, homegate and comparis) for apartments for rent. 
    
    Given the parameters defined and explained below, URLs are compiled and the html-code is retrieved 
    multithreadedly*. Then, the address, number of rooms, size of the apartment, monthly rent and 
    additional information is retrieved from the html-code. The information is then summarized in 
    a pd.DataFrame. 
    
    From this point on, the addresses can be geocoded (retrieval lat/lon coordinates) and filtered 
    spatially - e.g., subject to commuting time for work, cultural centers etc.
    
    *) Note, comparis will ban your IP address, if you use it too aggressively on their site. Hence, 
    it is recommended to use the notComparis option. Having said this, the comparis scraper is in its 
    first version and not updated (yet)...
    
    Parameters:
        PAGE (str): Pages to scrape currently supports Homegate, Immoscout, Comparis (or 'all'). Works with 
            multiple entries (e.g. homegate_immoscout). //Augmentations welcome.
        ROOMS_MIN / ROOMS_MAX (float): self-explanatory
        SIZE_MIN / SIZE_MAX (int): self-explanatory
        PRICE_MIN / PRICE_MAX (int): self-explanatory
        IMAGES (bool): to filter out ads without photos
        LOCATION (str): City in Switzerland [if int, it is converted retrieving info online -- takes roughly 2 sec]
        RADIUS (float): to allow ads of apartments outside of the city (in kilometers)
        FILTER_KEYWORDS (list): Filter out ads (e.g. if the apartment is shared, temporary contarct etc.)
        MAX_WORKERS (int): Number of workers for multi-threading
        INCLUDE_COORDS (bool): to keep or drop the columns lat/lon (as =True has many NULL values, it is recommended
                                                                    to use Module 2 for geocoding)
        
    Returns:
        pd.DataFrame with columns: url (to ad), address, nRooms, size, rent, currency, description (title of the ad)
                                    published (publishing date, if available), lat (if available), lon (if available)
    """
    
    
    PAGE = 'homegate_immoscout'
    ROOMS_MIN = 1.5
    ROOMS_MAX = 1000
    SIZE_MIN = 60
    SIZE_MAX = 800
    PRICE_MIN = 0
    PRICE_MAX = 1800
    IMAGES = True
    LOCATION = "Zürich"
    RADIUS = 0
    FILTER_KEYWORDS = ["Befristet", "befristet"]
    MAX_WORKERS = 10
    INCLUDE_COORDS = False
    
    def __init__(self):
        
        if isinstance(self.LOCATION,int):
            self.LOCATION = postalcode2city(self.LOCATION)

        if isinstance(self.PAGE,list):
            self.PAGE = '_'.join(self.PAGE)
            
        if self.PAGE == 'all':
            self.PAGE = 'homegate_immoscout_comparis'
        
        self._ORIGINAL_PAGE_ENTRY = self.PAGE
            
        self.URLS = self.__getURL__()
        
        self.MAX_WORKERS = int(self.MAX_WORKERS)
        self.results = pd.DataFrame({'url':[], 'address':[], 'nRooms':[], 'size':[], 'rent':[], 'currency':[], 
                                  'description':[], 'published':[], 'lat':[], 'lon':[], 'source':[]})
        
    def scrape(self):
        if (self.PAGE == 'all') or ('homegate' in self.PAGE):
            self.results =  self.results.append(self.__scrapeHomegate__())
            print('Homegate scraped.')
            
        if (self.PAGE == 'all') or ('immoscout' in self.PAGE):
            self.results =  self.results.append(self.__scrapeImmoscout__()) 
            print('Immoscout scraped.')
            
        if (self.PAGE == 'all') or ('comparis' in self.PAGE):
            self.results =  self.results.append(self.__scrapeComparis__())
            print('Comparis scraped.')
                
        self.results = self.results.drop_duplicates(subset=["address","description","rent"], keep='last').reset_index()
        self.results = self.results.drop(labels=['index'],axis=1)   
        
        self.results['description'] = correctUmlauts(self.results.description.tolist())
        self.results['address'] = correctUmlauts(self.results.address.tolist())
        
        if self.INCLUDE_COORDS==False:
            self.results.drop(['lat','lon'], axis=1, inplace=True)
        
        if self.FILTER_KEYWORDS:
            self.results = self.filterDescription(self.results)
          
        return self.results       
  
    
    def __getURL__(self):
        
        self.URL = {}
        
        if 'immoscout' in self.PAGE:
            
            if self.IMAGES == True:
                self.IMAGES = 1
            if self.IMAGES == False:
                self.IMAGES = 0
            if self.RADIUS == "null":
                self.RADIUS = 0
            
            baseURL = "https://www.immoscout24.ch/de/wohnung/mieten/"
            closePart = "&"
            closeURL = "pty=1" # nur wohnungen
            
            locationString = 'ort-' + self.LOCATION.lower().replace('ü','ue').replace('ö','oe').replace('ä','ae') + "?"
            
            price = "pf=" + str(int(self.PRICE_MIN/100))+"h" + closePart + "pt="+ str(int(self.PRICE_MAX/100))+"h" + closePart
            rooms = "nrf=" + str(self.ROOMS_MIN) + closePart + "nrt=" + str(self.ROOMS_MAX) + closePart
            size = "slf=" + str(self.SIZE_MIN) + closePart + "slt=" + str(self.SIZE_MAX) + closePart
            images = "mai="+str(self.IMAGES) + closePart
            
            searchRadius = "r=" + str(self.RADIUS) + closePart
            
            _URL = baseURL + locationString + price + rooms + size + searchRadius + images + closeURL
            
            self.URL['immoscout'] = _URL
        
        
        if 'homegate' in self.PAGE:
            
            if self.IMAGES == True:
                self.IMAGES = 1
            if self.IMAGES == False:
                self.IMAGES = 0
            if self.RADIUS == "null":
                self.RADIUS = 0
            
            locationString = 'ort-' + self.LOCATION.lower().replace('ü','ue').replace('ö','oe').replace('ä','ae')
            baseURL = "https://www.homegate.ch/mieten/wohnung/"+locationString+"/trefferliste?"
            closePart =  "&"
            
            price = "ag=" + str(self.PRICE_MIN) + closePart + "ah=" + str(self.PRICE_MAX) + closePart
            rooms = "ac=" + str(self.ROOMS_MIN) + closePart + "ad=" + str(self.ROOMS_MAX) + closePart 
            size = "ak=" + str(self.SIZE_MIN) + closePart + "al=" + str(self.SIZE_MAX) + closePart 
            
            if self.RADIUS > 0:
                searchRadius = "be=" + str(int(self.RADIUS*1000)) 
            else:
                 searchRadius = ""
            
            _URL = baseURL + price + rooms + size + searchRadius 
            
            self.URL['homegate'] = _URL
                    
        if 'comparis' in self.PAGE:
            
            if self.RADIUS == 0:
                self.RADIUS = "null"
            
            URLstart = '{"DealType":10,"SiteId":0,"RootPropertyTypes":[1],"PropertyTypes":[],'
            closePart = '",' #"%2C"#urllib.parse.quote('",')
            closeURL = "}"
            
            rooms_min = '"RoomsFrom":"'+str(self.ROOMS_MIN) + closePart
            rooms_max = '"RoomsTo":"'+str(self.ROOMS_MAX) + closePart
            
            size_min = '"LivingSpaceFrom":"'+str(self.SIZE_MIN) + closePart
            size_max = '"LivingSpaceTo":"'+str(self.SIZE_MAX) + closePart
            
            price_min = '"PriceFrom":"'+str(self.PRICE_MIN) + closePart
            price_max = '"PriceTo":"'+str(self.PRICE_MAX) + closePart
            
            searchRadius =  '"Radius":"'+str(self.RADIUS) + closePart
            has_images ='"WithImagesOnly":"'+str(self.IMAGES).lower() + closePart
            locationString = '"LocationSearchString":"'+self.LOCATION +'"'
            
            _URL = "https://www.comparis.ch/immobilien/result/list?requestobject=" + urllib.parse.quote(URLstart + rooms_min + rooms_max + size_min + size_max + price_min + price_max + searchRadius  + has_images  + locationString  + closeURL)+ '&page=0' 

            self.URL['comparis'] = _URL
        
        return self.URL
        
    
    
    def __scrapeComparis__(self):
        
        op = None
        driver = None
        
        URL = self.URLS['comparis']
        request = requests.get(URL)
        html = request.text

        urls = []
        addresses = []
        prices = []
        rooms = []
        sizes = []
        descriptions = []
        pdates = []
        currency = []
        lat = []
        lon = []
        
        startStr =  '<script id="__NEXT_DATA__" type="application/json">'
        endStr = ',"targetingInformation":{}'
        
        paginationsComparis = re.findall("page\=([0-9]|[1-9][0-9])\"", html)
        maxPagination = np.max([int(i) for i in paginationsComparis])
        
        print("Comparis accessed, no. of pages: {}".format(maxPagination+1))
        
        for page in range(1, maxPagination+2):
            startInfos = html.find(startStr)+len(startStr)
            endInfos = html.find(endStr)
            infoList = html[startInfos+1:endInfos-1].split("},{")
          
            
            for I, info in enumerate(infoList):
                if '"SiteId"' in info[:10]:
                    continue
                
                if '"AdId"' not in info[:10]:
                    xID = info.find('"AdId"')
                    info = info[xID:]
                    
                if '},"page":' in info:
                    info = info.split(',"ShowDefaultPersonalizationSegment"')[0]
                                       
                infoAsDict = json.loads('{'+info+'}')
                url = 'https://www.comparis.ch/immobilien/marktplatz/details/show/'+str(infoAsDict['AdId'])
                urls.append(url)
                
                address = ", ".join(infoAsDict['Address'])
                addresses.append(address)
                
                price = infoAsDict['PriceValue']
                prices.append(price)
                
                try:
                    nRooms = float(infoAsDict['EssentialInformation'][0].split(" ")[0])
                except IndexError:
                    print()
                
                rooms.append(nRooms)
                
                size = float(infoAsDict['EssentialInformation'][1].split(" ")[0])
                sizes.append(size)
                
                #if len(infoAsDict['EssentialInformation']) == 3:
                #    floor =  infoAsDict['EssentialInformation'][2]
                #else:
                #    floor = np.nan
                #floors.append(floor)
                
                pdates.append(infoAsDict['Date'])
                
                description = infoAsDict['Title']
                descriptions.append(description)
                currency.append(infoAsDict['Currency'])
                
                lat.append(np.nan)
                lon.append(np.nan)
                
                
            newURL = URL[:-len("&page=0")]+ "&page={}".format(page)
            time.sleep(np.random.random()) # maybe this helps
            driver = webdriver.Chrome(options=op) 
            driver.get(newURL) 
            html = driver.page_source
            driver.close()
            #print("Scraped comparis: page {}".format(page))

            
        trawledComparis = pd.DataFrame({'url':urls, 'address':addresses, 'nRooms':rooms, 'size':sizes, 'rent':prices, 'currency':currency, 
                                          'description':descriptions, 
                                          'published':pdates, 'lat':lat, 'lon':lon})
        trawledComparis['source'] = 'comparis'
        return trawledComparis.drop_duplicates(subset=["url"])
    
    
    def __scrapeImmoscout__(self):

        URL = self.URLS['immoscout']
        response = requests.get(URL)
        html = response.content.decode('utf-8')
   
        maxPagesTagStart = "<section class=\"Pagination__PaginationSection" 
        maxPagesTagEnd = "</section>"
        paginationChunk = html[html.find(maxPagesTagStart):html.find(maxPagesTagEnd,html.find(maxPagesTagStart))]
        paginationsImmo = re.findall(">([0-9]|[1-9][0-9])</button", paginationChunk)
        
        if len(paginationChunk) == 0:
            maxPagination = 1
        else:
            maxPagination = np.max([int(x) for x in paginationsImmo])
                
        print("Immoscout accessed, no. of pages: {}".format(maxPagination))
        
        urls = []
        addresses = []
        prices = []
        rooms = []
        sizes = []
        descriptions = []
        pdates = []
        currency = []
        lat = []
        lon = []
        
        startStr = '\{"id":\d{7},"accountId"'
        endStr = '\<\/script>'
        
        pages = list(range(maxPagination+1))
        
        def __scrapeImmoscout_pages(page):
            newURL = URL+"&pn="+str(page+1) #&pn=X
            response = requests.get(newURL)
            html = response.content.decode('utf-8')
            startInfos = re.search(startStr,html).span(0)[0]
            endInfos = re.search(endStr,html).span(0)[0]
            infoChunk = html[startInfos:endInfos]
            
            startPoints = [m.start(0)  for m in re.finditer(startStr,infoChunk)][:24]
            
            for i,point in enumerate(startPoints):
                
                info = infoChunk[point:infoChunk.find('"userRelevantScore"', point+1)-1]+"}"
                if len(info) > 15000:
                    continue
                
                infoAsDict = json.loads(info)
                if len(infoAsDict) <9:
                    continue
                
                urls.append('https://www.immoscout24.ch'+infoAsDict['propertyUrl'])
                
                if 'street' in infoAsDict:
                    addresses.append( ", ".join([infoAsDict['street'], " ".join([infoAsDict['zip'],  infoAsDict['cityName']])]))
                else:
                    addresses.append(" ".join([infoAsDict['zip'],  infoAsDict['cityName']]))
                
                if infoAsDict['priceFormatted'] == 'Preis auf Anfrage':
                    prices.append(np.nan)
                elif 'grossPrice' in infoAsDict:
                    prices.append( infoAsDict['grossPrice'] )
                else:
                    prices.append( infoAsDict['price'])
                    
                rooms.append( infoAsDict['numberOfRooms'] )
                sizes.append( infoAsDict['surfaceLiving'] )
                descriptions.append( infoAsDict['title'] )
                
                currency.append(infoAsDict['priceFormatted'][:3])
                
                pdate = infoAsDict['lastPublished']
                pdates.append(pdate)
                
                try:
                    lat.append( infoAsDict['latitude'])
                    lon.append( infoAsDict['longitude'])
                except KeyError:
                    lat.append(np.nan)
                    lon.append(np.nan)
                
            newURL = URL+"&pn="+str(page+1) #&pn=X
            response = requests.get(newURL)
            html = response.content.decode('utf-8')
            #print("Scraped immoscout24: page {}".format(page))
        
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for page in pages:
                executor.submit(__scrapeImmoscout_pages, page)
            
        
        trawledImmoscout = pd.DataFrame({'url':urls, 'address':addresses, 'nRooms':rooms, 'size':sizes, 'rent':prices, 'currency':currency, 
                                          'description':descriptions, 
                                          'published':pdates, 'lat':lat, 'lon':lon})
        trawledImmoscout['source'] = 'immoscout'

        return trawledImmoscout.drop_duplicates(subset=["url"])
    
    
    def __scrapeHomegate__(self):
        
        URL = self.URLS['homegate']
        response = requests.get(URL)
        html = response.content.decode('utf-8')
        
        maxPageSpan = re.search('"pageCount":\d{1,5}', html).span()
        maxPageStr = html[maxPageSpan[0]:maxPageSpan[1]].split(':')[1]
        maxPage = int(maxPageStr)
                          
        print("Homegate accessed, no. of pages: {}".format(maxPage))
        def __get_HTMLchunks(page):
            #for pageNumber in range(maxPage):
            newURL = URL+"&ep="+str(int(page)+1) 
            response = requests.get(newURL)
            html = response.content.decode('utf-8')
            chunkStart = re.search('<script>window.__INITIAL_STATE__=', html).span()[1]
            chunkEnd = re.search(',"page":\d{1,5},"pageCount":\d{1,5},"', html).span()[0]
            
            htmlChunk = html[chunkStart:chunkEnd]
            return htmlChunk
        
        pages = list(range(maxPage+1))
        htmlChunks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for page in pages:
                chunk = executor.submit(__get_HTMLchunks, page)
                htmlChunks.append(chunk)
                
        htmlChunks = " ".join([h.result() for h in htmlChunks])
        
        urls = []
        addresses = []
        prices = []
        rooms = []
        sizes = []
        descriptions = []
        pdates = []
        currency = []
        lat = []
        lon = []
    
        matches = [m.start(0) for m in re.finditer('{"listingType":{"type":"', htmlChunks)]
        
        def __harvest_HomegateChunks(miniStart):
        
            miniEnd = htmlChunks.find('"currency":',miniStart)+len('"currency":')+5
            miniChunk = htmlChunks[miniStart:miniEnd]+"}}}"
            chunkDict = json.loads(miniChunk)
            
            url = 'https://www.homegate.ch/mieten/'+chunkDict['listing']['id']
            urls.append(url)
            
            try: 
                if chunkDict['listing']['prices']['rent']['interval'] == 'WEEK':
                    multiplier = 4
                else:
                    multiplier = 1
                
            except KeyError:
                #print('KeyError: check rent interval, assumed interval: monthly')
                multiplier = 1
            
            if 'gross' not in chunkDict['listing']['prices']['rent']:
                chunkDict['listing']['prices']['rent']['gross'] = np.nan
        
            
            rent = chunkDict['listing']['prices']['rent']['gross'] * multiplier
            prices.append(rent)
            currency.append(chunkDict['listing']['prices']['currency'])
        
            if len(chunkDict['listing']['address']) == 3:
                plzAdr = " ".join([chunkDict['listing']['address']['postalCode'], chunkDict['listing']['address']['locality']])
                address = ", ".join([chunkDict['listing']['address']['street'], plzAdr])
                
            elif len(chunkDict['listing']['address']) == 2:
                address = " ".join([chunkDict['listing']['address']['postalCode'], chunkDict['listing']['address']['locality']])
                
            addresses.append(address.replace(',,',','))        
            
            rooms.append(chunkDict['listing']['characteristics']['numberOfRooms'])
            sizes.append(chunkDict['listing']['characteristics']['livingSpace'])
            descriptions.append(chunkDict['listing']['localization']['de']['text']['title'])
        
            pdates.append(np.nan)
            lat.append(np.nan)
            lon.append(np.nan)
        
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for miniStart in matches:
                executor.submit(__harvest_HomegateChunks, miniStart)
                

        trawledHomegate = pd.DataFrame({'url':urls, 'address':addresses, 'nRooms':rooms, 'size':sizes, 'rent':prices, 'currency':currency, 
                                 'description':descriptions, 
                                 'published':pdates, 'lat':lat, 'lon':lon})
        trawledHomegate['source'] = 'homegate'
        return trawledHomegate.drop_duplicates(subset=["url"])
            

        
    def filterDescription(self, dataframe):
        """
        Filters results / omits if keywords occur in the description column.
        Keywords must be specified as in FILTER_KEYWORDS.
        
        
        Parameters
        ----------
        dataframe : pd.DataFrame
        
        
        Returns
        -------
        dataframe : pd.DataFrame
        """
    
        if type(self.FILTER_KEYWORDS) != list:
            keywords = [self.FILTER_KEYWORDS]
        else:
            keywords = self.FILTER_KEYWORDS
        
        for key in keywords:
            dataframe = dataframe[~dataframe['description'].str.contains(key)]
        
        dataframe = dataframe.reset_index()
        dataframe = dataframe.drop(labels=['index'],axis=1)
        return dataframe


##################################################################################
#
# Module 2: Geocoding
#
##################################################################################
    
class Geocoding():
    """ 
    Module for geocoding. In a nutshell, it takes a list of addresses and finds the 
    latitude and longitude for the set of the list. Eventually returns a pd.DataFrame.
    
    In the context of the webscraper, the column 'address' can be joined with the addresses 
    retrieved in the scraping process. A couple of methods are used for data cleaning, namely to
    replaces certain strings (e.g. typos such as strase instead of strasse), to reduce the number 
    of not found locations using Nominatim. 

    Nominatim* can be retrieved either locally or online. Note, using the online/web-based approach 
    is only encouraged for small number of single-threadedly queried addresses (less than 100). Else,
    the IP address will be blocked for 1h.    
    
    
    Parameters:
        NOMINATIM (str): local or localhost for running it locally, else it web-based
        ADDRESSES (list pd/gpd.(Geo)DataFrame with 'address' column): list of strings with addresses to be geocoded
        CLEAN_ADDRESS_ENTRIES (dict): dict with key and value pair. Searches for key and replaces with value.
        MAX_WORKERS (int): Max. workers for multi-threading
    
    Returns:
        pd.DataFrame with the columns address (input address), address_located (cleaned address), lat, lon 
        
    * Please make sure to run Nominatim ("cd /folder/with/import && nominatim serve")    
    """
    
    
    NOMINATIM = 'localhost'
    DATA = ['']
    CLEAN_ADDRESS_ENTRIES = {}
    MAX_WORKERS = 10
    
    def __init__(self):
        if self.MAX_WORKERS < 1:
            self.MAX_WORKERS = 1
        if isinstance(self.DATA, str):
            self.DATA = [self.DATA]
        if isinstance(self.DATA, (pd.DataFrame or gpd.GeoDataFrame)):
            if 'address' in self.DATA.columns:
                self.DATA = self.DATA['address'].tolist()
        self.DATA = list(set(self.DATA))
        
        assert isinstance(self.CLEAN_ADDRESS_ENTRIES, dict), "CLEAN_ADDRESS_ENTRIES must be a dictionary"
        

    def geocode(self):
        cleanAddr =  self.__cleanAddresses()
        if self.NOMINATIM in ['localhost', 'local']:
            return  self.__geocode_local(cleanAddr)
        else:
            return self.__geocode_internet(cleanAddr)
 
        
    def __cleanAddresses(self):
        """ cleans addresses (issues with umlauts, abbvreviations, typos etc.) """
        originalAddresses = []        
        cleanAddresses = []
        
        cleaningLookUp =  {'Ã¶':'ö',
                            'Ã¼':'ü',
                            'Ã¤':'ä',
                            ' Strasse': 'strasse',
                            'strase':'strasse',
                            'stasse ': 'strasse ',
                            'Ni una menos Platz': 'Helvetiaplatz', # Noch fälschlich mit altem Namen in OSM/Nominatim (lang lebe 14.06)
                            'Mordor': 'Universitätsstrasse 16, 8006 Zürich', # Popkulturelles Easter Egg. Erinnerung an Studienzeit.
                            'Hogwarts': 'Rämistrasse 101, 8092 Zürich',      # Popkulturelles Easter Egg. Erinnerung an Studienzeit.
                            'Azkaban': 'Schafmattstrasse 34, 8049 Zürich',   # Popkulturelles Easter Egg. Erinnerung an Studienzeit.
                            'Universitätsstrasse':'Universitätstrasse', 
                            'Heliostrasse':'Heliosstrasse',
                            'Niedendorfstrasse':'Niederdorfstrasse',
                            'Schafhauserstrasse':'Schaffhauserstrasse',
                            'Zeughaustrasse':'Zeughausstrasse',
                            ': ZH': '',
                            ' ZH': '',
                            ' Nr.': '',
                            ' nr.': ''}
        
        if len(self.CLEAN_ADDRESS_ENTRIES) > 0:
            cleaningLookUp.update(self.CLEAN_ADDRESS_ENTRIES)
        
        for addressRaw in self.DATA:
            addressRaw = correctUmlauts(addressRaw)
            
            adr = addressRaw.replace('pl.','platz').replace('str.','strasse').replace('str ','strasse ')
            adr = adr.replace(', Schweiz','').replace('Schweiz','') #regex it...
            adr = re.sub("(N|n)(a|ä)he ","",adr)
            
            for key in cleaningLookUp.keys():
                adr = adr.replace(key, cleaningLookUp[key])
                
            # get nominatim ready address:    
            
            params = createNominatimParams(adr)  
            cleanAddress = params['street']+", "+ params['postalcode']+" "+params['city']
                
            cleanAddresses.append(cleanAddress)
            originalAddresses.append(addressRaw)
            
            
            
        cleanAddr = pd.DataFrame({'address':originalAddresses, 'cleanAddress':cleanAddresses})
        return cleanAddr 
    
    
    def __geocode_local(self,cleanAddr):
        """ Uses Nominatim from a localhost (multi-threaded processing if >1 workers) """
        
        def __locate(row):
           
            address_scraped = []
            address_located = []
            lats = []
            lons = []
            address = row['address']
            clean = row['cleanAddress']
            
            params = createNominatimParams(clean)
   
            r = requests.get('http://localhost:8088/search.php', params)
            if len(r.text) == 2:
                address_scraped.append(address) 
                address_located.append(clean) 
                lats.append(np.nan) 
                lons.append(np.nan) 
            
            else:
                result = json.loads(r.text)
                
                ranks = [res['place_rank'] for res in result]
                selectResult = np.argmax(ranks)
                
                address_scraped.append(address)
                address_located.append(params['street']+", "+params['postalcode']+" "+params['city'])
                lats.append(result[selectResult]['lat']) 
                lons.append(result[selectResult]['lon'])
            
            return pd.DataFrame({'address':address_scraped,'address_located':address_located,'lat':lats,'lon':lons})
       
        dflist = []
        
        #subprocess.Popen("nominatim serve", cwd="/home/user/IsThisEven/useful/Nominatim/Switzerland")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for idx, row in cleanAddr.iterrows():
                dfs = executor.submit(__locate, row)
                dflist.append(dfs)
        
        df = pd.concat([df.result() for df in dflist]) 
        return df.drop_duplicates(subset=['address'])


    def __geocode_internet(self,cleanAddr):
        """ Uses Nominatim from a web server. Multi-threading not encouraged. """
        
        nom = Nominatim(user_agent="FindApartment_CH", scheme='http', domain='nominatim.openstreetmap.org')
        lat = []
        lon = []       
                   
        for idx, row in cleanAddr.iterrows():  
            location = nom.geocode(row.cleanAddress)    
            if type(location) == type(None):
                lat.append(None)
                lon.append(None)
            else:
                location = location.raw
                lat.append(float(location['lat']))
                lon.append(float(location['lon']))

        df = pd.DataFrame({'address':cleanAddr.address, 'address_located':cleanAddr.cleanAddress, 'lat':lat, 'lon':lon})
        return df.drop_duplicates(subset=['address'])
    
    
##################################################################################
#
# Module 3: CommuteTimes
#
##################################################################################

class CommutingTimes(Geocoding):
    """
    Class to handle the retrieval of commuting times. 
    Currently, it uses the SBB-API to retrieve the average time spent for commuting, by
    accessing the schedule on the 'next monday morning at 8.30 AM'.
    
    Given multiple DESTINATIONs, it is possible to get average commuting time to multiple places,
    useful to estimate the reachability to other places. 
    
    If the air distance between the given and destination address is smaller than a given distance, 
    the commuting time is calculated for. Without this, it has been suggest rather complicated routes
    for adjacent locations.
    
    This module returns a pd.DataFrame which can be merged with another pd.DataFrame on the 'address' column
    
    Ideas for augmentation:
        - adding other means, namely bicycle
        
    Parameters:
        DATAFRAME (pd.DataFrame): containing columns address, lat and lon
        DESTINATION (tuple, list / str): pairs of coordinates (lat,lon - decimal degrees ) or str containing the address (which then is geocoded)
        MAX_WORKERS (int): max. number of threads for multi-threading
        WALKING_DISTANCE (int/float): for distance reasonable to walk
        TEST_FIRST (bool): Tests the access to the SBB-API
        
    Returns:
        pd.DataFrame with columns address, avg. commuting time in minutes. If DESTINATION contains more 
            than one entry, the results are successively enumerated (mins_sbb_1, mins_sbb_2,...)
    """
    
    DATA = pd.DataFrame(data=None)
    DESTINATION = [(47.3760832, 8.52690016762467)] ### as LatLon or str
    MAX_WORKERS = 10
    MEANS = 'public_transportation'
    WALKING_DISTANCE = 650
    TEST_FIRST = True
    
    def __init__(self):
        if self.MAX_WORKERS < 1:
            self.MAX_WORKERS = 1
        
        dataCond1 = isinstance(self.DATA, list)
        dataCond2 = isinstance(self.DATA, pd.Series)
        dataCond3 = isinstance(self.DATA, pd.DataFrame) 
        if dataCond3 == True:
            dataCond3a = 'address' in self.DATA.columns
            dataCond3b = 'lat' in self.DATA.columns
            dataCond3c = 'lon' in self.DATA.columns
            if all([dataCond3a,dataCond3b,dataCond3c]) == True:
                dataCond3 = True
            else:
                print("Enter DATA accordingly.")
        
        if any([dataCond1,dataCond2]):
            #getGeocoding = self.Geocoding()
            #getGeocoding.NOMINATIM = 'local'
            self.DATA = list(set(self.DATA))
            self.DATA = self.geocode()
            
        
        assert 'lat' in self.DATA.columns
        assert 'lon' in self.DATA.columns
        assert 'address' in self.DATA.columns
        assert 'public' in self.MEANS.lower()
        
        if not isinstance(self.DESTINATION,list):
            return "Enter DESTINATION accordingly"
        

        DESTINATION = []
        
        streets = []
        lats = []
        lons = []
        
        for dest in self.DESTINATION:
            if len(dest) == 2:
                DESTINATION.append((dest))
                address = reverseGeocode(dest)
                streets.append(address)
                lats.append(dest[0])
                lons.append(dest[1])
            elif isinstance(dest,str):
                destLat,destLon = geocode(dest)
                DESTINATION.append((destLat,destLon))
                streets.append(dest)
                lats.append(destLat)    
                lons.append(destLon)
            else:
                print("DESTINATION ("+str(dest)+") not recoginzed, please enter accordingly.")
            
            self.DESTINATION = DESTINATION
            self.DESTINATION_LENGTH = len(DESTINATION)      
            self.DESTINATION_DF = pd.DataFrame({'title':['Destination {}'.format(i) for i in range(len(streets))], 'address':streets, 'lat':lats, 'lon':lons})
        
    def getCommutingTimes(self):
        """ Gets commuting time. Currently only for public transportation. """
        
        if self.TEST_FIRST == True:
            test = self.test()
            if test == False:
                return

        for self.destNo in range(self.DESTINATION_LENGTH):
            if 'public' in self.MEANS.lower():
                commute = self.__commute_byTrain()
                self.DATA = self.DATA.merge(commute, on='address')
        return self.DATA
        
    
    def test(self):
        """ Tests, if the SBB-API is useable. """
        testURL = 'http://transport.opendata.ch/v1/connections?from=47.3799622+8.5281334&to=47.378294+8.5275268&datetime='
        r = requests.get(testURL)
        Res = json.loads(r.text)
        
        try:
            resultat = Res['connections']
            return True
        except KeyError:
            print(Res['errors'][0]['message'])
            return False
        return False
    
    
    def __commute_byTrain(self):
        """ get commuting time using SBB-API """
        def __commuteTimes(row):
            sbbResults = []#

            address = row['address']
            lat = float(row['lat'])
            lon = float(row['lon'])
            
            destLat = self.DESTINATION[self.destNo][0]
            destLon = self.DESTINATION[self.destNo][1]
            
            today = date.today()
            nextMonday = today + timedelta(days=-today.weekday(), weeks=1)
            startCommute = nextMonday.strftime("%Y-%m-%d")+"T08%3A00"
            
            
            airDistance = haversine((lat,lon),(destLat,destLon))
            
            if airDistance <= self.WALKING_DISTANCE:
                timeWalking = airDistance / 4000 * 60
                Res = json.loads('{"connections":[{"duration":'+str(np.ceil(timeWalking))+'}]}')
                sbbResults.append([address, Res])
    
            else:
                getURL = 'http://transport.opendata.ch/v1/connections?from='+str(lat)+'+'+str(lon)+'&to='+str(destLat)+'+'+str(destLon)+'&datetime='+startCommute 
                r = requests.get(getURL)
                Res = json.loads(r.text)  
                sbbResults.append([address, Res])
            return sbbResults
        
        sbbResults = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for i,row in self.DATA.iterrows():
                sbb = executor.submit(__commuteTimes, row)
                sbbResults.append(sbb)
        
        sbbResults = [sbbRes.result()[0] for sbbRes in sbbResults]
        
        avgMinutes = []
        addresses = []
        
        for i,result in enumerate(sbbResults):
            adr = result[0]
            res = result[1]
            mins = []
            for connection in res['connections']:
                if len(connection) == 1:
                    mins.append(np.int32(connection['duration']))
                    
                else:
                    h = connection['duration'][3:]
                    delta = timedelta(hours=float(h.split(':')[0]), minutes=float(h.split(':')[1]), seconds=float(h.split(':')[2]))
                    minutes = delta.total_seconds()/60
                    mins.append(minutes)                    
            avgMinute = np.int32(np.nanmean(mins))
            avgMinutes.append(avgMinute)
            addresses.append(adr)
    
        commuteTimes = pd.DataFrame({'address':addresses, 'mins_sbb_{}'.format(self.destNo+1):avgMinutes})
        return commuteTimes
        
    

##################################################################################
#
# Functions A: Prepare for display
#
##################################################################################


def df2GeoJSON(dataframe, outpath, varname='dataset', avgMinutesCol='mins_sbb_1'):
    """
    Writes the pd.DataFrame to the hard disk as a GeoJSON file, later to be used
    to be displayed (e.g. GIS or HTML/Leaflet).

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe containing address, geolocation (lat,lon), commute time.
    outpath : str
        Path to save DataFrame as GeoJSON.
    avgMinutesCol : str, optional
        Column to be displayed as commute time (on the website). The default is 'mins_sbb_1'.
    varname : str, optional
        Variable name (if used on a website). The default is 'dataset'.

    Returns
    -------
    None.

    """
    lons = dataframe.lon.tolist()
    lon = [float(lon) for lon in lons]
    lats = dataframe.lat.tolist()
    lat = [float(lat) for lat in lats]
    
    geometry = [Point(xy) for xy in zip(lon,lat)] # Achtung, lon lat
    gdf = gpd.GeoDataFrame(dataframe,geometry=geometry,crs=4326)
    if avgMinutesCol:
        gdf['avgMinutes'] = dataframe[avgMinutesCol]
    
    geojson = gdf.to_json()
    if varname:
        geojson = 'var '+varname+' = '+geojson
    gdf_output = correctUmlauts(geojson)
    
    if not outpath.endswith('.geojson'):
        outpath += '.geojson'
    
    with open(outpath, 'w') as file:
        file.write(gdf_output)
        

def createDestinationGeoJSON(outputPath,addresses,titles= ['Destination'], comments=['']):
    """
    Create GeoJSON file with the destination.
    """
    
    lats = []
    lons = []
    geometries = []
    titlesOut = []
    
    for i,address in addresses:
        lat,lon = geocode(address)
        geometry = Point(lon,lat) # Achtung, it takes lon lat
        lats.append(lat)
        lons.append(lon)
        geometries.append(geometry)
        titlesOut.append(titles[i]+"_"+str(i))
    
    if len(comments) != len(addresses):
        comments += comments * (len(addresses) - len(comments))
    
    destgdf = gpd.GeoDataFrame({'address':addresses, 'title':titlesOut, 'comment':comments, 'geometry': geometries})
    destgdf.to_file(outputPath, driver="GeoJSON")
      
    geojsoned = destgdf.to_json()
    gdf = 'var destAddress='+geojsoned
    with open(outputPath, 'w') as file:
        file.write(gdf)
        
##################################################################################
#
# Functions B: Auxiliary functions
#
##################################################################################
    
    
def createNominatimParams(address):
    """ 
    Splits a string containing the address to a dict. Used when running Nominatim locally. 
    
    Parameters
    ----------
        address (str): String containing the address: street street_number, postal_code city
    
    Returns
    -------
        params (dict): 
    """
    words = re.findall('[\u00C0-\u00FFA-Za-z\u00F0-\u02AF]+\-*\s*[\u00C0-\u00FFA-Za-z\u00F0-\u02AF]+', address)
    
    street = words[0]
    city = words[-1]
    numbers = re.findall(r"\d[0-9]*", address)
    houseNo = [n for n in numbers if len(n) < 4]
    
    if houseNo:
        houseNo = houseNo[0]
    else:
        houseNo = ''
        
    if street == city:
        street = ''
        
    postal_code = numbers[-1].replace('8000','8001')
    
    params = {'street':street+" "+houseNo,
              'city':city,
              'postalcode':postal_code,
              'format': 'jsonv2'}
    return params



def correctUmlauts(entries):
    """
    Replaces mis-encoded characters to umlauts:
      -  'Ã¶' is mapped to 'ö'
      -  'Ã¤' is mapped to 'ä'
      -  'Ã¼' is mapped to 'ü'
    
    Limitations: Supports only lowercased characters.
    
    Parameters
    ----------
    entries : str/list

    Returns
    -------
    str/list
        corrected entries.

    """
    umlaute = {'Ã¶':'ö', 'Ã¼':'ü','Ã¤':'ä'}
    
    if isinstance(entries,str):
        entriesList = [entries]
        
    else:
        entriesList = entries
    
    correctedUmlaute = []
    for entry in entriesList: 
        for umlaut in umlaute.keys():
            entry = entry.replace(umlaut,umlaute[umlaut])
        correctedUmlaute.append(entry)

    if len(correctedUmlaute) == 1:
        return correctedUmlaute[0]
    else:
        return correctedUmlaute
    

def haversine(latlon1,latlon2):
    """
    Haversine formula to calculate great-circle distance between two points on a sphere.
    https://en.wikipedia.org/wiki/Haversine_formula
    
    Raduius of the earth in meters (at latitude of 47.3 and 450 masl) retrieved from:
    https://rechneronline.de/earth-radius/
    
    Note, this is a quick approximation sufficient for the task at hand.
    
    Input: tuples with lat/lon in decimal degrees
    Returns: Distance in meters
    
    """
    lat1,lon1 = np.deg2rad(latlon1) 
    lat2,lon2 = np.deg2rad(latlon2)
    dlat = lat2 - lat1 
    dlon = lon2 - lon1 
    
    insideArcsin = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    d = 2 * np.arcsin(np.sqrt(insideArcsin)) 
    
    radiusToGroundlevel =  6367082
    distance = radiusToGroundlevel * d
    return distance


def geocode(addressString):
    """ Geocoding a single address using Nominatim online """
    nom = Nominatim(user_agent="FindApartment", scheme='http', domain='nominatim.openstreetmap.org')
    location = nom.geocode(addressString)
    try:
        location = location.raw
        lat = float(location['lat'])
        lon = float(location['lon'])
        return lat, lon
    
    except AttributeError:
        print("Address not found. Please correct it.")
        return


def reverseGeocode(latlon):
    """ Reverse geocoding. Returns an address for a given pair of coordinates (lat,lon) """
    lat = str(latlon[0])
    lon = str(latlon[1])
    request = requests.get("https://nominatim.openstreetmap.org/reverse?lat="+lat+"&lon="+lon)
    xml = request.text
    
    output = xmltodict.parse(xml)
    try:
        street = output['reversegeocode']['addressparts']['road'] 
        no = output['reversegeocode']['addressparts']['house_number'] 
        postcode = output['reversegeocode']['addressparts']['postcode'] 
        city = output['reversegeocode']['addressparts']['city'] 
        
        address = street+" "+no+", "+ postcode+" "+city    
        return address
    except:
        print("Address not found.")
        return
     

    
def postalcode2city(postalcode):
    """ Maps postal codes to cities (online) """
    tablesFromWeb = pd.read_html("https://postleitzahlenschweiz.ch/tabelle/")[0]
    tablesFromWeb.columns = ['postal', 'city', 'Kanton', 'Canton', 'Cantone',
           'Abkürzung / Abréviation / Abbreviazione', 'Land', 'Pays', 'Paese']
    
    lookupPLZ = tablesFromWeb.postal.tolist()
    lookupCities = tablesFromWeb.city.tolist()
    lookupdict = dict(zip(lookupPLZ,lookupCities))
    
    if isinstance(postalcode,int):
        return lookupdict[postalcode]
    elif isinstance(postalcode, list):
        cities = []
        for plz in postalcode:
            city = lookupdict[plz]
            cities.append(city)
        return cities
