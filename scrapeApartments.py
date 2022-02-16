#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 12:30:35 2021

@author: kahya-se

"""

from selenium import webdriver
import requests
import urllib.parse

import os, sys 
import re

import numpy as np
import pandas as pd
import geopandas as gpd
import json

from geopy.geocoders import Nominatim
from shapely.geometry import Point

import time
from datetime import datetime, timedelta, date

class Parameters():
    PAGE = ''
    ROOMS_MIN = 1.5
    ROOMS_MAX = 1000
    SIZE_MIN = 60
    SIZE_MAX = 200
    PRICE_MIN = 0
    PRICE_MAX = 1800
    IMAGES = True
    RADIUS = "null"
    LOCATION = "Zürich"
    

    def __getURL__(self):
        
        if 'comparis' in self.PAGE.lower():
            
            if self.RADIUS == 0:
                self.RADIUS = "null"
            
            baseURL = '{"DealType":10,"SiteId":0,"RootPropertyTypes":[1],"PropertyTypes":[],'
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
            
            URL = "https://www.comparis.ch/immobilien/result/list?requestobject=" + urllib.parse.quote(baseURL + rooms_min + rooms_max + size_min + size_max + price_min + price_max + searchRadius  + has_images  + locationString  + closeURL)+ '&page=0' 

            return URL
    
        elif 'immoscout' in self.PAGE.lower():
            
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
            
            URL = baseURL + locationString + price + rooms + size + searchRadius + images + closeURL
            
            return URL
        
        elif 'homegate' in self.PAGE.lower():
            
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
            
            URL = baseURL + price + rooms + size + searchRadius 
            
            return URL
            
        else:
            return 'ERROR: specify page'
        
    def __cleanAddresses__(self,addresses):
        
        cleanedAddresses = [i.replace('str ','strasse ') for i in addresses]
        cleanedAddresses = [i.replace('str. ','strasse ') for i in cleanedAddresses]
        cleanedAddresses = [i.replace(', ZH', '') for i in cleanedAddresses]
        cleanedAddresses = [i.replace(' ZH', '') for i in cleanedAddresses]
        return cleanedAddresses
    
    
    def __scrapeComparis__(self):
        
        URL = None
        op = None
        driver = None
        
        URL = self.__getURL__()
        op = webdriver.ChromeOptions()
        op.add_argument("--headless")
        op.add_argument("--no-sandbox") 
        op.add_argument("--disable-setuid-sandbox") 
        op.add_argument("--remote-debugging-port=9222")  # this
        op.add_argument("--disable-dev-shm-using") 
        op.add_argument("--disable-extensions") 
        op.add_argument("--disable-gpu") 
        op.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) PearWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.3")
        
        driver = webdriver.Chrome(options=op) 
        driver.get(URL)
        html = driver.page_source
        driver.close()
        
        urls = []
        addresses = []
        prices = []
        rooms = []
        sizes = []
        descriptions = []
        #imgs = []
        floors = []
        pdates = []
        currency = []
        lon = []
        lat = []
        
        
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
                
                #img = infoAsDict['ImageUrl']
                #imgs.append(img)
                try:
                    nRooms = float(infoAsDict['EssentialInformation'][0].split(" ")[0])
                except IndexError:
                    print()
                
                rooms.append(nRooms)
                
                size = float(infoAsDict['EssentialInformation'][1].split(" ")[0])
                sizes.append(size)
                
                if len(infoAsDict['EssentialInformation']) == 3:
                    floor =  infoAsDict['EssentialInformation'][2]
                else:
                    floor = np.nan
                floors.append(floor)
                
                pdates.append(infoAsDict['Date'])
                
                description = infoAsDict['Title']
                descriptions.append(description)
                currency.append(infoAsDict['Currency'])
                
                lon.append(np.nan)
                lat.append(np.nan)
                
                
            newURL = URL[:-len("&page=0")]+ "&page={}".format(page)
            time.sleep(np.random.random()) # maybe this helps
            driver = webdriver.Chrome(options=op) 
            driver.get(newURL) 
            html = driver.page_source
            driver.close()
            #print("Scraped comparis: page {}".format(page))

            
        addresses = self.__cleanAddresses__(addresses)
        trawledComparis = pd.DataFrame({'url':urls, 'address':addresses, 'nRooms':rooms, 'size':sizes, 'rent':prices, 'currency':currency, 
                                          'description':descriptions, 'floor':floors,
                                          'published':pdates, 'lon':lon, 'lat':lat})
        trawledComparis['source'] = [i[i.find("www.")+len("www."):i.find(".ch")] for i in trawledComparis.url.tolist()]
        return trawledComparis.drop_duplicates(subset=["url"])
    
    
    def __scrapeImmoscout__(self):
        URL = None
        op = None
        driver = None
        
        URL = self.__getURL__()
        urlprefix = "https://www."
        
        op = webdriver.ChromeOptions()
        op.add_argument("--headless")
        op.add_argument("--no-sandbox") 
        op.add_argument("--disable-setuid-sandbox") 
        op.add_argument("--remote-debugging-port=9222")  # this
        op.add_argument("--disable-dev-shm-using") 
        op.add_argument("--disable-extensions") 
        op.add_argument("--disable-gpu") 
        op.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.3")
        driver = webdriver.Chrome(options=op) 
        driver.get(URL)
        html = driver.page_source
        driver.close()
        
        urls = []
        addresses = []
        prices = []
        rooms = []
        sizes = []
        descriptions = []
        #imgs = []
        floors = []
        pdates = []
        currency = []
        lon = []
        lat = []
        
        startStr =  '"listData":['
        endStr = ',"searchTopListingResultCount":'
        
        newURL = URL+ "&page=0"
        urlprefix = "https://www."
        
        maxPagesTagStart = "<section class=\"Pagination__PaginationSection" 
        maxPagesTagEnd = "</section>"
        paginationChunk = html[html.find(maxPagesTagStart):html.find(maxPagesTagEnd,html.find(maxPagesTagStart))]
        paginationsImmo = re.findall("pn=([0-9]|[1-9][0-9])\&", paginationChunk)
        if len(paginationChunk) == 0:
            maxPagination = 1
        else:
            maxPagination = np.max([int(x) for x in paginationsImmo])
        
        startChunk = '{"id":\d{7}'
        endChunk = ',"userRelevantScore":'
        
        print("Immoscout accessed, no. of pages: {}".format(maxPagination+1))
        for page in range(maxPagination+1):
        
            startInfos = html.find(startStr)+len(startStr)
            htmlPart = html[startInfos+1:]
            endInfos = htmlPart.find(endStr)
            infoChunk = htmlPart[:endInfos]
            
            y = [0]+[m.start(0)  for m in re.finditer(startChunk,infoChunk)]
            
            for i,element in enumerate(y):
                
                info = '{'+infoChunk[y[i]:infoChunk.find(endChunk, y[i])].replace(',{"id":','"id":')+'}'
                info = info.replace('{{"id"','{"id"')
                
                if info.startswith('{"id":'):
                    
                    infoAsDict = json.loads(info)
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
                    
                    
                    floors.append(np.nan)
                    currency.append(infoAsDict['priceFormatted'][:3])
                    
                    pdate = infoAsDict['lastPublished']
                    pdates.append(pdate)
                    
                    try:
                        lon.append( infoAsDict['longitude'])
                        lat.append( infoAsDict['latitude'])
                    except KeyError:
                        lon.append(np.nan)
                        lat.append(np.nan)
                    
                else:
                    continue
                
            newURL = URL+"&pn="+str(page+2) #&pn=X
            driver = webdriver.Chrome(options=op) 
            driver.get(newURL)
            html = driver.page_source
            driver.close()
            #print("Scraped immoscout24: page {}".format(page+1))
            
        addresses = self.__cleanAddresses__(addresses)
        trawledImmoscout = pd.DataFrame({'url':urls, 'address':addresses, 'nRooms':rooms, 'size':sizes, 'rent':prices, 'currency':currency, 
                                          'description':descriptions, 'floor':floors,
                                          'published':pdates, 'lon':lon, 'lat':lat})
        trawledImmoscout['source'] = [i[i.find(urlprefix)+len(urlprefix):i.find(".ch")] for i in trawledImmoscout.url.tolist()]

        return trawledImmoscout.drop_duplicates(subset=["url"])
    
    
    def __scrapeHomegate__(self):
        
        URL = None
        op = None
        driver = None
        
        URL = self.__getURL__()
        urlprefix = "https://www."
        
        op = webdriver.ChromeOptions()
        op.add_argument("--headless")
        op.add_argument("--no-sandbox") 
        op.add_argument("--disable-setuid-sandbox") 
        op.add_argument("--remote-debugging-port=9222")  # this
        op.add_argument("--disable-dev-shm-using") 
        op.add_argument("--disable-extensions") 
        op.add_argument("--disable-gpu") 
        op.add_argument("--headless")
        op.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.3")
        driver = webdriver.Chrome(options=op) 
        driver.get(URL)
        html = driver.page_source
        driver.close()
        
        maxPageSpan = re.search('"pageCount":\d{1,5}', html).span()
        maxPageStr = html[maxPageSpan[0]:maxPageSpan[1]].split(':')[1]
        maxPage = int(maxPageStr)
        
        chunkStart = re.search('<script>window.__INITIAL_STATE__=', html).span()[1]
        chunkEnd = re.search(',"page":\d{1,5},"pageCount":\d{1,5},"', html).span()[0]
        
        htmlChunk = html[chunkStart:chunkEnd]
                
        urls = []
        addresses = []
        prices = []
        rooms = []
        sizes = []
        descriptions = []
        floors = []
        pdates = []
        currency = []
        #imgs = []
        lon = []
        lat = []
        
        print("Homegate accessed, no. of pages: {}".format(maxPage))
        for page in range(maxPage):
            chunkStart = re.search('<script>window.__INITIAL_STATE__=', html).span()[1]
            chunkEnd = re.search(',"page":\d{1,5},"pageCount":\d{1,5},"', html).span()[0]
            
            htmlChunk = html[chunkStart:chunkEnd]
            
            matches = [m.start(0) for m in re.finditer('{"listingType":{"type":"', htmlChunk)]
            
            for miniStart in matches:
                                
                miniEnd = htmlChunk.find('"currency":',miniStart)+len('"currency":')+5
                    
                miniChunk = htmlChunk[miniStart:miniEnd]+"}}}"
                
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

                floors.append(np.nan)
                pdates.append(np.nan)
                #imgs.append('')
                lon.append(np.nan)
                lat.append(np.nan)
                
            newURL = URL+"&ep="+str(page+2) 
            driver = webdriver.Chrome(options=op) 
            driver.get(newURL)
            html = driver.page_source
            driver.close()
            #print("Scraped homegate: page {}".format(page+1))
        
        addresses = self.__cleanAddresses__(addresses)
        trawledHomegate = pd.DataFrame({'url':urls, 'address':addresses, 'nRooms':rooms, 'size':sizes, 'rent':prices, 'currency':currency, 
                                          'description':descriptions, 'floor':floors,
                                          'published':pdates, 'lon':lon, 'lat':lat})
        trawledHomegate['source'] = [i[i.find(urlprefix)+len(urlprefix):i.find(".ch")] for i in trawledHomegate.url.tolist()]
        return trawledHomegate.drop_duplicates(subset=["url"])
            

    def scrape(self):
        URL = None
        op = None
        driver = None
        if 'comparis' in self.PAGE.lower():
            return self.__scrapeComparis__()
        
        elif 'immoscout' in self.PAGE.lower():
            return self.__scrapeImmoscout__()
        
        elif 'homegate' in self.PAGE.lower():
            return self.__scrapeHomegate__()
        
        
        elif 'all' in self.PAGE.lower():
            URL = None
            html = None
            op = None
            driver = None
            
            print("\nScraping: 'comparis'")
            self.PAGE = 'comparis'
            comparisTrawl =  self.__scrapeComparis__()
            URL = None
            op = None
            driver = None

            print("\nScraping: 'homegate'")
            self.PAGE = 'homegate'
            homegateTrawl = self.__scrapeHomegate__()
            URL = None
            op = None
            driver = None
                        
            print("\nScraping: 'immoscout'")
            self.PAGE = 'immoscout'
            immoscoutTrawl = self.__scrapeImmoscout__()
            URL = None
            op = None
            driver = None
            
            self.PAGE = 'all'
            
            concatenated = pd.concat([homegateTrawl,immoscoutTrawl,comparisTrawl])
            concatenated = concatenated.drop_duplicates(subset=["address","size","rent"], keep='last').reset_index()
            concatenated = concatenated.drop(labels=['index'],axis=1)
            
            return concatenated
        
def cleanDescription(dataframe, keywords):
    """Omit entries by keyword"""

    if type(keywords) != list:
        keywords = list(keywords)
    
    for key in keywords:
        dataframe = dataframe[~dataframe.description.str.contains(key)]
    
    dataframe = dataframe.reset_index()
    dataframe = dataframe.drop(labels=['index'],axis=1)
    return dataframe
    

def locate(dataframe,addresscolumn='address'):
    """Geocoding"""
    LONS = [float(x) for x in dataframe.lon.tolist()]
    LATS = [float(x) for x in dataframe.lat.tolist()]
    dataframe['lon'] = LONS
    dataframe['lat'] = LATS
    
    withLats = dataframe.loc[(dataframe.lat > 0)].copy()
    withoutLats = dataframe.loc[~(dataframe.lat > 0)].copy()
     
    lats = []
    lons = []
    nom = Nominatim(user_agent="FindApartment", scheme='http', domain='nominatim.openstreetmap.org')
    
    for i,item in enumerate(withoutLats[addresscolumn].tolist()):
        adressToBeLocated = item.replace(', ZH','')+', Schweiz' 
        location = nom.geocode(adressToBeLocated)
        
        if type(location) == type(None):
            lats.append(None)
            lons.append(None)
        
        else:
            location = location.raw
            lats.append(float(location['lat']))
            lons.append(float(location['lon']))
            
            
    withoutLats['lat'] = lats
    withoutLats['lon'] = lons
    
    addedCoordinates = pd.concat([withLats,withoutLats])
    return addedCoordinates

def locateAddress(addressString):
    nom = Nominatim(user_agent="FindApartment", scheme='http', domain='nominatim.openstreetmap.org')
    location = nom.geocode(addressString)
    location = location.raw

    lat = float(location['lat'])
    lon = float(location['lon'])
    
    return lat, lon

def getCommuteTimes(dataframe, targetLat, targetLon, startCommute=''):
    """Get commute time using a SBB-API"""
    html = None
    op = None
    driver = None
    
    targetLat = str(targetLat)
    targetLon = str(targetLon)
    
    if startCommute == '':
        today = date.today()
        nextMonday = today + timedelta(days=-today.weekday(), weeks=1)
        startCommute = nextMonday.strftime("%Y-%m-%d")+"T08%3A00"
        

    avgMinutes = []
    print("Retrieving commuting times")
    i = 0
    for idx,row in dataframe.iterrows():
        ############ LOOPING THROUGH EACH ENTRY IS rather SLOW 
        lat = str(row['lat'])
        lon = str(row['lon'])
        getURL = 'http://transport.opendata.ch/v1/connections?from='+lat+'+'+lon+'&to='+str(targetLat)+'+'+str(targetLon)+'&datetime='+startCommute 
        req = requests.get(getURL)
        toJSON = json.loads(req.text) 

        mins = []
        for connection in toJSON['connections']:
            h = connection['duration'][3:]
            delta = timedelta(hours=float(h.split(':')[0]), minutes=float(h.split(':')[1]), seconds=float(h.split(':')[2]))
            minutes = delta.total_seconds()/60
            mins.append(minutes)
        avgMinute = np.int32(np.average(mins))
        avgMinutes.append(avgMinute)
        
        if (i%10==0) and i>0:
            print("Commuting time retrieved for {} of {} entries.".format(i,len(dataframe)))
        i += 1
        
    print("Commuting time retrieved for {} of {} entries.".format(len(avgMinutes),len(dataframe)))         
    dataframe['avgMinutes'] = avgMinutes
    return dataframe

def createTargetGEOJSON(outputPath,address,title= 'Destination', comment=''):
        
    lat,lon = locateAddress(address)
    geometry = Point(lon,lat)
    
    targetgdf = gpd.GeoDataFrame({'address':[address], 'title':[title], 'comment':[comment], 'geometry': [geometry]})
    targetgdf.to_file(outputPath, driver="GeoJSON")
      
    geojsoned = targetgdf.to_json()
    gdf = 'var targetAddress='+geojsoned
    with open(outputPath, 'w') as file:
        file.write(gdf)
        
        






