# Webscraper for finding an apartment (in Switzerland)
## WebScraper_ApartmentsInSwitzerland
Next to criterias such as price, number of rooms and area, I was looking for an apartment in a 'favourable area' and with a limited commute time to work.
The following 'tool' successfully served me as a decision support during my apartment hunt.
<br>
Since not many websites offered a service for the latter mentioned criteria, a little script has been set up scraping three commonly used websites for apartments. 
A HTML document displays the scraped and geocoded data on a map using leaflet (https://leafletjs.com/) and maptiler (https://www.maptiler.com/). To be able to use 
the map, you require a maptiler API Key which must be inserted to the HTML file by replacing ':::API-KEY:::'. 
<br>
The script contains a minor inconvenience, 'falsely entered' addresses must be manually corrected. Ideas to avoid manual interference are welcome. 
<br>
This has been my first project setting up a webscraper. Your feedback is more than welcome. 

