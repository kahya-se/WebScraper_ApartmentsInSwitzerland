# WebScraper for finding an apartment (in Switzerland)
<br>
Next to criterias such as price, number of rooms and size of the apartment, I was looking for an apartment in a 'favourable neighbourhood' and with a limited commute time to work.
The following 'tool' successfully served me as a decision support during my apartment hunt.
<br>    
<br>
Since not many websites offered a service for the latter mentioned criteria, a little script has been set up scraping three commonly used websites for apartments, 
namely immoscout24, homegate, and comparis. A HTML document displays the scraped and geocoded data on a map using leaflet (https://leafletjs.com/) and 
maptiler (https://www.maptiler.com/). To be able to use 
the map, you require a maptiler API Key which has to be inserted to the HTML file by replacing '':::API-KEY:::''. 
<br>    
<br>
The script contains a minor inconvenience: addresses entered 'falsely' to the websites (as shown in the jupyter notebook) must be corrected manually. 
Ideas to avoid manual interference are welcome. 
<br>    
<br>
Feel free to add functions retrieving commuting time using a bicycle or to improve the retrieval of the commuting times using the SBB API. 
Needless to say, I haven't paid attention to the ''TheMap.html''. Be my guest and improve the user experience of it. 

## The result
As shown below, the background map can be changed between imagery and vector data. 
![Map view 1](https://github.com/kahya-se/WebScraper_ApartmentsInSwitzerland/blob/main/imgs/example02.png?raw=true)
![Map view 2](https://github.com/kahya-se/WebScraper_ApartmentsInSwitzerland/blob/main/imgs/example03.png?raw=true)
