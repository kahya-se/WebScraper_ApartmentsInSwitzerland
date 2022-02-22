# WebScraper for finding an apartment (in Switzerland)
<br>
Next to criterias such as price, number of rooms and size of the apartment, I was looking for an apartment in a 'favourable neighbourhood' and with a limited commute time to work.
The following 'tool' successfully served me as a decision support during my apartment hunt.
<br>    
<br>
Since not many websites offered a service for the latter mentioned criteria, a little script has been set up scraping three commonly used websites for apartments. 
A HTML document (with the sophistication one step further than hello_world.html) displays the scraped and geocoded data on a map using leaflet (https://leafletjs.com/) and 
maptiler (https://www.maptiler.com/). To be able to use 
the map, you require a maptiler API Key which has to be inserted to the HTML file by replacing '':::API-KEY:::''. 
<br>    
<br>
Feel free to add functions retrieving commuting time using a bicycle or to improve the retrieval of the commuting times using the SBB API. 
Needless to say, I haven't paid attention to the ''user experience of ScrapedApartmentsMap.html''. Be my guest and help me to improve the user experience. 

## Notes on the improved version
Bullet point summary of the improvements:
* Beware, names of classes and functions might have changed!
* More verbose descriptions of classes and functions
* For loops are -where possible and sensible- multi-threaded. This has a significant effect on reducing the time for the retrieval of the commuting time.  
* Replacing 'headless selenium' with requests. Significant impact on query time.
* Functions to run Nominatim locally. Significant impact on query time and useful for scale-up.
* More object oriented implementation  
<br>
The new implementation might come handy for scraping data beyond the original motive of having a decision support tool. As for instance, to grasp the housing market in Switzerland or set up a newsletter with an alert, if there is a new apartment for rent in an area of interest...  

## Visualization (beyond the jupyter notebook)
As shown below, the background map can be changed between imagery and vector data. 
![Map view 1](https://github.com/kahya-se/WebScraper_ApartmentsInSwitzerland/blob/main/imgs/example02.png?raw=true)
![Map view 2](https://github.com/kahya-se/WebScraper_ApartmentsInSwitzerland/blob/main/imgs/example03.png?raw=true)

## Final note
This WebScraper retrieves data from other sources and cannot claim correctness, completeness, accuracy or actuality of the data. 
The use of this WebScraper does not make you the owner the data. 
The content found in this repository is intended for non-commercial, educational use only.
Beware, your IP will be banned from the comparis site, if used extensively. 

<br> 
<br> 
<br> 
## Very final note
Feedback is highly appreciated, since I have no formal training on computer science. 
