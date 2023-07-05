# Spanish airports

The Web site  [OurAirports](http://ourairports.com/)  provides open data about international airports. The data file can be downloaded from this link:  [airports.csv](http://ourairports.com/data/airports.csv). This is a  [CSV file](https://en.wikipedia.org/wiki/Comma-separated_values)  containing, among other information, the following fields:

- type: the type of the airport ("heliport", "small_airport", "large_airport", "medium_airport", etc)  

- iso_country: the code of Spain is "ES".

Taking as example the WordCount program, write two versions of a RDD-based Spark program called "SpanishAirports" (SpanishAirports.java and SpanishAirports.py) that read the airports.csv file and counts the number of spanish airports for each of the four types, writing the results in a text file.  

The program outputs should look similar to this:

```
"small_airport": 291  
"heliport": 60  
"medium_airport": 41  
"closed": 13  
"large_airport": 10  
"seaplane_base": 1  
```
The deliverables of this exercise will be three files, two containing the source codes and another one with the output file. 
