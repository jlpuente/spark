# Analysis of access to electricity (source: the World Bank)

The World Bank offers a large amount of  [open data](https://data.worldbank.org/). In this exercise we will focus on the percentage of population having access to electricity in all the countries of the world from 1960 to 2014. The data can be obtained from this link:  [https://data.worldbank.org/indicator/EG.ELC.ACCS.ZS.](https://data.worldbank.org/indicator/EG.ELC.ACCS.ZS)  After selecting the CSV format you will get a zip file containing, among others, these two files:

-   API_EG.ELC.ACCS.ZS_DS2_en_csv_v2.csv

-   Metadata_Country_API_EG.ELC.ACCS.ZS_DS2_en_csv_v2.csv  
    
After taking a look at the content of these files, write a program named `electricityAnalysis.py/java` which, given a range of years (e.g., 1970 and 1980), prints on the screen the average number of countries of each of the regions (South Asia, North America, etc.) having at least 99% of electricity for all the years between the indicated range. The output must be ordered in descendent order by the percentage value, and it should look like this example (note: these data are invented):  

```
Years: 1970 - 1980  

  
| Region                | Percentage |
|-----------------------|------------|
| North America         | 100 %      |
| Europe & Central Asia | 99 %       |
...
```

You have to take into account that the two files have headers and some lines might have missing fields.  

The deliverables of this exercise will be the program file and the screen capture of its output.
