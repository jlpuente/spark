# PySpark vs Pandas vs Polars: A comparative study

This challenge is intended to investigate the computational performance of PySpark, Pandas and  [Polars](https://www.pola.rs/)  when dealing with identical analysis on the same data. Intuitively, Pandas should be faster when the size of the data is small and PyPark should deliver a best performance when the size of the data is large. The point is to quantify what is intended by "small" and "large" in this context.

The starting point of this challenge is to take the following data files:

-   [Film location in San Francisco](https://data.sfgov.org/Culture-and-Recreation/Film-Locations-in-San-Francisco/yitu-d5am/data). Size: 1,6 MB
-   [San Francisco crimes](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783). Size: 58 MB   
-   [Chicago crimes](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2). Size: 1.6 GB  
    

and define a set of simple analisys procedures (e.g., grouping and counting, filtering, or sorting) to be implemented in the three systems and compare them by measuring the computing time to carry out the analysis. The conclusions of the work should include recommendations about which tool should be used depending on the data size. An important factor is the computer configuration where the programs are executed; this information must be clearly stated.

The deliverable of the task should be a Jupyter notebook describing the study.
