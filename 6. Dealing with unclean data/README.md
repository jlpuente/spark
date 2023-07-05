# Dealing with unclean data

Typically, data to be analyzed in real world applications is not fully clean. Frequently, there are missing fields, invalid values, etc.  

A civil engineer is working in the design of a bridge, trying to find different alternatives, each of them having a total bridge weight and the degree of deformation in certain parts (e.g., see  [http://ebesjmetal.sourceforge.net/problems.html](http://ebesjmetal.sourceforge.net/problems.html)). After using an optimization software, she/he has obtained a .txt file (attached to this task) with a number of rows indicating different trade-off designs.

Unfortunately, some lines/fields have invalid values (blank lines, missing values, characters instead of numbers, etc), and there are also repeteated lines.  

This task consists in developing a Jupyter notebook with PySpark to read the file, remove all the invalid lines and remove those that are appears more than one time, and plot the clean data.
