
|Service|Status|
| -------------: | :---- |
| Travis Build   | [![Build Status](https://travis-ci.org/JeffreyJackovich/entity_resolution.svg?branch=master)](https://travis-ci.org/JeffreyJackovich/entity_resolution) |
| Coverage | [![codecov](https://codecov.io/gh/JeffreyJackovich/entity_resolution/branch/master/graph/badge.svg)](https://codecov.io/gh/JeffreyJackovich/entity_resolution) |


## Goal
1. Provide scripts to setup a Postgres database with a 1Million+ records.
2. Assess Postgres's GROUP BY vs ['string_grouper'](https://github.com/Bergvca/string_grouper), a library that finds 
groups of similar strings using tf-idf to calculate the cosine similarities within a single list or between two lists 
of strings.

 
## Objectives
1. Setup Postgres database with 4 tables 
2. Assess string_grouper performance vs. SQL 'GROUP BY':
    1. Quantity of duplicate records identified 
    2. Query duration

## Dataset
There are about 700,000 unduplicated donors in this database of Illinois political campaign contributions.[3]  

## Setup  
1. Setup a virtual environment (*requires Python version >3.7)  
2. Install dependencies `pip install -r requirements.txt`
3. Create a PostgreSQL database, set environment variables with your PostgreSQL connection details



```bash
git clone https://github.com/JeffreyJackovich/entity_resolution
cd entity_resolution
```
#### Postgres Setup 
1. Download the 'Illinois political campaign contributions' dataset. 
```bash 
python main.py -gd 
```
2. Setup database tables.
```bash 
python main.py -sd
```

#### Duplicate Identification
1. Get exact duplicates via SQL GROUP BY. 
```bash 
python main.py -ged
```

2. Get partial duplicates via string_grouper.
```bash 
python main.py -gpd
```

#### Commandline SQL query for exploratory analysis, etc. 
Example query:  
```bash 
python main.py -q "SELECT count(distinct name) FROM processed_donors;"
```


## Results
 
| Method | Time | Unique Count | Duplicate Count| Total Count |
| :-------------: | :----: | :----: | :----: | :----: |
| SQL | 13.40 sec (0.223 min)  |  432,201 | 273,829 | 706,030 |
| string_grouper | 1575.6 sec (26.26 min)| 315,088 | 390,942 | 706,030 |        
| Difference| +1562.2 sec (+26.03 min) | -117,113 | +117,113 | 0 |

string_grouper default paramaters: 
(Source Data: table.column: processed_donors.name)
Laptop Processor: 2.5 GHz Intel Core i5

## Conclusion
string_grouper identified 117,113 more records as duplicate compared to using 'group by' with SQL.  Next step 
is assess string_grouper's performance based on: recall, precision, and F-1 Score.  

## Next Steps
1. Evaluate string_grouper's performance based on: Recall, Precision, and F-1 Score.    

2. Test the functions available in Postgresql's [fuzzstrmatch](https://www.postgresql.org/docs/9.6/fuzzystrmatch.html) 
  module to determine similarities and distance between strings.

3. Use Apache Spark's to perform Entity Resolution.  


## What is Entity Resolution?
Entity Resolution (ER) refers to the task of finding records in a dataset that refer to the same entity across different
data sources (e.g., data files, books, websites, databases). ER is necessary when joining datasets based on entities 
that may or may not share a common identifier (e.g., database key, URI, National identification number), 
as may be the case due to differences in record shape, storage location, and/or curator style or preference. A dataset 
that has undergone ER may be referred to as being cross-linked [2].


## Reference
[1] Van Den Berg, C., (2020). string_grouper. Github repository, https://github.com/Bergvca/string_grouper

[2] ChunLu, Y., (2020), Entity Resolution On Text Similarity. Github repository, https://github.com/YungChunLu/Entity-Resolution-On-Text-Similarity/blob/master/PySpark%20-%20Entity%20Resolution.ipynb

[3] Illinois Campaign Contributions, Github repository, https://s3.amazonaws.com/dedupe-data/Illinois-campaign-contributions.txt.zip