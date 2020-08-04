# entity_resolution


|Service|Status|
| -------------: | :---- |
| Travis Build   | [![Build Status](https://travis-ci.org/JeffreyJackovich/entity_resolution.svg?branch=master)](https://travis-ci.org/JeffreyJackovich/entity_resolution) |
| Coverage | [![codecov](https://codecov.io/gh/JeffreyJackovich/entity_resolution/branch/master/graph/badge.svg)](https://codecov.io/gh/JeffreyJackovich/entity_resolution) |


## Goal
Scripts to setup a Postgres database with a 1Million+ record dataset to assess 
['string_grouper'](https://github.com/Bergvca/string_grouper), a library that 
makes finding groups of similar strings within a single or within multiple lists of strings easy.

string_grouper uses tf-idf to calculate the cosine similarities within a single list or between two lists of strings.

 
## Objectives
1. Setup Postgres database with 4 tables 
2. Assess string_grouper performance vs. SQL 'GROUP BY':
    1. Quantity of duplicate records identified 
    2. Query duration

## Dataset
There are about 700,000 unduplicated donors in this database of Illinois political campaign contributions.  

## Setup
1. Setup a virtual environment (*requires Python version >3.7)  
2. Install dependencies `pip install -r requirements.txt`
3. Create a PostgreSQL database, set environment variables with your PostgreSQL connection details

## Quick Start
```bash
git clone https://github.com/JeffreyJackovich/entity_resolution
cd entity_resolution
```
1. Get exact duplicates via SQL GROUP BY. `python main.py -ged`
2. Get partial duplicates via string_grouper. `python main.py -gpd`
3. Pass in SQL query from command line. Example `python main.py -q "SELECT count(distinct name) FROM processed_donors;"` 


## Results
 
| Method | Time | Unique Count | Duplicate Count| Total Count |
| :-------------: | :----: | :----: | :----: | :----: |
| SQL | 13.40 sec (0.223 min)  |  432,201 | 273,829 | 706,030 |
| string_grouper | 1575.6 sec (26.26 min)| 315,088 | 390,942 | 706,030 |        


(Source table.column: processed_donors.name)


## Postgresql db Setup commands
1. Download the 'Illinois political campaign contributions' dataset. `python main.py -gd`
2. Setup database tables. `python main.py -sd`


## What is Entity Resolution?
Entity Resolution (ER) refers to the task of finding records in a dataset that refer to the same entity across different data sources (e.g., data files, books, websites, databases). ER is necessary when joining datasets based on entities that may or may not share a common identifier (e.g., database key, URI, National identification number), as may be the case due to differences in record shape, storage location, and/or curator style or preference. A dataset that has undergone ER may be referred to as being cross-linked [2].


## Data Source
Illinois Campaign Contributions, Github repository, https://s3.amazonaws.com/dedupe-data/Illinois-campaign-contributions.txt.zip

## Credits
[1] Van Den Berg, C., (2020). string_grouper. Github repository, https://github.com/Bergvca/string_grouper

[2] ChunLu, Y., (2020), Entity Resolution On Text Similarity. Github repository, https://github.com/YungChunLu/Entity-Resolution-On-Text-Similarity/blob/master/PySpark%20-%20Entity%20Resolution.ipynb
