# entity_resolution


## Goal
Compare the performance to identify duplicate records with string_grouper (tf-idf and cosine similarity) 
vs Postgresql's 'GROUP BY' in a dataset with exact and partial duplicates. 


## Objectives
Assess the perforamnce based on:
    1. Duplicate records identified 
    2. Query duration
 

## Steps
1. Setup a virtual env (*requires Python version >3.7)  
2. Install dependencies 'pip install -r requirements.txt' 
3. Create a PostgreSQL database


## Results



## What is Entity Resolution
Entity Resolution (ER) refers to the task of finding records in a dataset that refer to the same entity across different data sources (e.g., data files, books, websites, databases). ER is necessary when joining datasets based on entities that may or may not share a common identifier (e.g., database key, URI, National identification number), as may be the case due to differences in record shape, storage location, and/or curator style or preference. A dataset that has undergone ER may be referred to as being cross-linked [2].

## Credits
[1] Van Den Berg, C., (2020). string_grouper. Github repository, https://github.com/Bergvca/string_grouper

[2] ChunLu, Y., (2020), Entity Resolution On Text Similarity. Github repository, https://github.com/YungChunLu/Entity-Resolution-On-Text-Similarity/blob/master/PySpark%20-%20Entity%20Resolution.ipynb

## Data Source
Illinois Campaign Contributions, Github repository, https://s3.amazonaws.com/dedupe-data/Illinois-campaign-contributions.txt.zip
