# Data Modeling and Rule Mining
This repository contains a project that implements data modeling (ETL Transactions) and Data Mining on a dataset containing 50M+ tuples.

# Data Description
The chosen dataset is the information about a list of Animes and their corresponding Viewers, their ratings and preferences. 

The dataset is picked from Kaggle - https://www.kaggle.com/datasets/hasegawa9823174/myanimelist-2021

The data is constituted of 8 csv files - • users.csv 
• anime.csv
• anime_genres.csv
• anime_studios.csv
• anime_list.csv
• favourite_chracters.csv
• favourite_manga.csv

# ETL Transactions
To do a comparitive analysis for the better choice of data-model to use , both relational and document based models are implemented. 

## Relational Model
<img width="1200" height="500" alt="image" src="https://github.com/user-attachments/assets/a2907721-d2f8-4cc7-8b77-c3dfeff55311">

DB used for storing the data in a relational model - PostgreSQL <br /> 
Code- relational_model.py

## Document-Based Model
The document based model has 2 collections - anime and users. <br /> 
<img width="255" height="350" alt="image" src="https://github.com/user-attachments/assets/7af8d40b-86ed-4054-98fe-1543a9379596"> 
<img width="385" height="300" alt="image" src="https://github.com/user-attachments/assets/f6d13997-8e5b-4275-85b5-e1c323519018"> 
DB used for storing the data in a relational model - MongoDB <br /> 
Code- document_model.py

## Data Cleaning
The following dirty data issues were found and handled while loading - • Missing data • Outliers •Invalid Data •Non-Uniform Data •Timeliness
Data Cleaning Code - data_cleaning.py








