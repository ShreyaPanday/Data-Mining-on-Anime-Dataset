# Data Modeling and Rule Mining
This repository contains a project that implements data modeling (ETL Transactions) and Data Mining on a dataset containing 50M+ tuples.

# Data Description
The chosen dataset is the information about a list of Animes and their corresponding Viewers, their ratings and preferences. 

The dataset is picked from Kaggle - https://www.kaggle.com/datasets/hasegawa9823174/myanimelist-2021

The data is constituted of 8 csv files - <br /> • users.csv <br />
• anime.csv <br />
• anime_genres.csv <br />
• anime_studios.csv <br />
• anime_list.csv <br />
• favourite_chracters.csv <br />
• favourite_manga.csv 

# ETL Transactions
To do a comparitive analysis for the better choice of data-model to use , both relational and document based models are implemented. 

## Relational Model
<img width="1200" height="500" alt="image" src="https://github.com/user-attachments/assets/a2907721-d2f8-4cc7-8b77-c3dfeff55311">

DB used for storing the data in a relational model - $${\color{green} PostgreSQL}$$ <br /> 
Code - <code style="color : green">relational_model.py</code>

## Document-Based Model
The document based model has 2 collections - anime and users. <br /> 
<img width="255" height="350" alt="image" src="https://github.com/user-attachments/assets/7af8d40b-86ed-4054-98fe-1543a9379596"> 
<img width="385" height="300" alt="image" src="https://github.com/user-attachments/assets/f6d13997-8e5b-4275-85b5-e1c323519018"> <br />
DB used for storing the data in a relational model - $${\color{green} MongoDB}$$ <br />
Code- <code style="color : green">document_model.py</code>

## Data Cleaning
The following dirty data issues were found and handled while loading - • Missing data • Outliers •Invalid Data •Non-Uniform Data •Timeliness <br /> 
Data Cleaning Code - <code style="color : green">data_cleaning.py</code>

## Normalisation 
The data in the tables is present in the 3NF form. It has all atomic values and non redundant ransitive dependancies. 

# Data Mining 

## Frequent Itemset Mining 

The attribute chosen to perform the itemset mining is the “anime genres” .
Thus the final association between genres will indicate the trends in the genres that are
generally being watched by a particular user.

The frequent itemset mining is being done using lattice formation by finding the genres(greater than a threshold) that occur together for user preferences as we go up the lattice levels. 
A custom SQL query is written in the program that finds the elements in each level of the lattice. <br /> 

Code - <code style="color : green">creating_latices.py</code>

## Results and Implications

In the last lattice - Level 9 , the observation included the following combination of genres: <br /> 
Kids, Comedy, Shonen, Samurai, Horror, Sports, Romance, Demons, Mecha

<code style="color : red">• Streaming platforms can purchase/renew anime rights based on viewer habits. <br /> 
• MyAnimeList is lucrative for streaming companies and hence will become well funded. <br /> 
• Anime adaptations of niche Manga/light novels can get greenlit based on their niche combination of genres. <br /> </code>
