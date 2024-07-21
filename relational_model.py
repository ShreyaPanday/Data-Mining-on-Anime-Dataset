"""
Author Shreya Shivanand Pandey

This script creates tables, parses the zst compressed MyAnimeList CSV dataset and inserts it into the database
"""

import pandas as pd
import zstandard as zstd
import psycopg2 as psy
import time

# Configure the constants for connecting to the DB
# Ensure dependencies such as psycopg2,Pandas are installed
# Ensure all the csv.zst files are present in the /mal_data/ directiory

# Run the script normally using any python IDE such a as PyCharm or VSCode after ensuring all the preconditions, the script will return time elapsed for runnning the script

# Config Constants 
DB = "mal"
USER = ""
PASS = ''
HOST = '127.0.0.1'
PORT = '5432'
FILENAMES = ["users.csv.zst","anime.csv.zst","anime_genres.csv.zst","anime_studios.csv.zst","favorite_anime.csv.zst","favorite_characters.csv.zst","favorite_manga.csv.zst","anime_list.csv.zst"]

CREATE_TABLES = """CREATE TABLE Users (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(127),
    gender VARCHAR(20),
    birthday TIMESTAMP,
    location VARCHAR(255),
    joined TIMESTAMP,
    days_watched DECIMAL(5,2),
    mean_score DECIMAL(3,2),
    watching INT,
    completed INT,
    on_hold INT,
    dropped INT,
    plan_to_watch INT,
    total_entries INT,
    rewatched INT,
    episodes_watched INT
);


CREATE TABLE Anime (
    anime_id INT PRIMARY KEY,
    title VARCHAR(2550),
    title_english VARCHAR(2550),
    title_japanese VARCHAR(2550),
    type VARCHAR(20),
    source VARCHAR(32),
    episodes INT,
    status VARCHAR(50),
    airing BOOLEAN,
    aired_from TIMESTAMP,
    aired_to TIMESTAMP,
    duration VARCHAR(50),
    rating VARCHAR(50),
    score DECIMAL(3,2),
    scored_by INT,
    rank INT,
    popularity INT,
    members INT,
    favorites INT,
    synopsis TEXT,
    background TEXT,
    premiered VARCHAR(50),
    broadcast VARCHAR(50)
);


CREATE TABLE Anime_Genres (
    anime_id INT,
    genres VARCHAR(100),
    PRIMARY KEY (anime_id, genres),
    FOREIGN KEY (anime_id) REFERENCES Anime(anime_id)
);


CREATE TABLE Anime_Studios (
    anime_id INT,
    studio VARCHAR(100),
    PRIMARY KEY (anime_id, studio),
    FOREIGN KEY (anime_id) REFERENCES Anime(anime_id)
);


CREATE TABLE Anime_List (
    user_id INT,
    anime_id INT,
    watching_status INT,
    score INT,
    watched_episodes INT,
    is_rewatching BOOLEAN,
    watch_start_date TIMESTAMP,
    watch_end_date TIMESTAMP,
    priority VARCHAR(10),
    PRIMARY KEY (user_id, anime_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id),
    FOREIGN KEY (anime_id) REFERENCES Anime(anime_id)
);


CREATE TABLE Favorite_Anime (
    user_id INT,
    anime_id INT,
    PRIMARY KEY (user_id, anime_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id),
    FOREIGN KEY (anime_id) REFERENCES Anime(anime_id)
);


CREATE TABLE Favorite_Characters (
    user_id INT,
    character_id INT,
    PRIMARY KEY (user_id, character_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);


CREATE TABLE Favorite_Manga (
    user_id INT,
    manga_id INT,
    PRIMARY KEY (user_id, manga_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);"""

# Creates the necessary tables in the database
def create_tables(cursor,conn):
    try:
        cursor.execute(CREATE_TABLES)
        conn.commit()
        print("Tables Created")
    except:
        conn.rollback()
        print("Failed to create Tables")

# Reads the zst files and returns a Textreader object to process data on 
def read_zstd(file):
    x = pd.read_csv(zstd.ZstdDecompressor().stream_reader(file),iterator=True,chunksize=1)
    return x

# Assembles query for the specified dataset and commits it to the db
def query(cursor,table,conn,**items):
    table_name = table[0:-8]
    keys = ["%s" % k for k in items]
    values = ["'%s'" % v for v in items.values()]
    building_block = []
    building_block.append("insert into %s (" % table_name)
    building_block.append(", ".join(keys))
    building_block.append(") values (")
    building_block.append(", ".join(values))
    building_block.append(");")
    q = "".join(building_block)
    try:
        cursor.execute(q)
        conn.commit()
    except:
        conn.rollback()

# Main function determines the opening and closing of connections and the order of operations
def main():
    start_time = time.time()
    conn = psy.connect(database=DB, user=USER, password=PASS, host=HOST, port=PORT)
    cursor = conn.cursor()
    count = 0
    create_tables(cursor,conn)

    for f_name in FILENAMES:
        print("Processing",f_name)
        file = open("mal_data/"+f_name,"rb")
        df = read_zstd(file)
        ch = df.get_chunk(1).to_dict(orient='records')
        while(ch[0]):
            try:
                query(cursor,f_name,conn,**ch[0])
                count+=1
            except:
                pass
            try:
                 ch = df.get_chunk(1).to_dict(orient='records')
            except:
                break
        file.close()
        
    
    end_time = time.time()
    time_taken = end_time - start_time
    print(time_taken,"Seconds Elapsed For Running the Script", count,"Entries Processed")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()