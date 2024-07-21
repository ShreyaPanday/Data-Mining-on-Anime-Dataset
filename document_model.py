"""
Author - Shreya Pandey

This script picks the data from the postgres tables and creates a document based model on mongodb for them .
"""

'''
Instructions to run the program : It’s a python program. To run the program, we need to directly run the program in any python IDE such as PyCharm or VSCode. 
The code makes use of the psycopg2 library and hence to run the program, we need to also install the psycopg2 library. 

You can do that using the “pip install psycopg2” command. 

'''


import psycopg2
from datetime import datetime
from pymongo import MongoClient



def connect_to_mongodb():
    try:
        client = MongoClient(connection_url)
        db = client[mongo_db_name]
        return db, client
    except Exception as e:
        print("Error connecting to MongoDB:", e)

def create_collection():
    try:
        db, client = connect_to_mongodb()
        col_name = "users"
        col2_name = "anime"
        db.create_collection(col_name)
        print("Collection '{}' created successfully.".format(col_name))
        db.create_collection(col2_name)
        print("Collection '{}' created successfully.".format(col2_name))
    except Exception as e:
        print("Error creating collection:", e)
    finally:
        client.close()

def fetch_anime_studios_data():
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM anime_studios")
    rows = cursor.fetchall()

    studio_data = {}
    for row in rows:
        studio_info = {
            "Studio": row[1]
        }
        anime_id = row[0]
        if anime_id not in studio_data:
            studio_data[anime_id] = []
        studio_data[anime_id].append(studio_info)


    return studio_data

def fetch_anime_genres_data():
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM anime_genres")
    rows = cursor.fetchall()

    genres_data = {}
    for row in rows:
        genres_info = {
            "Genres": row[1]
        }
        anime_id = row[0]
        if anime_id not in genres_data:
            genres_data[anime_id] = []
        genres_data[anime_id].append(genres_info)

    return genres_data

def fetch_favorite_anime_data():
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM favorite_anime")
    rows = cursor.fetchall()


    favorite_anime_data = {}
    for row in rows:
        favorite_anime_info = {
            "Favorite_Anime": row[1]
        }
        user_id = row[0]
        if user_id not in favorite_anime_data:
            favorite_anime_data[user_id] = []
        favorite_anime_data[user_id].append(favorite_anime_info)

    return favorite_anime_data

def fetch_favorite_manga_data():
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM favorite_manga")
    rows = cursor.fetchall()

    favorite_manga_data = {}
    for row in rows:
        favorite_manga_info = {
            "Favorite_Manga": row[1]
        }
        user_id = row[0]
        if user_id not in favorite_manga_data:
            favorite_manga_data[user_id] = []
        favorite_manga_data[user_id].append(favorite_manga_info)

    return favorite_manga_data

def fetch_favorite_characters_data():
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM favorite_characters")
    rows = cursor.fetchall()
    favorite_characters_data = {}
    for row in rows:
        favorite_characters_info = {
            "Favorite_Characters": row[1]
        }
        user_id = row[0]
        if user_id not in favorite_characters_data:
            favorite_characters_data[user_id] = []
        favorite_characters_data[user_id].append(favorite_characters_info)


    return favorite_characters_data


def fetch_anime_data():
    try:
        conn = connection_with_database()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM anime")
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        print("Error fetching anime table data:", e)
    finally:
        cursor.close()
        conn.close()

def fetch_users_data():
    try:
        conn = connection_with_database()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        print("Error fetching users table data:", e)
    finally:
        cursor.close()
        conn.close()


def load_data_to_mongodb_users(favorite_anime_data, favorite_manga_data, favorite_characters_data):
    try:
        db, client = connect_to_mongodb()
        collection = db["users"]
        rows = fetch_users_data()
        for row in rows:
            user_id = row[0]
            favorite_anime = favorite_anime_data.get(user_id, [])
            favorite_manga =  favorite_manga_data.get(user_id, [])
            favorite_characters = favorite_characters_data.get(user_id, [])

            fav_animes = [{"Favorite_Anime": anime["Favorite_Anime"]} for anime in favorite_anime]
            fav_mangas = [{"Favorite_Manga": anime["Favorite_Manga"]} for anime in favorite_manga]
            fav_characters = [{"Favorite_Characters": anime["Favorite_Characters"]} for anime in favorite_characters]

            user_data = {
                "_id": user_id,
            }

            if row[1]:
                user_data["Name"] = row[1]
            if row[2]:
                user_data["Gender"] = row[2]
            if row[3]:
                user_data["Birthday"] = row[3]
            if row[4]:
                user_data["Location"] = row[4]
            if row[5]:
                user_data["Joined"] = row[5]
            if row[6]:
                user_data["Days_Watched"] = int(row[6])
            if row[7]:
                user_data["Mean_Score"] = int(row[7])
            if row[8]:
                user_data["Watching"] = row[8]
            if row[9]:
                user_data["Completed"] = row[9]
            if row[10]:
                user_data["On_Hold"] = row[10]
            if row[11]:
                user_data["Dropped"] = row[11]
            if row[12]:
                user_data["Plan_To_Watch"] = row[12]
            if row[13]:
                user_data["Total_Enteries"] = int(row[13])
            if row[14]:
                user_data["Rewatched"] = row[14]
            if row[15]:
                user_data["Episodes_Watched"] = row[15]
            if fav_animes:
                user_data["Favorite_Animes"] = fav_animes
            if fav_mangas:
                user_data["Favorite_Mangas"] = fav_mangas
            if fav_characters:
                user_data["Favorite_Characters"] = fav_characters
            collection.insert_one(user_data)
        print("Data loaded into MongoDB successfully.")
    except Exception as e:
        print("Error loading data to MongoDB:", e)
    finally:
        client.close()

def load_data_to_mongodb_anime(studio_data, genres_data):
    try:
        db, client = connect_to_mongodb()
        collection = db["anime"]
        rows = fetch_anime_data()
        for row in rows:
            anime_id = row[0]
            anime_studios = studio_data.get(anime_id, [])  # Retrieve badges for the user if available
            anime_genres =  genres_data.get(anime_id, [])

            studios = [{"Studios": studio["Studio"]} for studio in anime_studios]
            genres = [{"Genres": genres["Genres"]} for genres in anime_genres]
            anime_data = {
                "_id": anime_id,
            }

            if row[1]:
                anime_data["Title"] = row[1]
            if row[2]:
                anime_data["Title_English"] = row[2]
            if row[3]:
                anime_data["Title_Japanese"] = row[3]
            if row[4]:
                anime_data["Type"] = row[4]
            if row[5]:
                anime_data["Source"] = row[5]
            if row[6]:
                anime_data["Episodes"] = row[6]
            if row[7]:
                anime_data["Status"] = row[7]
            if row[8]:
                anime_data["Airing"] = row[8]
            if row[9]:
                anime_data["AiredFrom"] = row[9]
            if row[10]:
                anime_data["AiredTo"] = row[10]
            if row[11]:
                anime_data["Duration"] = row[11]
            if row[12]:
                anime_data["Rating"] = row[12]
            if row[13]:
                anime_data["Score"] = int(row[13])
            if row[14]:
                anime_data["ScoredBy"] = row[14]
            if row[15]:
                anime_data["Rank"] = row[15]
            if row[16]:
                anime_data["Popularity"] = row[16]
            if row[17]:
                anime_data["Members"] = row[17]
            if row[18]:
                anime_data["Favorites"] = row[18]
            if row[19]:
                anime_data["Synopsis"] = row[19]
            if row[20]:
                anime_data["Background"] = row[20]
            if row[21]:
                anime_data["Premiered"] = row[21]
            if row[22]:
                anime_data["Broadcast"] = row[22]
            if studios:
                anime_data["Studios"] = studios
            if genres:
                anime_data["Genres"] = genres
            collection.insert_one(anime_data)
        print("Data loaded into MongoDB successfully.")
    except Exception as e:
        print("Error loading data to MongoDB:", e)
    finally:
        client.close()




def connection_with_database():
    '''
    Establishes a connection to the PostgreSQL database.

    Returns:
    connection: PostgreSQL database connection object
    '''

    connection = psycopg2.connect(
        user="postgres",
        password="123456",
        host="localhost",
        port="5432",
        database="Project"
    )
    return connection




try:
    # Connection to the database
    conn = connection_with_database()

    current_time = datetime.now()

    # Using this to record the amount of time it will take.
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    print("Current System Time:", formatted_time)


    studio_data = fetch_anime_studios_data()
    genres_data = fetch_anime_genres_data()
    favorite_anime_data = fetch_favorite_anime_data()
    favorite_manga_data = fetch_favorite_manga_data()
    favorite_characters_data = fetch_favorite_characters_data()

    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    print("Current System Time:", formatted_time)
    conn.close()

except (Exception, psycopg2.DatabaseError) as error:
    print("Error while connecting to PostgreSQL:", error)


print("reached here")
connection_url = "mongodb://localhost:27017/"
mongo_db_name = "My_Anime_List"

create_collection()
load_data_to_mongodb_users(favorite_anime_data, favorite_manga_data, favorite_characters_data)







