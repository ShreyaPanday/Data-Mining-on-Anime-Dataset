"""
Author - Shreya Pandey

This program is used to clean the dirty data present in the dataset.
"""

'''
Instructions to run the program : It’s a python program. To run the program, we need to directly run the program in any python IDE such as PyCharm or VSCode. 
The code makes use of the psycopg2 library and hence to run the program, we need to also install the psycopg2 library. 

You can do that using the “pip install psycopg2” command. 

'''
from datetime import datetime
import psycopg2

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

def missing_data(conn):
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE anime SET title_english = COALESCE(title_english, 'No English Title'),  title_japanese = COALESCE(title_japanese, 'No Japanese Title') WHERE title_english IS NULL OR title_japanese IS NULL;")
    conn.commit()
    cursor.execute("UPDATE anime  SET aired_to = CURRENT_DATE WHERE aired_to IS NULL;")
    conn.commit()
    cursor.execute("UPDATE anime  SET aired_from = DATE '9999-12-31'WHERE aired_from IS NULL;")
    conn.commit()
    cursor.execute("UPDATE anime SET episodes = ROUND( (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY episodes) AS median_episodes FROM anime WHERE episodes IS NOT NULL)) WHERE episodes IS NULL;")
    conn.commit()
    cursor.execute("UPDATE anime SET score = ROUND((SELECT AVG(score) FROM anime WHERE score IS NOT NULL)) WHERE score IS NULL;")
    conn.commit()
    cursor.execute("UPDATE anime SET scored_by = (SELECT scored_by FROM ( SELECT scored_by, COUNT(*) AS frequency FROM anime WHERE scored_by IS NOT NULL GROUP BY scored_by ORDER BY frequency DESC LIMIT 1) AS mode_query) WHERE scored_by IS NULL;")
    conn.commit()
    cursor.execute("UPDATE anime SET rank = COALESCE(rank, -1) WHERE rank IS NULL;")
    conn.commit()
    cursor.execute("UPDATE anime SET background = 'No Background available' WHERE background IS NULL;")
    conn.commit()
    cursor.execute("UPDATE anime SET premiered = CAST(EXTRACT(YEAR FROM aired_from) AS VARCHAR) WHERE premiered IS NULL;")
    conn.commit()
    cursor.execute("UPDATE anime SET broadcast = 'Mondays at 12:00 (JST)' WHERE broadcast IS NULL;")
    conn.commit()

def invalid_data(conn):
    cursor = conn.cursor()
    cursor.execute("UPDATE anime SET title_japanese = 'No Japanese title available' WHERE title_japanese ~ '[a-zA-Z]';")
    conn.commit()
    cursor.execute("UPDATE users SET gender = ( SELECT gender FROM ( SELECT gender, COUNT(*) AS frequency FROM users WHERE gender != 'Missing' GROUP BY gender ORDER BY frequency DESC LIMIT 1) AS mode_query)WHERE gender = 'Missing';")
    conn.commit()
    cursor.execute("UPDATE anime_list SET watch_start_date = watch_end_date, watch_end_date = watch_start_date WHERE watch_start_date > watch_end_date;")
    conn.commit()

def non_uniform_data(conn):
    cursor = conn.cursor()
    cursor.execute("UPDATE anime SET duration = REGEXP_REPLACE(duration, ' per ep$', '') WHERE duration ~ ' per ep$';")
    conn.commit()

def timeliness_issues_data(conn):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM anime WHERE EXTRACT(YEAR FROM aired_from) > 2021;")
    conn.commit()

def outlier_data(conn):
    cursor = conn.cursor()
    cursor.execute("delete from anime_list where watching_status = 33;")
    conn.commit()


def data_cleaning(conn):
    '''
    Executes predefined SQL queries  to clean the data.

    Args:
    conn: PostgreSQL database connection object
    '''
    try:
        cursor = conn.cursor()
        missing_data(conn)
        print("\n \n MISSING DATA cleaned successfully")
        invalid_data(conn)
        print("\n \n INVALID DATA cleaned successfully")
        non_uniform_data(conn)
        print("\n \n NON-UNIFORM DATA cleaned successfully")
        timeliness_issues_data(conn)
        print("\n \n TIMELINESS ISSUES DATA cleaned successfully")
        outlier_data(conn)
        print("\n \n OUTLIER DATA cleaned successfully")
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while quering the tables", error)


try:
    # Connection to the database
    conn = connection_with_database()
    data_cleaning(conn)
    conn.close()

except (Exception, psycopg2.DatabaseError) as error:
    print("Error while connecting to PostgreSQL:", error)