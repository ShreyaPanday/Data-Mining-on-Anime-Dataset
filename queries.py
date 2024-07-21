"""
Author -  Shreya Pandey

This query is used to query on the postgres tables being created.
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

def run_the_queries(conn):
    '''
    Executes predefined SQL queries and prints the results.

    Args:
    conn: PostgreSQL database connection object
    '''
    try:
        cursor = conn.cursor()
        start_time = datetime.now()
        cursor.execute("select a.title, a.popularity, s.studio from anime a join anime_studios s on a.anime_id = s.anime_id order by a.popularity desc limit 10;")
        conn.commit()
        print("\n \n Query 1 executed successfully")
        rows_to_print = cursor.fetchall()
        end_time = datetime.now()
        execution_time= end_time -  start_time
        print(execution_time, " seconds")
        for row in rows_to_print:
            print(row)
        start_time = datetime.now()
        cursor.execute("select ag.genres, a.score, a.title  from anime a join anime_genres ag on a.anime_id = ag.anime_id  where a.score is not null order by score desc;")
        conn.commit()
        print("\n \n Query 2 executed successfully")
        rows_to_print = cursor.fetchall()
        end_time = datetime.now()
        execution_time = end_time - start_time
        print(execution_time, " seconds")
        for row in rows_to_print:
            print(row)
        start_time = datetime.now()
        cursor.execute("select a.title, al.watch_end_date-al.watch_start_date as time_taken  from anime a join anime_list al on al.anime_id = a.anime_id  where al.watch_start_date is not null and al.watch_end_date is not null  order by time_taken desc limit 10;")
        conn.commit()
        print("\n \n Query 3 executed successfully")
        rows_to_print = cursor.fetchall()
        end_time = datetime.now()
        execution_time = end_time - start_time
        print(execution_time, " seconds")
        for row in rows_to_print:
            print(row)
        start_time = datetime.now()
        cursor.execute("select u.user_name, count(al.anime_id) as anime_watch_count from users u join anime_list al on u.user_id = al.user_id group by u.user_id, u.user_name order by anime_watch_count desc limit 10;")
        conn.commit()
        print("\n \n Query 4 executed successfully")
        rows_to_print = cursor.fetchall()
        end_time = datetime.now()
        execution_time = end_time - start_time
        print(execution_time, " seconds")
        for row in rows_to_print:
            print(row)
        start_time = datetime.now()
        cursor.execute("select avg(a.score), ag.genres  from anime a join anime_genres ag on a.anime_id = ag.anime_id where a.score is not null group by ag.genres;")
        conn.commit()
        print("\n \n Query 5 executed successfully")
        rows_to_print = cursor.fetchall()
        end_time = datetime.now()
        execution_time = end_time - start_time
        print(execution_time, " seconds")
        for row in rows_to_print:
            print(row)
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while quering the tables", error)


try:
    # Connection to the database
    conn = connection_with_database()
    run_the_queries(conn)
    conn.close()

except (Exception, psycopg2.DatabaseError) as error:
    print("Error while connecting to PostgreSQL:", error)