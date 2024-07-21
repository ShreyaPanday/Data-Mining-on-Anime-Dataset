"""
Author - Shreya Pandey

This program is used to create and insert indexes on the columns.
"""

'''
Instructions to run the program : It’s a python program. To run the program, we need to directly run the program in any python IDE such as PyCharm or VSCode. 
The code makes use of the psycopg2 library and hence to run the program, we need to also install the psycopg2 library. 

You can do that using the “pip install psycopg2” command. 

'''

import psycopg2


def create_indexes(cursor, db_connection):
    cursor.execute("CREATE INDEX anime_popularity_index ON anime (popularity)")
    cursor.execute("CREATE INDEX anime_score_index ON anime (score)")
    cursor.execute("CREATE INDEX watch_start_date_index ON anime_list (watch_start_date)")
    cursor.execute("CREATE INDEX watch_end_date_index ON anime_list (watch_end_date)")
    cursor.execute("CREATE INDEX anime_genres_index ON anime_genres (genres)")

    # Commit changes to the database
    db_connection.commit()

    print("Indexes created")



def main():
    try:
        # creating database connection
        db_connection = psycopg2.connect(
            user="postgres",
            password="123456",
            host="localhost",
            port="5432",
            database="Project"
        )

        # create cursor object
        cursor = db_connection.cursor()

        # add foreign key constraints
        create_indexes(cursor, db_connection)

        # Close the cursor and connection
        cursor.close()

        # closing the database connection
        db_connection.close()

    # handles any exceptions that occur when connecting to the database
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL:", error)
        cursor.close()
        db_connection.close()


if __name__ == '__main__':
    main()