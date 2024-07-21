"""
Author - Shreya Pandey

This script is used to create the lattices for performing itemset mining.
"""

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

def create_the_query(level):
    a = ""
    for i in range(1, level+2):
        a = a + f'''TempTable{i}.genre1 AS genre{i}, \n'''


    b = ""
    for i in range(1,level+2):
        b = b + f'''TempTable{i}.genre1, \n'''
    e = b[:len(b)-3]


    c  =  ""
    for i in range(1, level+2):
        c  =  c +  f'''INNER JOIN user_genres AS user_genres{i} ON TempTable{i}.genre1 = user_genres{i}.genres AND user_genres1.user_id = user_genres{i}.user_id \n'''


    d  = ""
    for i in range(2 , level+2):
        d  =  d + f'''INNER JOIN(SELECT DISTINCT genre1 FROM S{level}) AS TempTable{i} ON TempTable{i-1}.genre1 < TempTable{i}.genre1 \n'''


    query = f'''
          CREATE TABLE S{level + 1} AS
          SELECT
              {a}
              COUNT(*) AS count
          FROM
              (SELECT DISTINCT genre1 FROM S{level}) AS TempTable1
          {d}
          {c}
          GROUP BY {e}
          HAVING
              COUNT(*) >= 50000;
    '''
    return query



def create_the_lattice(conn):
    level = 0
    '''
    Executes predefined SQL queries and prints the results.

    Args:
    conn: PostgreSQL database connection object
    '''
    try:
        cursor = conn.cursor()
        while(True):
            if level == 0:
                cursor.execute(
                    """create table S1 as SELECT genre1, user_count
                        FROM (
                            SELECT genres AS genre1, COUNT(DISTINCT user_id) AS user_count
                            FROM user_genres
                            GROUP BY genres
                        ) AS genre_counts
                        WHERE user_count >= 50000;""")
                conn.commit()
                print("\n \n Level 1 lattice created successfully")
                level = level+1
            else:
                query = create_the_query(level)
                cursor.execute("CREATE TABLE L2 AS SELECT TempTable1.genres AS genre1,TempTable2.genres AS genre2, COUNT(*) AS count FROM (SELECT DISTINCT genres FROM l1) AS TempTable1 INNER JOIN(SELECT DISTINCT genres FROM l1) AS TempTable2 ON TempTable1.genres < TempTable2.genres INNER JOIN user_genres AS usergenres1 ON TempTable1.genres = usergenres1.genres INNER JOIN user_genres AS usergenres2 ON TempTable2.genres = usergenres2.genres AND usergenres1.user_id = usergenres2.user_id GROUP BY TempTable1.genres, TempTable2.genres HAVING COUNT(*) >= 50000;")
                conn.commit()
                print("\n \n Level", level+1, "lattice created successfully")
                level = level + 1

            statement  = f'''SELECT * FROM L{level};'''
            cursor.execute(statement)
            rows = cursor.fetchall()
            rowsall = list(rows)
            print("\n The number of frequent itemsets in this level are", len(rowsall))
            if len(rowsall) == 0:
                break


    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while creating the tables", error)


try:

    conn = connection_with_database()
    create_the_lattice(conn)
    conn.close()

except (Exception, psycopg2.DatabaseError) as error:
    print("Error while connecting to PostgreSQL:", error)


