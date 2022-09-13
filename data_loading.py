import mysql.connector
import csv
import json
import pymongo


def setup_sql_table(user, password, host, new_db, new_table):
    #setup_sql_table('dbuser', 'dbroot', 'localhost', 'books', 'goodreads')
    conn = mysql.connector.connect(
    host = host,
    user = user,
    password = password
    )
    
    del_db_statement = f'DROP DATABASE IF EXISTS {new_db};'
    create_db_statement = f'CREATE DATABASE {new_db};'
    create_table_statement = f'''
        CREATE TABLE {new_db}.{new_table} (
            id VARCHAR(255) PRIMARY KEY,
            title VARCHAR(1024),
            authors VARCHAR(1024),
            average_rating FLOAT(9,2),
            isbn VARCHAR(255),
            isbn13 VARCHAR(255),
            language_code VARCHAR(255),
            num_pages INT,
            ratings_count INT,
            text_reviews_count INT,
            publication_date VARCHAR(255),
            publisher VARCHAR(255)

        );
    '''
    cur = conn.cursor()
    cur.execute(del_db_statement)
    cur.execute(create_db_statement)
    cur.execute(create_table_statement)
    cur.close()
    conn.commit()

def load_to_sql(user, password, host, db, table, csv_in):
    conn = mysql.connector.connect(
    host = host,
    user = user,
    password = password
    )
    cur = conn.cursor()
    insert_statement = f'''
        INSERT INTO {db}.{table}
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''
    with open(csv_in, newline='') as csv_file:
        csv_dict_reader = csv.DictReader(csv_file)
        for row in csv_dict_reader:
            cur.execute(insert_statement, (row['bookID'],row['title'], row['authors'], row['average_rating'],row['isbn'], row['isbn13'], row['language_code'], row['num_pages'],
            row['ratings_count'], int(row['text_reviews_count']), row['publication_date'], row['publisher']))
        
    cur.close()
    conn.commit()

def load_to_mongodb(db, coll, json_in):
    client = pymongo.MongoClient()
    client.drop_database(db)
    database = client[db]
    json_data = open(json_in)
    data = json.load(json_data)
    for i in data:
        average_rating = i['average_rating']
        i['average_rating'] = float(average_rating)
        ratings_count = i['ratings_count']
        i['ratings_count'] = int(ratings_count)
        num_pages = i['num_pages']
        i['num_pages'] = int(num_pages)
        text_reviews_count = i['text_reviews_count']
        i['text_reviews_count'] = int(text_reviews_count)
    database[coll].insert_many(data)
