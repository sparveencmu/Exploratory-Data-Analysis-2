import csv
import json
import xml.etree.ElementTree as ET
import mysql.connector
import pymongo

def retrieve_popular_books(csv_in, json_out):
    dict_book = {}
    with open(csv_in, newline='') as csv_file:
        csv_dict_reader = csv.DictReader(csv_file)

        for row in csv_dict_reader:
            
            if float(row['average_rating']) > 4.5 and int(row['num_pages']) > 50 and int(row['ratings_count']) > 1000:
                bookID = row['bookID']
                dict_info ={}
                dict_info['title'] = row['title']
                dict_info['authors'] = row['authors']
                dict_info['isbn'] = row['isbn']
                dict_info['isbn13'] = row['isbn13']
                dict_info['language_code'] = row['language_code']
                dict_info['num_pages'] = int(row['num_pages'])
                dict_info['publication_date'] = row['publication_date']
                dict_info['publisher'] = row['publisher']
                dict_rating ={}
                dict_rating['average_rating'] = float(row['average_rating'])
                dict_rating['ratings_count'] = int(row['ratings_count'])
                dict_rating['text_reviews_count'] = int(row['text_reviews_count'])
                dict_data ={}
                dict_data['info'] = dict_info
                dict_data['rating'] = dict_rating
                dict_book[bookID] = dict_data



    with open(json_out, 'w') as json_file:
        jsonString = json.dumps(dict_book, indent=4)
        json_file.write(jsonString)        

def retrieve_boring_books(json_in, xml_out):
    #read the json file
    json_data = open(json_in)
    data = json.load(json_data)
    root = ET.Element('books')
    for x in data:
        if float(x['average_rating']) < 3.0 and int(x['num_pages']) > 300 and int(x['ratings_count']) > 100:
                child = ET.SubElement(root, 'book')
                child.attrib['id'] = x['bookID']
                info = ET.SubElement(child, 'info')
                title_node = ET.SubElement(info, 'title')            
                title_node.text = x['title']
                author_node = ET.SubElement(info, 'authors')
                author_node.text = x['authors']
                isbn_node = ET.SubElement(info, 'isbn')
                isbn_node.text = x['isbn']
                isbn13_node = ET.SubElement(info, 'isbn13')
                isbn13_node.text = x['isbn13']
                language_node = ET.SubElement(info, 'language_code')
                language_node.text = x['language_code']
                num_pages_node = ET.SubElement(info, 'num_pages')
                num_pages_node.text = x['num_pages']
                publication_date_node = ET.SubElement(info, 'publication_date')
                publication_date_node.text = x['publication_date']
                publisher_node = ET.SubElement(info, 'publisher')
                publisher_node.text = x['publisher']
                
                child_rating = ET.SubElement(child, 'rating')
                average_rating_node = ET.SubElement(child_rating, 'average_rating')
                average_rating_node.text = x['average_rating']
                ratings_count_node = ET.SubElement(child_rating, 'ratings_count')
                ratings_count_node.text = x['ratings_count']
                text_reviews_count_node = ET.SubElement(child_rating, 'text_reviews_count')
                text_reviews_count_node.text = x['text_reviews_count']
    tree = ET.ElementTree(root)
    tree.write(xml_out)            
                
def retrieve_wildly_popular_books(xml_in, csv_out):
    #read the xml file
    tree = open(xml_in)
    books = ET.parse(tree).getroot()
    csv_row_list = []
    for data in books.findall('book'):
        if float(data.find('average_rating').text) > 4 and int(data.find('ratings_count').text) > 1000000:
            dict_data = {}
            dict_data['book_id'] = data.find('bookID').text
            dict_data['title'] = data.find('title').text
            dict_data['authors'] = data.find('authors').text
            dict_data['average_rating'] = data.find('average_rating').text
            dict_data['isbn'] = data.find('isbn').text
            dict_data['isbn13'] = data.find('isbn13').text
            dict_data['language_code'] = data.find('language_code').text
            dict_data['num_pages'] = data.find('num_pages').text
            dict_data['ratings_count'] = data.find('ratings_count').text
            dict_data['text_reviews_count'] = data.find('text_reviews_count').text
            dict_data['publication_date'] = data.find('publication_date').text
            dict_data['publisher'] = data.find('publisher').text
            csv_row_list.append(dict_data)
          
            
    with open(csv_out, 'w', newline='') as new_csv_file:
            field_names_list = ['book_id', 'title', 'authors', 'average_rating', 'isbn', 'isbn13',
                                'language_code', 'num_pages', 'ratings_count', 'text_reviews_count',
                                'publication_date', 'publisher']
            csv_writer = csv.DictWriter(new_csv_file, 
                                fieldnames = field_names_list, 
                                delimiter=',', 
                                quotechar='"', 
                                quoting=csv.QUOTE_ALL)
            csv_writer.writeheader()
            csv_writer.writerows(csv_row_list)
            
def retrieve_long_books(user, password, host, db, table, csv_out):
    csv_row_list = []
    conn = mysql.connector.connect(
    host = host,
    user = user,
    password = password
    )
    
    select_table_statement = f'''
        SELECT * FROM  {db}.{table} WHERE num_pages > 2000;
    '''
    cur = conn.cursor()
    cur.execute(select_table_statement)
    myresult = cur.fetchall()

    for x in myresult:
        dict_data = {}
        dict_data['book_id'] = x[0]
        dict_data['title'] = x[1]
        dict_data['authors'] = x[2]
        dict_data['average_rating'] = x[3]
        dict_data['isbn'] = x[4]
        dict_data['isbn13'] = x[5]
        dict_data['language_code'] = x[6]
        dict_data['num_pages'] = x[7]
        dict_data['ratings_count'] = x[8]
        dict_data['text_reviews_count'] = x[9]
        dict_data['publication_date'] = x[10]
        dict_data['publisher'] = x[11]
        csv_row_list.append(dict_data)

    cur.close()
    conn.commit()
    with open(csv_out, 'w', newline='') as new_csv_file:
            field_names_list = ['book_id', 'title', 'authors', 'average_rating', 'isbn', 'isbn13',
                                'language_code', 'num_pages', 'ratings_count', 'text_reviews_count',
                                'publication_date', 'publisher']
            csv_writer = csv.DictWriter(new_csv_file, 
                                    fieldnames = field_names_list, 
                                    delimiter=',', 
                                    quotechar='"', 
                                    quoting=csv.QUOTE_MINIMAL)
            csv_writer.writeheader()
            csv_writer.writerows(csv_row_list)

def retrieve_obscure_books(db, coll, json_out):
    dict_book = {}
    client = pymongo.MongoClient()
    database = client[db]
    books_coll = database[coll]
    for row in books_coll.find():
        if int(row['ratings_count']) == 1:
                bookID = row['bookID']
                dict_info ={}
                dict_info['title'] = row['title']
                dict_info['authors'] = row['authors']
                dict_info['isbn'] = row['isbn']
                dict_info['isbn13'] = row['isbn13']
                dict_info['language_code'] = row['language_code']
                dict_info['num_pages'] = int(row['num_pages'])
                dict_info['publication_date'] = row['publication_date']
                dict_info['publisher'] = row['publisher']
                dict_rating ={}
                dict_rating['average_rating'] = float(row['average_rating'])
                dict_rating['ratings_count'] = int(row['ratings_count'])
                dict_rating['text_reviews_count'] = int(row['text_reviews_count'])
                dict_data ={}
                dict_data['info'] = dict_info
                dict_data['rating'] = dict_rating
                dict_book[bookID] = dict_data

    with open(json_out, 'w') as json_file:
        jsonString = json.dumps(dict_book, indent=4)
        json_file.write(jsonString)  
    
