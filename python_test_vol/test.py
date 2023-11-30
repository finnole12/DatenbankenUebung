# test print
print("test output visible in console")

# db read test
import psycopg2
try:
    conn = psycopg2.connect(
        host="postgres_db",
        database="dvdrental",
        user="postgres",
        password="1234",
        port="5432"
    )
    cur = conn.cursor()


    cur.execute('select version()')
    print(cur.fetchone())

    print('Test command: select * from actor limit 1')
    cur.execute('select * from actor limit 1')
    db_version = cur.fetchone()
    print(db_version)
    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
        print('Database connection closed')

# mongodb insert test
from pymongo import MongoClient

client = MongoClient()

hostname = 'mongodb'
port = 27017  # Default MongoDB port

# Create a MongoClient instance
client = MongoClient(hostname, port)

db = client['local']
collection = db['startup_log']
print('mongodb output test:')
print(collection.find_one())
client.close()