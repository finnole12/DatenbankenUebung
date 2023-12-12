import psycopg2
from pymongo import MongoClient
from decimal import Decimal
from bson.decimal128 import Decimal128
import datetime


try:
    # postgres connection 
    conn = psycopg2.connect(
        host="postgres_db",
        database="dvdrental",
        user="postgres",
        password="1234",
        port="5432"
    )
    cur = conn.cursor()
    print('Postgres_db connection established')

    # connection to mongodb
    hostname = 'mongodb'
    port = 27017 
    client = MongoClient(hostname, port)
    print('Mongo_db connection established')

    # create database at mongodb (will be created on first document)
    client.drop_database('dvdrental')
    db = client['dvdrental']

    # for each table
    cur.execute("""SELECT table_name, table_type FROM information_schema.tables
       WHERE table_schema = 'public'""")

    for table in cur.fetchall():
        if table[1] != 'BASE TABLE':
            continue;
        tableName = table[0]

        # read all entries
        cur.execute(f'SELECT * from {tableName}')
        tableContent = cur.fetchall()
        columnNames = [desc[0] for desc in cur.description]

        # iterate over rows
        mongoFormatObjectList = []
        for row in tableContent:

            # build mongoformatobject
            mongoFormatObject = {}
            for index, column in enumerate(columnNames):

                # convert to mongodb format
                val = row[index]
                if isinstance(val, Decimal) : mongoFormat = Decimal128(str(val))
                elif isinstance(val, datetime.date) : mongoFormat = datetime.datetime.combine(val, datetime.time.min)
                elif isinstance(val, memoryview) : mongoFormat = val.tobytes()
                else : mongoFormat = val
                    
                insertObj = { column: mongoFormat }
                mongoFormatObject.update(insertObj)

            # add to list
            mongoFormatObjectList.append(mongoFormatObject)

        # create mongodb collection
        collection = db[tableName]

        # insert all at mongodb
        collection.insert_many(mongoFormatObjectList)

    print('Transfer finished')

    # close connections
    cur.close()
    client.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
        print('Database connection closed')