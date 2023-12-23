from pymongo import MongoClient

# connection to mongodb
hostname = 'mongodb'
port = 27017 
client = MongoClient(hostname, port)
print('Mongo_db connection for deletes established')

db = client['dvdrental']

print("Delete Aufgabe A+B (delete.py Zeile 11-33):")
# the necessary collections
films = db['film']
inventory = db['inventory']
rentals = db['rental']

# deleting movies with length<60 and saving their film_ids
toDelete = films.find({"length":{"$lt":60}})
deletedIDs = toDelete.distinct("film_id")
deleted = films.delete_many({"length":{"$lt":60}})
print(f"Deleted {deleted.deleted_count} films")

# finding all inventory_ids that belong to deleted films
toDeleteIDs=[]
for id in deletedIDs:
    toDeleteIDs.extend(inventory.find({"film_id":id}).distinct("inventory_id"))

# deleting all rentals associated with the collected inventory_ids
totalDeletes = 0
for id in toDeleteIDs:
    deleted = rentals.delete_many({"inventory_id":id})
    totalDeletes += deleted.deleted_count
print(f"Deleted {totalDeletes} rentals")

print("Deletes done. Closing connection")
client.close()