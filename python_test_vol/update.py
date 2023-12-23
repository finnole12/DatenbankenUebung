from pymongo import MongoClient
import datetime

# connection to mongodb
hostname = 'mongodb'
port = 27017 
client = MongoClient(hostname, port)
print('Mongo_db connection for updates established')

db = client['dvdrental']

# Aufgabe A
# updating every staff members password
staff = db['staff']
staff.update_many({},{"$set":{"password":"newMoreSecurePassword"
                              ,"last_update":datetime.datetime.now()}})
print("Update Aufgabe A (update.py Zeile 12-17): Staff updated")

# Aufgabe B
# the necessary collections
addresses = db['address']
stores = db['store']
inventory = db['inventory']

# finding a new ID for the new address
addressIDs = addresses.distinct("address_id")
newAddressID = max(addressIDs)+1

# inserting a made up address
addresses.insert_one({"address_id":newAddressID,
                      "address":"123 Rodeo Drive",
                      "address2": None,
                      "district":"Orange County",
                      "city_id":306,
                      "postal_code":"",
                      "phone":"",
                      "last_update":datetime.datetime.now()}
                      )

#finding a new ID for the new store
storeIDs = stores.distinct("store_id")
newStoreID = max(storeIDs)+1

# inserting a made up store
stores.insert_one({"store_id":newStoreID,
                   "manager_staff_id":1,
                   "address_id":newAddressID,
                   "last_update":datetime.datetime.now()})

# setting all inventorys store ID to the new stores ID
inventory.update_many({},{"$set":{"store_id":newStoreID,
                                  "last_update":datetime.datetime.now()}})
print("Update Aufgabe B (update.py Zeile 19-53): Iventory, Stores, Addresses updated")

print("Updates done. Closing connection")
client.close()