from pymongo import MongoClient
from bson import decimal128
import decimal

# connection to mongodb
hostname = 'mongodb'
port = 27017 
client = MongoClient(hostname, port)
print('Mongo_db connection established')

db = client['dvdrental']
#Aufgabe A
inventory = db['inventory']
print(inventory)
availableFilmsCount = inventory.count_documents(filter={})
print(f"Antwort Aufgabe A: {availableFilmsCount}")

#Aufgabe B
stores = db['store']
storeIDs = stores.distinct("store_id")
print("Antwort Aufgabe B:")
for id in storeIDs:
    films = inventory.find({"store_id":id})
    distinctFilm = films.distinct("film_id")
    print(f"Store {id}: "+str(len(distinctFilm)))

#Aufgabe C
print("Antwort Aufgabe C:")
actors = db['actor']
filmActors = db['film_actor']
actorIDs = actors.distinct("actor_id")
numberList = []
for id in actorIDs:
    filmsPerActor = filmActors.find({"actor_id":id})
    numberList.append((id,len(list(filmsPerActor))))

def takeSecond(elem):
    return(elem[1])
numberList.sort(key=takeSecond,reverse=True)
for i in range(10):
    print(f"Actor: {numberList[i][0]}, Films: {numberList[i][1]}")

#Aufgabe D
print("Antwort Aufgabe D:")
staffIDs = db['staff'].distinct("staff_id")
payments = db['payment']
d128context = decimal128.create_decimal128_context()
with decimal.localcontext(d128context):
    for id in staffIDs:
        totalAmount = decimal.Decimal(0.0)
        for payment in list(payments.find({"staff_id":id})):
            totalAmount+=payment["amount"].to_decimal()
        print(f"Staff {id}: {totalAmount}")

#Aufgabe E
print("Antwort Aufgabe E:")
rentals = db['rental']
customers = db['customer']
customerIDs = customers.distinct(key="customer_id")
customerRentalsList = []
for id in customerIDs:
    rentalsPerCustomer = rentals.find({"customer_id":id})
    customerRentalsList.append((id,len(list(rentalsPerCustomer))))
customerRentalsList.sort(key=takeSecond,reverse=True)

for i in range(10):
    print(f"Kunde: {customerRentalsList[i][0]} Anzahl: {customerRentalsList[i][1]}")
    
client.close()