from pymongo import MongoClient
from bson import decimal128
import decimal

# connection to mongodb
hostname = 'mongodb'
port = 27017 
client = MongoClient(hostname, port)
print('Mongo_db connection for reads established')

db = client['dvdrental']
#Aufgabe A
print("Antwort Aufgabe A (read.py Zeile 12-16):")
inventory = db['inventory']
availableFilmsCount = inventory.count_documents(filter={})
print(availableFilmsCount)

#Aufgabe B
stores = db['store']
storeIDs = stores.distinct("store_id")
print("Antwort Aufgabe B (read.py Zeile 18-25):")
for id in storeIDs:
    films = inventory.find({"store_id":id})
    distinctFilm = films.distinct("film_id")
    print(f"Store {id}: "+str(len(distinctFilm)))

#Aufgabe C
print("Antwort Aufgabe C (read.py Zeile 27-42):")
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
print("Antwort Aufgabe D (read.py Zeile 44-54):")
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
print("Antwort Aufgabe E (read.py Zeile 56-68):")
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

#Aufgabe F
print("Antwort Aufgabe F (read.py Zeile 70-144):")
outputF = db.payment.aggregate([
    {
        "$group": {
            "_id": "$customer_id",
            "sum": {
                "$sum": {"$toDouble": "$amount" }
            }
        }
    },
    {
        "$sort" : {
            "sum": -1
        }
    },
    {
        "$limit": 10
    },
    {
        "$lookup": {
            "from": "customer",
            "localField": "_id",
            "foreignField": "customer_id",
            "pipeline": [
                {
                    "$lookup": {
                        "from": "address",
                        "localField": "address_id",
                        "foreignField": "address_id",
                        "pipeline": [{
                            "$project": {
                                "_id": 0,
                                "address": 1,
                                "address2": 1,
                                "city_id": 1,
                                "district": 1,
                                "phone": 1,
                                "postal_code": 1
                            }
                        }],
                        "as": "address"
                    }
                },
                {
                    "$unwind": "$address"
                },
                {
                    "$project": {
                        "first_name": 1,
                        "last_name": 1,
                        "address": 1,
                        "customer_id": 1,
                        "_id": 0
                    }
                }
            ],
            "as": "customer"
        }
    },
    {
        "$unwind": "$customer"
    }, 
    {
        "$project": {
            "first_name": "$customer.first_name",
            "last_name": "$customer.last_name",
            "address": "$customer.address",
            "roundedSum": { "$round": ["$sum", 2] },
            "_id": 0
        }
    }
])
import pprint
pprint.pprint(list(outputF))

# Aufgabe G
print("Antwort Aufgabe G (read.py Zeile 146-200):")
outputG = db.rental.aggregate([
    {
        "$lookup": {
            "from": "inventory",
            "localField": "inventory_id",
            "foreignField": "inventory_id",
            "as": "inventory"
        }
    },
    {
        "$unwind": "$inventory"
    },
    {
        "$group": {
            "_id": "$inventory.film_id",
            "rental_amount": {
                "$count": { }
            }
        }
    },
    {
        "$sort": {
            "rental_amount": -1
        }
    },
    {
        "$limit": 10
    },
    {
        "$lookup": {
            "from": "film",
            "localField": "_id",
            "foreignField": "film_id",
            "pipeline": [
                {
                    "$project": {
                        "_id": 0,
                        "title": 1
                    }
                }
            ],
            "as": "film"
        }
    },
    {
        "$project": {
            "_id": 0,
            "title": "$film.title",
            "rental_amount": 1
        }
    }
])
pprint.pprint(list(outputG))

#Aufgabe H
print("Antwort Aufgabe H (read.py Zeile 202-289):")
outputH = db.rental.aggregate([
    {
        "$lookup": {
            "from": "inventory",
            "localField": "inventory_id",
            "foreignField": "inventory_id",
            "as": "inventory"
        }
    },
    {
        "$unwind": "$inventory"
    },
    {
        "$project": {
            "_id": 0,
            "film_id": "$inventory.film_id"
        }
    },
    {
        "$lookup": {
            "from": "film_category",
            "localField": "film_id",
            "foreignField": "film_id",
            "pipeline": [
                {
                    "$project": {
                        "_id": 0,
                        "category_id": 1
                    }
                }
            ],
            "as": "film"
        }
    },
    {
        "$unwind": "$film"
    },
    {
        "$project": {
            "category_id": "$film.category_id",
        }
    },
    {
        "$group": {
            "_id": "$category_id",
            "rental_amount": {
                "$count": { }
            }
        }
    },
    {
        "$lookup": {
            "from": "category",
            "localField": "_id",
            "foreignField": "category_id",
            "pipeline": [
                {
                    "$project": {
                        "_id": 0,
                        "name": 1
                    }
                }
            ],
            "as": "category"
        }
    },
    {
        "$unwind": "$category"
    },
    {
        "$sort": {
            "rental_amount": -1
        }
    },
    {
        "$limit": 10
    },
    {
        "$project": {
            "_id": 0,
            "category": "$category.name",
            "rental_amount": 1
        }
    }
])
pprint.pprint(list(outputH))

#Aufgabe I
print("Antwort Aufgabe I (read.py Zeile 291-324):")
outputI = db.customer.aggregate([
    {
        "$lookup": {
            "from": "address",
            "localField": "address_id",
            "foreignField": "address_id",
            "as": "address"
        }
    },
    {
        "$unwind": "$address"
    },
    {
        "$project": {
            "_id": 0,
            "id": "customer_id",
            "name": { "$concat": [ "$first_name", " ", "$last_name" ]},
            "address": "$address.address",
            "zipcode": "$address.zipcode",
            "phone": "$address.phone",
            "city": "$address.city",
            "country": "$address.country",
            "notes": {
                "$cond": {
                    "if": {  "$eq": ["$active", 1] }, "then": "active", "else": ""
                }
            }
        }
    }
])
import pprint
pprint.pprint(list(outputI))

print("Reads done. Closing connection.")
client.close()