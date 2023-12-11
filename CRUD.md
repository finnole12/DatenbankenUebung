# READ
## Aufgabe A
### Code:
inventory = db['inventory']
availableFilmsCount = inventory.count_documents(filter={})
print(f"Antwort Aufgabe A: {availableFilmsCount}")
### Ausgabe: 
Antwort Aufgabe A: 4581
## Aufgabe B
### Code: 
stores = db['store']
storeIDs = stores.distinct("store_id")
print("Antwort Aufgabe B:")
for id in storeIDs:
    films = inventory.find({"store_id":id})
    distinctFilm = films.distinct("film_id")
    print(f"Store {id}: "+str(len(distinctFilm)))
### Ausgabe:
Antwort Aufgabe B:
Store 1: 759
Store 2: 762
## Aufgabe C:
### Code:
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
### Ausgabe:
Antwort Aufgabe C:
Actor: 107, Films: 42
Actor: 102, Films: 41
Actor: 198, Films: 40
Actor: 181, Films: 39
Actor: 23, Films: 37
Actor: 81, Films: 36
Actor: 13, Films: 35
Actor: 37, Films: 35
Actor: 60, Films: 35
Actor: 106, Films: 35
## Aufgabe D
### Code:
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
### Ausgabe:
Antwort Aufgabe D:
Staff 1: 30252.12
Staff 2: 31059.92
## Aufgabe E
### Code:
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
### Ausgabe:
Antwort Aufgabe E:
Kunde: 148 Anzahl: 46
Kunde: 526 Anzahl: 45
Kunde: 144 Anzahl: 42
Kunde: 236 Anzahl: 42
Kunde: 75 Anzahl: 41
Kunde: 197 Anzahl: 40
Kunde: 469 Anzahl: 40
Kunde: 137 Anzahl: 39
Kunde: 178 Anzahl: 39
Kunde: 468 Anzahl: 39
## Aufgabe F
### Code:
print("Antwort Aufgabe F:")
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
### Ausgabe:
Antwort Aufgabe F:
[{'address': {'address': '1952 Pune Lane',
              'address2': '',
              'city_id': 442,
              'district': 'Saint-Denis',
              'phone': '354615066969',
              'postal_code': '92150'},
  'first_name': 'Eleanor',
  'last_name': 'Hunt',
  'roundedSum': 211.55},
 {'address': {'address': '1427 Tabuk Place',
              'address2': '',
              'city_id': 101,
              'district': 'Florida',
              'phone': '214756839122',
              'postal_code': '31342'},
  'first_name': 'Karl',
  'last_name': 'Seal',
  'roundedSum': 208.58},
 {'address': {'address': '1891 Rizhao Boulevard',
              'address2': '',
              'city_id': 456,
              'district': 'So Paulo',
              'phone': '391065549876',
              'postal_code': '47288'},
  'first_name': 'Marion',
  'last_name': 'Snyder',
  'roundedSum': 194.61},
 {'address': {'address': '1749 Daxian Place',
              'address2': '',
              'city_id': 29,
              'district': 'Gelderland',
              'phone': '963369996279',
              'postal_code': '11044'},
  'first_name': 'Rhonda',
  'last_name': 'Kennedy',
  'roundedSum': 191.62},
 {'address': {'address': '1027 Songkhla Manor',
              'address2': '',
              'city_id': 340,
              'district': 'Minsk',
              'phone': '563660187896',
              'postal_code': '30861'},
  'first_name': 'Clara',
  'last_name': 'Shaw',
  'roundedSum': 189.6},
 {'address': {'address': '76 Kermanshah Manor',
              'address2': '',
              'city_id': 423,
              'district': 'Esfahan',
              'phone': '762361821578',
              'postal_code': '23343'},
  'first_name': 'Tommy',
  'last_name': 'Collazo',
  'roundedSum': 183.63},
 {'address': {'address': '682 Garden Grove Place',
              'address2': '',
              'city_id': 333,
              'district': 'Tennessee',
              'phone': '72136330362',
              'postal_code': '67497'},
  'first_name': 'Ana',
  'last_name': 'Bradley',
  'roundedSum': 167.67},
 {'address': {'address': '432 Garden Grove Street',
              'address2': '',
              'city_id': 430,
              'district': 'Ontario',
              'phone': '615964523510',
              'postal_code': '65630'},
  'first_name': 'Curtis',
  'last_name': 'Irby',
  'roundedSum': 167.62},
 {'address': {'address': '1479 Rustenburg Boulevard',
              'address2': '',
              'city_id': 527,
              'district': 'Southern Tagalog',
              'phone': '727785483194',
              'postal_code': '18727'},
  'first_name': 'Marcia',
  'last_name': 'Dean',
  'roundedSum': 166.61},
 {'address': {'address': '990 Etawah Loop',
              'address2': '',
              'city_id': 564,
              'district': 'Tamil Nadu',
              'phone': '206169448769',
              'postal_code': '79940'},
  'first_name': 'Mike',
  'last_name': 'Way',
  'roundedSum': 162.67}]
## Aufgabe G
### Code:
print("Antwort Aufgabe G:")
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
### Ausgabe:
Antwort Aufgabe G:
[{'rental_amount': 34, 'title': ['Bucket Brotherhood']},
 {'rental_amount': 33, 'title': ['Rocketeer Mother']},
 {'rental_amount': 32, 'title': ['Scalawag Duck']},
 {'rental_amount': 32, 'title': ['Ridgemont Submarine']},
 {'rental_amount': 32, 'title': ['Forward Temple']},
 {'rental_amount': 32, 'title': ['Grit Clockwork']},
 {'rental_amount': 32, 'title': ['Juggler Hardly']},
 {'rental_amount': 31, 'title': ['Apache Divine']},
 {'rental_amount': 31, 'title': ['Wife Turn']},
 {'rental_amount': 31, 'title': ['Robbers Joon']}]
## Aufgabe H
### Code:
print("Antwort Aufgabe H:")
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
### Ausgabe:
Antwort Aufgabe H:
[{'category': 'Sports', 'rental_amount': 1179},
 {'category': 'Animation', 'rental_amount': 1166},
 {'category': 'Action', 'rental_amount': 1112},
 {'category': 'Sci-Fi', 'rental_amount': 1101},
 {'category': 'Family', 'rental_amount': 1096},
 {'category': 'Drama', 'rental_amount': 1060},
 {'category': 'Documentary', 'rental_amount': 1050},
 {'category': 'Foreign', 'rental_amount': 1033},
 {'category': 'Games', 'rental_amount': 969},
 {'category': 'Children', 'rental_amount': 945}]
## Aufgabe I
### Code:
print("Antwort Aufgabe I:")
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
### Ausgabe:
Antwort Aufgabe I:
[{'address': '1003 Qinhuangdao Street',
  'id': 'customer_id',
  'name': 'Jared Ely',
  'notes': 'active',
  'phone': '35533115997'},
 {'address': '1913 Hanoi Way',
  'id': 'customer_id',
  'name': 'Mary Smith',
  'notes': 'active',
  'phone': '28303384290'},
 {'address': '1121 Loja Avenue',
  'id': 'customer_id',
  'name': 'Patricia Johnson',
  'notes': 'active',
  'phone': '838635286649'},
 {'address': '692 Joliet Street',
  'id': 'customer_id',
  'name': 'Linda Williams',
  'notes': 'active',
  'phone': '448477190408'},
 {'address': '1566 Inegl Manor',
  'id': 'customer_id',
  'name': 'Barbara Jones',
  'notes': 'active',
  'phone': '705814003527'},
  [....]
  (Hier gekürzt auf die ersten 5 Zeilen)
]
# UPDATE
## AUFGABE A:
### CODE: 
staff = db['staff']
staff.update_many({},{"$set":{"password":"newMoreSecurePassword"
                              ,"last_update":datetime.datetime.now()}})
## AUFGABE B:
### CODE:
addresses = db['address']
stores = db['store']
inventory = db['inventory']

#finding a new ID for the new address
addressIDs = addresses.distinct("address_id")
newAddressID = max(addressIDs)+1

#inserting a made up address
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

#inserting a made up store
stores.insert_one({"store_id":newStoreID,
                   "manager_staff_id":1,
                   "address_id":newAddressID,
                   "last_update":datetime.datetime.now()})

#setting all inventorys store ID to the new stores ID
inventory.update_many({},{"$set":{"store_id":newStoreID,
                                  "last_update":datetime.datetime.now()}})
# DELETE
## AUFGABE A+B:
### CODE: 
#the necessary collections
films = db['film']
inventory = db['inventory']
rentals = db['rental']

#deleting movies with length<60 and saving their film_ids
toDelete = films.find({"length":{"$lt":60}})
deletedIDs = toDelete.distinct("film_id")
deleted = films.delete_many({"length":{"$lt":60}})
print(f"Deleted {deleted.deleted_count} films")

#finding all inventory_ids that belong to deleted films
toDeleteIDs=[]
for id in deletedIDs:
    toDeleteIDs.extend(inventory.find({"film_id":id}).distinct("inventory_id"))

#deleting all rentals associated with the collected inventory_ids
totalDeletes = 0
for id in toDeleteIDs:
    deleted = rentals.delete_many({"inventory_id":id})
    totalDeletes += deleted.deleted_count
print(f"Deleted {totalDeletes} rentals")