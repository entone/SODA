from parser import Parser
from pymongo.connection import Connection

Connection('localhost:27017')['gericare']['locations'].remove()

def mongo_insert(ar):
    conn = Connection('localhost:27017')
    db = conn['gericare']
    collection = db['locations']
    for a in ar:
        if a['city'] == "Chicago":
            collection.insert(a)


pa = Parser(file="sample_data/nursing_homes.json", post=mongo_insert)