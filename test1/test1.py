import json
from bson import json_util
from pymongo import MongoClient, collection

def connect()-> collection.Collection:
    client = MongoClient()
    db = client["test-database"]
    return db.person

def get_data_from_text(file_name: str)-> list[object]:
    with open(file_name,'r', encoding='utf-8') as f:
        lines = f.readlines()
    items = list()
    item = dict()
    for line in lines:
        if line != "=====\n":
            elem = line.strip().split("::")
            if(elem[0] in ("id", "salary", "age", "year")):
                item[elem[0]] = int(elem[1])
            else:
                item[elem[0]] = elem[1]
        else:
            items.append(item)
            item = dict()
    return items

def add_data_into_json(file_name, data):
    with open("test1/" + file_name + ".json", 'w', encoding='utf-8') as f:
        f.write(json_util.dumps(data, ensure_ascii=False, indent=4))

def insert_data(collection: collection.Collection, data: list[dict])-> None:
    collection.insert_many(data)


def write_sort_by_salary(collection: collection.Collection)-> None:
    result = list(collection.find(limit=10).sort({"salary" : -1}))
    add_data_into_json("test1_sort_by_salary",result)

def write_filter_by_age(collection: collection.Collection)-> None:
    result = list(collection.find({"age": {"$lt": 30}},limit=15).sort({"salary" : -1}))
    add_data_into_json("test1_filter_by_age",result)

def write_filter_by_city_jobs(collection: collection.Collection)-> None:
    result = list(collection.find({"city": "Пласенсия", "job": {"$in": ["Программист", "IT-специалист", "Инженер"]}},limit=10).sort({"age" : 1}))
    add_data_into_json("test1_filter_by_city_jobs",result)

def write_count_filter_by_age_year_salary(collection: collection.Collection)-> None:
    result = collection.count_documents({"age": {"$lt": 35, "$gt": 20}, "year": {"$gt": 2018, "$lt": 2023}, 
                                         "$or": [{"salary": {"$gt": 50000, "$lte": 75000}}, {"salary":{"$gt": 125000, "$lt": 150000}}]})
    add_data_into_json("test1_count_filter_by_age_year_salary",result)


data = get_data_from_text(r"data\var_24\task_1_item.text")
insert_data(connect(), data)
write_sort_by_salary(connect())
write_filter_by_age(connect())
write_filter_by_city_jobs(connect())
write_count_filter_by_age_year_salary(connect())