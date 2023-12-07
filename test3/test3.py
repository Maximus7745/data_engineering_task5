import json
from bson import json_util
from pymongo import MongoClient, collection

def get_data_from_json(file_name):
    with open(file_name, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    return data

def connect()-> collection.Collection:
    client = MongoClient()
    db = client["test-database"]
    return db.person

def add_data_into_json(file_name, data):
    with open("test3/" + file_name + ".json", 'w', encoding='utf-8') as f:
        f.write(json_util.dumps(data, ensure_ascii=False, indent=4))

def insert_data(collection: collection.Collection, data: list[dict])-> None:
    collection.insert_many(data)


def delete_by_selary(collection: collection.Collection)-> None:
    res = collection.delete_many({
        "$or": [
            {"salary": {"$lt": 25000}},
            {"salary": {"$gt": 175000}}
        ]
    })
    print(res)

def increase_age(collection: collection.Collection)-> None:
    res = collection.update_many({},{
        "$inc": {"age": 1}
    })
    print(res)

def increase_salary_for_jobs(collection: collection.Collection)-> None:
    filter = {
        "job": {"$in": ["Программист", "IT-специалист", "Инженер"]}
    }
    update = {
        "$mul": {"salary": 1.05}
    }
    res = collection.update_many(filter, update)
    print(res)

def increase_salary_for_city(collection: collection.Collection)-> None:
    filter = {
        "city": {"$in": ["Варшава", "Малага", "Санкт-Петербург"]}
    }
    update = {
        "$mul": {"salary": 1.07}
    }
    res = collection.update_many(filter, update)
    print(res)

def increase_salary_for_city_job_age(collection: collection.Collection)-> None:
    filter = {
        "city": {"$nin": ["Варшава", "Малага", "Санкт-Петербург"]},
        "job": {"$nin": ["Бухгалтер", "Продавец", "Повар"]},
        "age": {"$gt": 18, "$lt": 25}
    }
    update = {
        "$mul": {"salary": 1.1}
    }
    res = collection.update_many(filter, update)
    print(res)

def delete_by_lucky(collection: collection.Collection)-> None:
    res = collection.delete_many({
        "$or": [
            {"age": {"$lt": 20}},
            {"age": {"$gt": 50}}
        ]
    })
    print(res)


data = get_data_from_json(r"data\var_24\task_3_item.json")
insert_data(connect(), data)

delete_by_selary(connect())

increase_age(connect())

increase_salary_for_jobs(connect())

increase_salary_for_city(connect())

increase_salary_for_city_job_age(connect())

delete_by_lucky(connect())

add_data_into_json("result3", list(connect().find())) 
