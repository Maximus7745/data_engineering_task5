import pickle
import json
from bson import json_util
from pymongo import MongoClient, collection

def get_data_from_pickle(file_name):
    with open(file_name, "rb") as f:
        data = pickle.load(f)
    return data

def connect()-> collection.Collection:
    client = MongoClient()
    db = client["test-database"]
    return db.person

def add_data_into_json(file_name, data):
    with open("test2/" + file_name + ".json", 'w', encoding='utf-8') as f:
        f.write(json_util.dumps(data, ensure_ascii=False, indent=4))

def insert_data(collection: collection.Collection, data: list[dict])-> None:
    collection.insert_many(data)


def get_selary_stat(collection: collection.Collection)-> list[dict]:
    query =[
        {
            "$group": {
                "_id": "result",
                "max": {"$max": "$salary"},
                "min": {"$min": "$salary"},
                "avg": {"$avg": "$salary"}
            }
        }
    ]
    return collection.aggregate(query)

def get_count_jobs(collection: collection.Collection)-> list[dict]:
    query =[
        {
            "$group": {
                "_id": "$job",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {
                "count": -1
            }
        }
    ]
    return collection.aggregate(query)

def get_param1_stat_group_by_param2(collection: collection.Collection, param1, param2)-> list[dict]:
    query =[
        {
            "$group": {
                "_id": f"${param2}",
                "min": {"$min": f"${param1}"},
                "max": {"$min": f"${param1}"},
                "avg": {"$avg": f"${param1}"}
            }
        }
    ]
    return collection.aggregate(query)

def get_max_salary_min_age(collection: collection.Collection)-> list[dict]:
    query =[
        {
            "$group": {
                "_id": "$age",
                "max_salary": {"$max": "$salary"}
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        },
        {
            "$limit": 1
        }
    ]
    return collection.aggregate(query)

def get_min_salary_max_age(collection: collection.Collection)-> list[dict]:
    query =[
        {
            "$group": {
                "_id": "$age",
                "min_salary": {"$min": "$salary"}
            }
        },
        {
            "$sort": {
                "_id": -1
            }
        },
        {
            "$limit": 1
        }
    ]
    return collection.aggregate(query)

def get_age_stat_group_by_city_filter_salary(collection: collection.Collection)-> list[dict]:
    query =[
        {
            "$match": {
                "salary": {"$gt": 50000}
            }
        },
        {
            "$group": {
                "_id": "$city",
                "min": {"$min": "$age"},
                "max": {"$max": "$age"},
                "avg": {"$avg": "$age"}
            }
        },
        {
            "$sort": {"avg": -1}
        }
    ]
    return collection.aggregate(query)

def get_salary_stat_group_by_city_job_age_filter_age(collection: collection.Collection)-> list[dict]:
    query =[
        {
            "$match": {
                "job": {"$in": ["Программист", "IT-специалист", "Инженер"]},
                "city": {"$in": ["Варшава", "Малага", "Санкт-Петербург"]},
                "$or": [{"age": {"$gt": 18, "$lt": 25}}, {"age":{"$gt": 50, "$lt": 65}}]
            }
        },
        {
            "$group": {
                "_id": "result",
                "min": {"$min": "$salary"},
                "max": {"$max": "$salary"},
                "avg": {"$avg": "$salary"}
            }
        }
    ]
    return collection.aggregate(query)

def random_query(collection: collection.Collection)-> list[dict]:
    #Найти среднюю salary для "Программист", "IT-специалист", "Инженер" для каждого возраста в возрасте от 20 до 30 лет и сортировать по возрастанию
    query =[
        {
            "$match": {
                "job": {"$in": ["Программист", "IT-специалист", "Инженер"]},
                "age": {"$gt": 20, "$lt": 30}
            }
        },
        {
            "$group": {
                "_id": "$age",
                "avg": {"$avg": "$salary"}
            }
        },
        {
            "$sort": {"avg": -1}
        }
    ]
    return collection.aggregate(query)
data = get_data_from_pickle(r"data\var_24\task_2_item.pkl")
insert_data(connect(), data)
result = dict()

result["selary_stat"] = get_selary_stat(connect())
result["count_jobs"] = get_count_jobs(connect())
result["salary_stat_group_by_city"] = get_param1_stat_group_by_param2(connect(), "salary", "city")
result["salary_stat_group_by_job"] = get_param1_stat_group_by_param2(connect(), "salary", "job")
result["age_stat_group_by_city"] = get_param1_stat_group_by_param2(connect(), "age", "city")
result["age_stat_group_by_job"] = get_param1_stat_group_by_param2(connect(), "age", "job")
result["max_salary_min_age"] = get_max_salary_min_age(connect())
result["min_salary_max_age"] = get_min_salary_max_age(connect())
result["age_stat_group_by_city_filter_salary"] = get_age_stat_group_by_city_filter_salary(connect())
result["salary_stat_group_by_city_job_age_filter_age"] = get_salary_stat_group_by_city_job_age_filter_age(connect())
result["random_query"] = random_query(connect())
add_data_into_json("result2", result) # это всё вместе
for key in result:  # здесь для каждого запроса индивидуальный файл с результатом
    add_data_into_json(key, result[key])