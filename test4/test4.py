import json
import csv
from bson import json_util
from pymongo import MongoClient, collection

#Предметная область: книжный магазин

def get_data_from_csv(file_name):
    data = list()
    with open(file_name, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=",")
        next(reader, None)
        for row in reader:
            item = {}
            if(len(row) > 0):
                item["title"] = row[0]
                item["category"] = row[1].lower()
                item["stars"] = parse_star(row[2])
                item["price"] = float(row[3])
                item["availability"] = int(row[5])
                data.append(item)
    
    return data


def parse_star(star_rating: str):
    match star_rating:
        case "One":
            return 1
        case "Two":
            return 2
        case "Three":
            return 3
        case "Four":
            return 4
        case "Five":
            return 5
        case _:
            return 0

def get_data_from_json(file_name):
    with open(file_name, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    
    return data


def connect()-> collection.Collection:
    client = MongoClient()
    db = client["test-database"]
    return db.books

def insert_data(collection: collection.Collection, data: list[dict])-> None:
    collection.insert_many(data)

def add_data_into_json(file_name, data):
    with open("test4/" + file_name + ".json", 'w', encoding='utf-8') as f:
        f.write(json_util.dumps(data, ensure_ascii=False, indent=4))

#1
def sort_by_price_filter_by_stars(collection: collection.Collection)-> list[dict]:
    #Получить 20 книг с рейтингом меньше 2 и отсортированных по цене
    return list(collection.find({"stars" : {"$lt": 2}},limit=20).sort({"price" : -1}))
    
def filter_by_stars_category(collection: collection.Collection)-> list[dict]:
    #Получить книги с рейингом 5, которых не относятся к определённым жанрам
    return list(collection.find({"stars" : 5, "category": {"$nin": ["romance", "fiction", "history"]}}))

def filter_by_title_availability(collection: collection.Collection)-> list[dict]:
    #Получить книги, число которых больше 18 и вычеркнуть из этого списка некторые книги по названию, а также отсортировать по цене
    return list(collection.find({"availability" : {"$gt": 18}, "title": {"$nin": ["The Black Maria", "The Requiem Red"]}}).sort({"price" : -1}))

def filter_by_url_availability_sort_by_stars(collection: collection.Collection)-> list[dict]:
    #Получить книги количество которых меньше 2, где есть ссылка и сортировать по рейтингу
    return list(collection.find({"availability" : {"$lt": 2},"url": {"$exists": True}}).sort({"stars" : -1}))

def get_count_filter_by_category_price(collection: collection.Collection)-> int:
    #Получить количество книг с ценой меньше 25 или больше 50 в некоторых жанрах
    return collection.count_documents({"category": {"$in": ["romance", "fiction", "history"]}, 
                                         "$or": [{"price": {"$lte": 25}}, {"price":{"$gt": 50}}]})

#2

def get_price_stat(collection: collection.Collection)-> list[dict]:
    #Получить статистику по ценам
    query =[
        {
            "$group": {
                "_id": "result",
                "max": {"$max": "$price"},
                "min": {"$min": "$price"},
                "avg": {"$avg": "$price"}
            }
        }
    ]
    return list(collection.aggregate(query))

def get_count_category(collection: collection.Collection)-> list[dict]:
    #Получить все жанры книг
    query =[
        {
            "$group": {
                "_id": "$category",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {
                "count": -1
            }
        }
    ]
    return list(collection.aggregate(query))


def get_max_price_min_price(collection: collection.Collection)-> list[dict]:
    #Для минимального рейтинга получить максимальную цену
    query =[
        {
            "$group": {
                "_id": "$stars",
                "max_price": {"$max": "$price"}
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
    return list(collection.aggregate(query))


def get_stars_stat_group_by_category_filter_price_availability(collection: collection.Collection)-> list[dict]:
    #Получить статистику рейтинга для определённых жанров, цен и количества
    query =[
        {
            "$match": {
                "$or": [{"availability": {"$gt": 0, "$lt": 3}}, {"availability":{"$gt": 10, "$lt": 30}}],
                "category": {"$in": ["romance", "fiction", "history"]},
                "$or": [{"price": {"$gt": 1, "$lt": 10}}, {"price":{"$gt": 50, "$lt": 100}}]
            }
        },
        {
            "$group": {
                "_id": "result",
                "min": {"$min": "$stars"},
                "max": {"$max": "$stars"},
                "avg": {"$avg": "$stars"}
            }
        }
    ]
    return list(collection.aggregate(query))

def get_max_price_group_by_category_stars(collection: collection.Collection)-> list[dict]:
    #Получить самую большую цену книги в жанре business и poetry с рейтингом равным 1
    query =[
        {
            "$match": {
                "category": {"$in": ["business", "poetry"]},
                "stars": 1
            }
        },
        {
            "$group": {
                "_id": "$result",
                "max": {"$max": "$price"}
            }
        }
    ]
    return list(collection.aggregate(query))

#3

def delete_by_price(collection: collection.Collection)-> None:
    #Удалить книги дешевле 10
    res = collection.delete_many({
        "price": {"$lt": 15}
    })
    print(res)

def increase_price_by_availability(collection: collection.Collection)-> None:
    #Увеличить цену в два раз книг в единичном экземпляре
    filter = {
        "availability": 1
    }
    update = {
        "$mul": {"price": 2}
    }
    res = collection.update_many(filter, update)
    print(res)

def reduce_price_by_category_stars(collection: collection.Collection)-> None:
    #Уменьшить цену книг с рейтингом 1 в некоторых категориях, если их количество больше 3
    filter = {
        "stars": 1,
        "category": {"$in": ["business", "poetry"]},
        "availability": {"$gt": 3}
    }
    update = {
        "$mul": {"price": 0.95}
    }
    res = collection.update_many(filter, update)
    print(res)

def increase_availability(collection: collection.Collection)-> None:
    #Увелчить число каждой книги на единицу
    res = collection.update_many({},{
        "$inc": {"availability": 1}
    })
    print(res)

def delete_by_category_stars(collection: collection.Collection)-> None:
    #Удалить книги в определённых категориях или с рейтингом равным 1
    res = collection.delete_many({
        "$or": [{"category": {"$in": ["business", "poetry"]}}, {"stars": 1}]
        
    })
    print(res)



data = get_data_from_json(r"test4\data\Books.json")
data += get_data_from_csv(r"test4\data\books_scraped.csv")
insert_data(connect(), data)


#1-2
result = dict()
result["sort_by_price_filter_by_stars"] = sort_by_price_filter_by_stars(connect())
result["filter_by_stars_category"] = filter_by_stars_category(connect())
result["filter_by_title_availability"] = filter_by_title_availability(connect())
result["filter_by_url_availability_sort_by_stars"] = filter_by_url_availability_sort_by_stars(connect())
result["count_filter_by_category_price"] = get_count_filter_by_category_price(connect())
result["price_stat"] = get_price_stat(connect())
result["count_category"] = get_count_category(connect())
result["max_price_min_price"] = get_max_price_min_price(connect())
result["stars_stat_group_by_category_filter_price_availability"] = get_stars_stat_group_by_category_filter_price_availability(connect())
result["max_price_group_by_category_stars"] = get_max_price_group_by_category_stars(connect())
add_data_into_json("result4", result) # это всё вместе
for key in result:  # здесь для каждого запроса индивидуальный файл с результатом
    add_data_into_json(key, result[key])

#3
delete_by_price(connect())

increase_price_by_availability(connect())

reduce_price_by_category_stars(connect())

increase_availability(connect())

delete_by_category_stars(connect())