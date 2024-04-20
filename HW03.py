# Загрузите данные который вы получили на предыдущем уроке путем скрейпинга сайта с помощью Buautiful Soup в MongoDB и создайте базу данных и коллекции для их хранения.
# Поэкспериментируйте с различными методами запросов.

# - найдите первый документ в коллекции и распечатайте его в формате JSON.
# - Используйте функцию count_documents(), чтобы получить общее количество документов в коллекции.
# - Отфильтруйте документы по критерию "num_avail", равному "20", и подсчитайте количество совпадающих документов.
# - Используйте проекцию для отображения только полей "name"  для документов с "num_avail" равным "20".
# - Используйте операторы $lt и $gte для подсчета количества документов с "num_avail" меньше 10 и больше или равно 10, соответственно.
# - Используйте оператор $regex для подсчета количества документов, содержащих слово "cloud" в поле "describe", игнорируя регистр.
# - Используйте оператор $in для подсчета количества документов, в которых "num_avail" является либо 20, либо 22, либо 19


from pymongo import MongoClient
import json
import re

client = MongoClient('mongodb://localhost:27017')

database_list = client.list_database_names()
print(database_list)
db_name = 'literature'
if db_name not in database_list:
    db = client['literature']
    table_book = db['book']
    print('создали базу')

    with open('book_2.json', 'r', encoding='UTF-8') as f:
        literature = json.load(f)

    table_book.insert_many(literature)

print('база уже есть')

db = client['literature']
book = db['book']

# - найдите первый документ в коллекции и распечатайте его в формате JSON.

all_el = book.find()
first_el = all_el[0]
print(f'Первый элемент в коллекции: \n {json.dumps(first_el, indent=4, default=str)} \n')

# - Отфильтруйте документы по критерию "num_avail", равному "20", и подсчитайте количество совпадающих документов.
query = {"num_avail": {'$eq': 20}}
num_avail_20 = book.count_documents(query)
print(f'Количество документов: {num_avail_20}')


# - Используйте проекцию для отображения только полей "name"  для документов с "num_avail" равным "20"

query = {"num_avail": {'$eq': 20}}
proection = {"_id": 0, "name" : 1}
names = book.find(query, proection)
print(f'Проекция имен:')
print(*names)

# - Используйте операторы $lt и $gte для подсчета количества документов с "num_avail" меньше равно 10 и больше или равно 10, соответственно.
query_1 = {'num_avail': {'$gt': 10}}
query_2 = {'num_avail': {'$lte': 10}}

count_less_10 = book.count_documents(query_2)
print(f'Количество документов с остатком меньше равно 10: {count_less_10}')
count_greet_10 = book.count_documents(query_1)
print(f'Количество документов с остатком больше 10: {count_greet_10} \n')

# - Используйте оператор $regex для подсчета количества документов, содержащих слово "cloud" в поле "describe", игнорируя регистр.

regex = re.compile('cloud', re.IGNORECASE)
query = {'describe': {"$regex": regex}}
count_cloud = book.count_documents(query)
cloud = book.find(query)
print(f'Количество документов с cloud: {count_cloud}')
print(*cloud)

# - Используйте оператор $in для подсчета количества документов, в которых "num_avail" является либо 20, либо 22, либо 19
query = {'num_avail': {'$in': [20, 22, 19]}}
count_cloud = book.count_documents(query)
print(f'Количество документов с опредленными остатками: {count_cloud}')