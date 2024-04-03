"""Скрипт для заполнения данными таблиц в БД Postgres."""

import csv
import psycopg2

# with open('north_data/customers_data.csv', 'r', encoding='utf-8') as csv_file:
#     csv_reader = csv.DictReader(csv_file)
#     for line in csv_reader:
#         print(line['customer_id'], line['company_name'], line['contact_name'], sep='---')

with open('north_data/customers_data.csv', 'r', encoding='utf-8', ) as csv_file:
	csv_reader = csv.reader(csv_file)
	next(csv_reader)
	customers_list = []
	for line in csv_reader:
		customers_list.append(line)
# print(customers_list)

with open('north_data/employees_data.csv', 'r', encoding='utf-8', ) as csv_file:
	csv_reader = csv.reader(csv_file)
	next(csv_reader)
	employees_list = []
	for line in csv_reader:
		employees_list.append(line)
# print(employees_list)

with open('north_data/orders_data.csv', 'r', encoding='utf-8', ) as csv_file:
	csv_reader = csv.reader(csv_file)
	next(csv_reader)
	orders_list = []
	for line in csv_reader:
		orders_list.append(line)
# print(orders_list)


conn = psycopg2.connect(host="localhost", database="north_2", user="postgres", password="postgres_psw")

try:
	with conn:
		with conn.cursor() as cur:
			cur.executemany("INSERT INTO customer VALUES (%s, %s, %s)", customers_list)
			cur.executemany("INSERT INTO employee VALUES (%s, %s, %s, %s, %s, %s)", employees_list)
			cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders_list)
finally:
	conn.close()
