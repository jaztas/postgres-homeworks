"""Скрипт для заполнения данными таблиц в БД Postgres."""

import csv
import psycopg2


# with open('north_data/customers_data.csv', 'r', encoding='utf-8') as csv_file:
#     csv_reader = csv.DictReader(csv_file)
#     for line in csv_reader:
#         print(line['customer_id'], line['company_name'], line['contact_name'], sep='---')

# with open('north_data/customers_data.csv', 'r', encoding='utf-8', ) as csv_file:
#     csv_reader = csv.reader(csv_file)
#     next(csv_reader)
#     for line in csv_reader:
#         print(line)

def read_csv(csv_file):
    ''' функция для чтения csv - файлов '''

    with open(csv_file, encoding='utf-8', ) as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        list_lines = []
        for line in csv_reader:
            list_lines.append(line)
    return list_lines

customer_list = []
customer_lines = read_csv("north_data/customers_data.csv")
for i in list(customer_lines):
    customer_lines_tuple = (str(i[0]), str(i[1]), str(i[2]))
    customer_list.append(customer_lines_tuple)
# print(customer_list)

employee_list = []
employee_lines = read_csv("north_data/employees_data.csv")
for i in list(employee_lines):
    employee_lines_tuple = (int(i[0]), str(i[1]), str(i[2]), str(i[3]), str(i[4]), str(i[5]))
    employee_list.append(employee_lines_tuple)
# print(employee_list)

orders_list = []
order_rows = read_csv("north_data/orders_data.csv")
for r in list(order_rows):
    order_rows_tuple = (int(r[0]), str(r[1]), int(r[2]), str(r[3]), str(r[4]))
    orders_list.append(order_rows_tuple)
# print(orders_list)

conn = psycopg2.connect(host="localhost", database="north", user="postgres", password="postgres_psw")

try:
    with conn:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO customer VALUES (%s, %s, %s)", customer_list)
            cur.executemany("INSERT INTO employee VALUES (%s, %s, %s, %s, %s, %s)", employee_list)
            cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders_list)
finally:
    conn.close()
