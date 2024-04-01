-- SQL-команды для создания таблиц

CREATE TABLE customer
(
    customer_id varchar(5) PRIMARY KEY,
    company_name varchar(100),
    contact_name varchar(100)
);

CREATE TABLE employee
(
    employee_id int PRIMARY KEY,
    first_name  varchar(100),
    last_name  varchar(100),
	title  varchar(100),
    birth_date date,
    notes text
);

CREATE TABLE orders
(
    order_id int PRIMARY KEY,
    customer_id varchar(5) REFERENCES customer(customer_id) NOT NULL,
    employee_id int REFERENCES employee(employee_id) NOT NULL,
	order_date date,
    ship_city varchar(100)
);