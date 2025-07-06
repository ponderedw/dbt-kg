-- Creating source tables and populating them in PostgreSQL
CREATE SCHEMA IF NOT EXISTS raw;

-- 1. Customers source
CREATE TABLE if not exists raw.customers (
    customer_id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    created_at TIMESTAMP DEFAULT now()
);

Truncate table raw.customers;

INSERT INTO raw.customers (name, email) VALUES
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith', 'bob@example.com'),
    ('Charlie Brown', 'charlie@example.com');

-- 2. Orders source
CREATE TABLE if not exists raw.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES raw.customers(customer_id),
    order_total DECIMAL(10,2),
    order_date TIMESTAMP DEFAULT now()
);

Truncate table raw.orders;

INSERT INTO raw.orders (customer_id, order_total) VALUES
    (1, 250.75),
    (2, 100.00),
    (3, 320.50),
    (1, 75.25);
