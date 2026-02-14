CREATE TABLE users (
    id INTEGER,
    name VARCHAR(50),
    email VARCHAR(100),
    age INTEGER,
    active BOOLEAN,
    balance FLOAT
);

CREATE TABLE products (
    id INTEGER,
    name VARCHAR(80),
    price FLOAT,
    in_stock BOOLEAN,
    quantity INTEGER
);

CREATE TABLE orders (
    id INTEGER,
    user_id INTEGER,
    product_id INTEGER,
    amount INTEGER,
    total FLOAT
);

INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30, TRUE, 1500.75);
INSERT INTO users VALUES (2, 'Bob', 'bob@test.org', 25, TRUE, 320.00);
INSERT INTO users VALUES
    (3, 'Charlie', 'charlie@mail.com', 42, FALSE, -50.25),
    (4, 'Diana', 'diana@example.com', 19, TRUE, 0.0),
    (5, 'Eve', 'eve@secure.net', 37, TRUE, 9999.99);

INSERT INTO products VALUES
    (1, 'Laptop', 999.99, TRUE, 15),
    (2, 'Mouse', 29.50, TRUE, 200),
    (3, 'Keyboard', 75.00, TRUE, 80),
    (4, 'Monitor', 450.00, FALSE, 0),
    (5, 'USB Cable', 9.99, TRUE, 500);

INSERT INTO orders VALUES
    (1, 1, 1, 1, 999.99),
    (2, 1, 2, 2, 59.00),
    (3, 2, 5, 10, 99.90),
    (4, 3, 3, 1, 75.00),
    (5, 5, 1, 1, 999.99),
    (6, 4, 2, 3, 88.50);

SELECT * FROM users;
SELECT * FROM products;
SELECT * FROM orders;

SELECT name, email FROM users;
SELECT name, price FROM products;
SELECT id, user_id, total FROM orders;

SELECT * FROM users WHERE id = 3;
SELECT * FROM products WHERE name = 'Laptop';

SELECT * FROM users WHERE age > 30;
SELECT * FROM users WHERE age <= 25;
SELECT * FROM users WHERE balance >= 0.0;
SELECT * FROM products WHERE price < 100.0;
SELECT name, price FROM products WHERE price != 999.99;

SELECT * FROM users WHERE active = TRUE;
SELECT * FROM users WHERE active = FALSE;
SELECT * FROM products WHERE in_stock = TRUE;
SELECT name FROM products WHERE in_stock = FALSE;

SELECT * FROM users WHERE age > 20 AND active = TRUE;
SELECT * FROM users WHERE balance > 0.0 AND age < 40;
SELECT * FROM products WHERE in_stock = TRUE AND price < 100.0;

SELECT * FROM users WHERE age < 20 OR age > 40;
SELECT * FROM users WHERE balance < 0.0 OR balance > 5000.0;

SELECT * FROM users WHERE (age > 30 AND active = TRUE) OR balance > 1000.0;
SELECT * FROM products WHERE (price < 50.0 AND in_stock = TRUE) OR quantity = 0;

SELECT * FROM users WHERE balance > -100.0;

SELECT * FROM products WHERE price > 50;
SELECT * FROM users WHERE balance < 1000;

DELETE FROM orders WHERE id = 6;
SELECT * FROM orders;

DELETE FROM orders WHERE user_id = 1 AND total < 100.0;
SELECT * FROM orders;

DELETE FROM orders;
SELECT * FROM orders;
