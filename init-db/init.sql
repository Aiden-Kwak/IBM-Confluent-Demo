-- CDC 실습용 쇼핑몰 데이터베이스 초기화

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock INT NOT NULL DEFAULT 0
);

-- 초기 상품 데이터
INSERT INTO products (name, category, price, stock) VALUES
    ('MacBook Pro 14"', 'electronics', 2499.99, 50),
    ('iPhone 15 Pro', 'electronics', 1199.99, 200),
    ('AirPods Pro', 'electronics', 249.99, 500),
    ('Nike Air Max', 'shoes', 179.99, 300),
    ('Levi''s 501', 'clothing', 89.99, 400),
    ('Samsung Galaxy S24', 'electronics', 999.99, 150),
    ('Sony WH-1000XM5', 'electronics', 349.99, 100),
    ('Adidas Ultraboost', 'shoes', 189.99, 250);

-- 초기 주문 데이터
INSERT INTO orders (customer_name, product, quantity, price, status) VALUES
    ('김철수', 'MacBook Pro 14"', 1, 2499.99, 'COMPLETED'),
    ('이영희', 'AirPods Pro', 2, 499.98, 'COMPLETED'),
    ('박민수', 'Nike Air Max', 1, 179.99, 'SHIPPED'),
    ('정지은', 'iPhone 15 Pro', 1, 1199.99, 'PENDING');

-- Debezium CDC를 위한 publication 생성
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
