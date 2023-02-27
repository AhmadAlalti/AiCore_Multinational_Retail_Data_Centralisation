-- orders_table

-- Changing the columns datatype in oders_table
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(50),
ALTER COLUMN store_code TYPE VARCHAR(50),
ALTER COLUMN product_code TYPE VARCHAR(50),
ALTER COLUMN product_quantity TYPE SMALLINT;

-- Getting the maximum length of the varchar to change the type
SELECT MAX(CHAR_LENGTH(card_number)) FROM orders_table;
SELECT MAX(CHAR_LENGTH(store_code)) FROM orders_table;
SELECT MAX(CHAR_LENGTH(product_code)) FROM orders_table;

-- Changing the columns datatype according to max varchar
ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11);

-- dim_users

-- Changing the columns datatype in dim_users
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE;

-- dim_store_details

-- Changing the columns datatype in dim_store_details
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(50),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);


-- dim_products

-- Add weight_class column
ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(14);
UPDATE dim_products
SET weight_class = CASE
    WHEN weight_kg < 2 THEN 'Light'
    WHEN weight_kg BETWEEN 3 AND 40 THEN 'Mid_Sized'
    WHEN weight_kg BETWEEN 41 AND 140 THEN 'Heavy'
    ELSE 'Truck_required'
END;

-- Change Column name
ALTER TABLE dim_products RENAME COLUMN removed TO still_available;

-- Change columns datatypes
ALTER TABLE dim_products
ALTER COLUMN price_£ TYPE FLOAT,
ALTER COLUMN weight_kg TYPE FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(50),
ALTER COLUMN product_code TYPE VARCHAR(50),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
ALTER COLUMN still_available TYPE BOOL USING 
	CASE 
		WHEN still_available LIKE 'Still_available' THEN true
		ELSE false
	END;


-- dim_date_times

-- Change columns datatypes
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE CHAR(2),
ALTER COLUMN year TYPE CHAR(4),
ALTER COLUMN day TYPE CHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(11),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

-- dim_card_details

-- Change columns datatypes
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE;


-- Make Primary keys
ALTER TABLE dim_card_details
ADD CONSTRAINT PK_dim_card_details_card_number PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
ADD CONSTRAINT PK_dim_date_times_date_uuid PRIMARY KEY (date_uuid);

ALTER TABLE dim_store_details
ADD CONSTRAINT PK_dim_store_details_store_code PRIMARY KEY (store_code);

ALTER TABLE dim_products
ADD CONSTRAINT PK_dim_products_product_code PRIMARY KEY (product_code);

ALTER TABLE dim_users
ADD CONSTRAINT PK_dim_users_user_uuid PRIMARY KEY (user_uuid);


-- Make foreign keys
INSERT INTO dim_store_details (store_code, staff_numbers, opening_date, country_code)
VALUES ('WEB-1388012W', 325, '2010-06-12', 'WEB');

ALTER TABLE orders_table
ADD CONSTRAINT FK_orders_table_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
ADD CONSTRAINT FK_orders_table_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid);

INSERT INTO dim_store_details (store_code, staff_numbers, opening_date, store_type)
VALUES ('WEB-1388012W', 325, '2010-06-12', 'Web Portal');

DELETE FROM orders_table
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

ALTER TABLE orders_table
ADD CONSTRAINT FK_orders_table_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code);

DELETE FROM orders_table
WHERE product_code NOT IN (SELECT product_code FROM dim_products);

ALTER TABLE orders_table
ADD CONSTRAINT FK_orders_table_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);

DELETE FROM orders_table
WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);

ALTER TABLE orders_table
ADD CONSTRAINT FK_orders_table_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);

-- How many stores does the business have and in which countries?
SELECT country_code AS country, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- Which locations currently have the most stores?
SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

-- Which months produce the most sales?
SELECT SUM(orders_table.product_quantity * dim_products.price_£) as total_sales, dim_date_times.month 
FROM dim_date_times 
JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid 
JOIN dim_products ON orders_table.product_code = dim_products.product_code 
GROUP BY dim_date_times.month 
ORDER BY total_sales DESC;

-- How many sales are coming from online?

SELECT COUNT(*) as number_of_sales, SUM(product_quantity) as product_quantity_count,
CASE 
    WHEN store_code LIKE 'WEB%' THEN 'Web'
    ELSE 'Offline'
END AS location    
FROM orders_table
GROUP BY location

-- What percentage of sales comes through from each type of store?
SELECT
	store_type,
	SUM(product_quantity * price_£) AS total_sales,
	ROUND((SUM(product_quantity * price_£)::numeric / SUM(SUM(product_quantity * price_£)::numeric) OVER ()) * 100.0, 2) AS percentage_total
FROM
	orders_table
	JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY store_type
ORDER BY total_sales DESC;


-- Which month in which year produced the most sales?
SELECT
    SUM(product_quantity * price_£) AS total_sales,
    year, month 
FROM orders_table
    JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 5;

-- What is our staff headcount?
SELECT 
	SUM(staff_numbers) as total_staff_numbers, country_code
FROM
	dim_store_details
GROUP BY
	country_code
ORDER BY
	total_staff_numbers DESC

-- Which German Store Type is selling the most?
SELECT 
	SUM(product_quantity * price_£) AS total_sales, 
	store_type, 
	country_code 
FROM orders_table
	JOIN dim_products on orders_table.product_code = dim_products.product_code 
	JOIN dim_store_details on orders_table.store_code = dim_store_details.store_code AND dim_store_details.country_code = 'DE' 
GROUP BY store_type, country_code
ORDER BY total_sales 

-- How quickly is the company making sales?
WITH cte AS(
    SELECT TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') as datetimes, year FROM dim_date_times
    ORDER BY datetimes DESC
), cte2 AS(
    SELECT 
        year, 
        datetimes, 
        LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) as time_difference 
        FROM cte
) SELECT year, AVG((datetimes - time_difference)) as actual_time_taken FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC
