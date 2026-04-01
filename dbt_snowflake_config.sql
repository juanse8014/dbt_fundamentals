USE ROLE ACCOUNTADMIN;
GRANT OWNERSHIP ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

CREATE DATABASE ANALYTICS;
CREATE DATABASE RAW;
CREATE SCHEMA RAW.JAFFLE_SHOP;
CREATE SCHEMA RAW.STRIPE;

CREATE TABLE RAW.JAFFLE_SHOP.CUSTOMERS(
    ID INTEGER,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR);

COPY INTO RAW.JAFFLE_SHOP.CUSTOMERS(ID, FIRST_NAME, LAST_NAME)
FROM 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
file_format= (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
);

select * from RAW.JAFFLE_SHOP.CUSTOMERS;

create table raw.jaffle_shop.orders(
    ID INTEGER,
    USER_ID INTEGER,
    ORDER_DATE DATE,
    STATUS VARCHAR,
    _ETL_LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO RAW.JAFFLE_SHOP.ORDERS(ID, USER_ID, ORDER_DATE, STATUS)
FROM 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
file_format= (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
);

SELECT * FROM RAW.JAFFLE_SHOP.ORDERS;

CREATE TABLE RAW.STRIPE.PAYMENT(
    ID INTEGER,
    ORDERID INTEGER,
    PAYMENTMETHOD VARCHAR,
    STATUS VARCHAR,
    AMOUNT INTEGER,
    CREATED DATE,
    _BATCHED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO RAW.STRIPE.PAYMENT(ID,ORDERID,PAYMENTMETHOD,STATUS,AMOUNT,CREATED)
FROM 's3://dbt-tutorial-public/stripe_payments.csv'
file_format= (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
);

SELECT * FROM RAW.STRIPE.PAYMENT;

-----------------------------------
-------- TESTING SNAPSHOTS --------
-----------------------------------
--Run DBT snapshot
SELECT * FROM ANALYTICS.DBT_SCHEMA_SNAPSHOTS.ORDERS_HISTORY;
SELECT * FROM ANALYTICS.DBT_SCHEMA.STG_JAFFLE_SHOP__ORDERS;

-- INSERT NEW COLUMNS
INSERT INTO RAW.JAFFLE_SHOP.ORDERS(ID, USER_ID, ORDER_DATE, STATUS, _ETL_LOADED_AT)
VALUES
(100, 100, '2025-02-15', 'shipped', current_timestamp),
(101, 84, '2025-02-15', 'shipped', current_timestamp),
(102, 42, '2025-02-15', 'shipped', current_timestamp),
(103, 80, '2025-02-15', 'shipped', current_timestamp),
(104, 66, '2025-02-15', 'shipped', current_timestamp);

--Run DBT snapshot
SELECT * FROM ANALYTICS.DBT_SCHEMA_SNAPSHOTS.ORDERS_HISTORY;

-- UPDATES
UPDATE RAW.JAFFLE_SHOP.ORDERS SET STATUS = 'return_pending', _ETL_LOADED_AT = current_timestamp
WHERE ID IN (100,101,102,103,104);

SELECT * FROM ANALYTICS.DBT_SCHEMA.STG_JAFFLE_SHOP__ORDERS;
--Run DBT snapshot
SELECT * FROM ANALYTICS.DBT_SCHEMA_SNAPSHOTS.ORDERS_HISTORY;

----------------------------------------
------------INCREMENTAL MODELS ---------
----------------------------------------
SELECT * FROM RAW.JAFFLE_SHOP.ORDERS;
--In this case whe are working with merge incremental Strategy
INSERT INTO RAW.JAFFLE_SHOP.ORDERS(ID, USER_ID, ORDER_DATE, STATUS, _ETL_LOADED_AT)
VALUES
(105, 100, '2025-02-15', 'shipped', current_timestamp),
(106, 84, '2025-02-15', 'shipped', current_timestamp),
(107, 42, '2025-02-15', 'shipped', current_timestamp),
(108, 80, '2025-02-15', 'shipped', current_timestamp),
(109, 66, '2025-02-15', 'shipped', current_timestamp);

SELECT * FROM RAW.JAFFLE_SHOP.ORDERS
ORDER BY ID DESC;

--Run dbt build
SELECT * FROM ANALYTICS.DBT_SCHEMA.STG_JAFFLE_SHOP__ORDERS;
SELECT * FROM ANALYTICS.DBT_SCHEMA.FCT_ORDERS;

INSERT INTO RAW.JAFFLE_SHOP.ORDERS(ID, USER_ID, ORDER_DATE, STATUS, _ETL_LOADED_AT)
VALUES
(110, 100, '2025-02-15', 'shipped', current_timestamp),
(111, 84, '2025-02-15', 'shipped', current_timestamp),
(112, 42, '2025-02-15', 'shipped', current_timestamp),
(113, 80, '2025-02-15', 'shipped', current_timestamp),
(114, 66, '2025-02-15', 'shipped', current_timestamp);


--Run dbt build
SELECT * FROM ANALYTICS.DBT_SCHEMA.STG_JAFFLE_SHOP__ORDERS;
SELECT * FROM ANALYTICS.DBT_SCHEMA.FCT_ORDERS;


