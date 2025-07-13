-- Create staging tables for ETL process
CREATE TABLE stg_orders (
    order_id INT,
    customer_id INT,
    product_id INT,
    store_id INT,  -- standardized from store_code
    order_datetime DATETIME,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_price DECIMAL(10,2),
    payment_method VARCHAR(50),
    is_valid_order BIT DEFAULT 1, -- flag to help reject bad rows
    raw_store_code VARCHAR(50),   -- retain original value for audit/debug
    raw_price_string VARCHAR(50), -- retain original value for audit/debug
    raw_order_time VARCHAR(50)    -- retain raw timestamp for parsing tracking
);

CREATE TABLE stg_products (
    product_id INT,
    name_clean VARCHAR(100),
    category_clean VARCHAR(50),
    unit_price DECIMAL(10,2),
    in_stock_flag BIT,
    added_date DATE,
    is_valid_product BIT DEFAULT 1,
    raw_price_string VARCHAR(50),
    raw_category VARCHAR(50),
    raw_stock_status VARCHAR(50)
);

CREATE TABLE stg_customers (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    join_date DATE,
    region_code_clean VARCHAR(10),
    is_valid_email BIT DEFAULT 1,
    raw_region_code VARCHAR(50),
    raw_email VARCHAR(100)
);
