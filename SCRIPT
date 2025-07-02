-- Create schema
CREATE SCHEMA IF NOT EXISTS gold;

-- Create dimension and fact tables
CREATE TABLE gold.dim_customers (
    customer_key INT,
    customer_id INT,
    customer_number VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    country VARCHAR(50),
    marital_status VARCHAR(50),
    gender VARCHAR(50),
    birthdate DATE,
    create_date DATE
);

CREATE TABLE gold.dim_products (
    product_key INT,
    product_id INT,
    product_number VARCHAR(50),
    product_name VARCHAR(50),
    category_id VARCHAR(50),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    maintenance VARCHAR(50),
    cost INT,
    product_line VARCHAR(50),
    start_date DATE
);

CREATE TABLE gold.fact_sales (
    order_number VARCHAR(50),
    product_key INT,
    customer_key INT,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount INT,
    quantity SMALLINT,
    price INT
);


SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;
SELECT * FROM gold.fact_sales;

-- Trend sales over the time
SELECT EXTRACT(YEAR FROM order_date) AS year,
       EXTRACT(MONTH FROM order_date) AS month, 
       SUM(sales_amount) AS total_sales,
	   COUNT(DISTINCT customer_key) AS total_customers,
	   SUM(quantity) as total_quantity	   
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY year, month
ORDER BY year, month;





-- Comulative Analysis (Window Functions)
-- Total Sales per month and running total of sales over time
SELECT 
    MAKE_DATE(year::int, month::int, 1) AS order_date,
    total_sales,
    SUM(total_sales) OVER (
        PARTITION BY year
        ORDER BY MAKE_DATE(year::int, month::int, 1)
    ) AS running_total_sales,
    
    AVG(avg_price) OVER (
        PARTITION BY year
        ORDER BY MAKE_DATE(year::int, month::int, 1)
    ) AS moving_average_price
FROM (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month, 
        SUM(sales_amount) AS total_sales,
        AVG(price) AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
) AS monthly_sales;



-- Yearly
SELECT 
    MAKE_DATE(year::int, 1, 1) AS order_date,
    total_sales,
    SUM(total_sales) OVER (
        ORDER BY MAKE_DATE(year::int, 1, 1)
    ) AS running_total_sales,
    
    AVG(avg_price) OVER (
        ORDER BY MAKE_DATE(year::int, 1, 1)
    )::DECIMAL(10,2) AS moving_average_price
FROM (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS year,
        SUM(sales_amount) AS total_sales,
        AVG(price)::DECIMAL(10,2) AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM order_date)
) AS yearly_sales;


-- Performance Analysis (Comparison with tearget measure)
-- analyzing yearly performance of products by comparing each products's sales to both average sales performance and the previous year's sales
WITH yearly_product_sales AS (
    SELECT 
        EXTRACT(YEAR FROM f.order_date) AS order_year,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY order_year, p.product_name
)

SELECT 
    order_year,
    product_name,
    current_sales,

    -- Average sales per product over all years
    AVG(current_sales) OVER (PARTITION BY product_name)::DECIMAL(10,0) AS average_sales,

    -- Difference from average
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name)::DECIMAL(10,0) AS diff_avg,

    -- Classification against average
    CASE 
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name)::DECIMAL(10,0) > 0 THEN 'Above AVG'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name)::DECIMAL(10,0) < 0 THEN 'Below AVG'
        ELSE 'AVG'
    END AS avg_change,

    -- Prior year sales using LAG
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,

    -- Difference from previous year
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,

    -- Classification of yearly trend
    CASE 
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increasing'
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decreasing'
        ELSE 'No Change'
    END AS py_change

FROM yearly_product_sales
ORDER BY  product_name,order_year;

-- Part-To-Whole Analysis
-- Which category contribute to the most overall sales
WITH category_sales AS(
SELECT p.category,
SUM(f.sales_amount) as total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
GROUP BY p.category)

SELECT category,
total_sales,
SUM(total_sales) OVER() AS overall_sales,
CONCAT(ROUND((total_sales * 100.0 / SUM(total_sales) OVER ()), 2), '%') AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC
;


-- Customer Segmentation
-- segment products into cost ranges and count how many products fall into each category

WITH product_segments AS (
    SELECT 
        product_key,
        product_name,
        cost,
        CASE 
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)

SELECT 
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products;


-- Grouping customers into three segments based on their spending behaviour
--VIP : atleast 12 months of history and spending of more than 5000
--Regular : atleast 12 months of history but spending 5000 or less
--New: lifespan of less than 12 months
-- Total Number of customers by each group
WITH customer_spending AS (
    SELECT
        c.customer_key,
        SUM(f.sales_amount) AS total_spending,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order,
        (EXTRACT(year FROM MAX(order_date)) - EXTRACT(year FROM MIN(order_date))) * 12
        + (EXTRACT(month FROM MAX(order_date)) - EXTRACT(month FROM MIN(order_date))) AS lifespan
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT 
    customer_segment,
    COUNT(customer_key) AS total_customers
FROM (
    SELECT 
        customer_key,
        CASE 
            WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM customer_spending
) AS segmented_customers
GROUP BY customer_segment
ORDER BY total_customers DESC;



