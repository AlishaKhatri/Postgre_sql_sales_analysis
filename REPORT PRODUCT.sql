/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
===============================================================================
*/
-- =============================================================================
-- Create Report: gold.report_products
-- =============================================================================


CREATE VIEW gold.report_products AS
WITH base_query AS (
    SELECT
        f.order_number,
        f.order_date,
        f.sales_amount,
        f.quantity,
        f.customer_key,
		p.product_key,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    WHERE order_date IS NOT NULL -- only considering the valid sales dates
),

product_aggregations AS (

-- product_aggregations summarizes key metrics at product level 
SELECT 
     product_key,
	 product_name,
	 category,
	 subcategory,
	 cost,
	 (EXTRACT(YEAR FROM MAX(order_date)) - EXTRACT(YEAR FROM MIN(order_date))) * 12
            + (EXTRACT(MONTH FROM MAX(order_date)) - EXTRACT(MONTH FROM MIN(order_date))) AS lifespan,
	  MAX(order_date) AS last_sales_date,
	  COUNT(DISTINCT order_number) AS total_orders,
	  COUNT(DISTINCT customer_key) AS total_customers,
	  SUM(sales_amount) AS total_sales,
	  SUM(quantity) AS total_quantity,
	  ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0))::numeric, 1) AS avg_selling_price
FROM base_query
GROUP BY 
     product_key,
	 product_name,
	 category,
	 subcategory,
	 cost
)

-- FINAL QUERY COMBINING ALL PRODUCT RESULTS INTO OUTPUT
SELECT 
      product_key,
	  product_name,
	  category,
	  subcategory,
	  cost,
	  last_sales_date,
	  EXTRACT(YEAR FROM AGE(CURRENT_DATE, last_sales_date)) * 12
        + EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_sales_date)) AS months_of_recency,
	  CASE 
	      WHEN total_sales > 50000 THEN 'High-Performer'
		  WHEN total_sales >= 10000 THEN 'Mid-Range'
		  ELSE 'Low-Performer'
	   END AS product_segment,
	   lifespan,
	   total_orders,
	   total_sales,
	   total_quantity,
	   total_customers,
	   avg_selling_price,
	   -- AVERAGE ORDER REVENUE (AOR)
	   CASE WHEN total_orders = 0 THEN 0
       ELSE total_sales/total_orders
END AS avg_order_revenue,

        -- AVERAGE MONTHLY REVENUE
		CASE WHEN lifespan = 0 THEN total_sales
        ELSE ROUND(total_sales / lifespan::numeric, 2)

END AS avg_monthly_revenue
FROM product_aggregations;





