-- =============================================================
-- Quality Checks (PostgreSQL Version)
-- =============================================================
-- This script performs quality checks on the Gold Layer to validate:
-- - Uniqueness of surrogate keys in dimension tables
-- - Referential integrity between fact and dimension tables
-- - Data model relationships for analytical purposes
--
-- Usage: Run these checks after loading the Gold layer
-- Expectation: All queries should return no results
-- Action: Investigate and resolve any discrepancies found
-- =============================================================

-- Check for duplicate customer keys in dim_customers
-- Expectation: No results
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Check for duplicate product keys in dim_products
-- Expectation: No results
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- Check referential integrity between fact_sales and dimensions
-- Expectation: No results (all fact records should have valid dimension keys)
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
   OR c.customer_key IS NULL;
