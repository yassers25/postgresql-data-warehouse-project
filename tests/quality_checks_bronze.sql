/*
===============================================================================
Quality Checks - Bronze Layer
===============================================================================
Script Purpose:
    This script performs data quality checks on the 'bronze' layer to identify
    issues that need to be addressed during the transformation to 'silver' layer.
    
    These checks help identify:
    - Null or duplicate primary keys
    - Unwanted spaces in string fields
    - Data inconsistencies and non-standardized values
    - Invalid date formats and ranges
    - Missing or incorrect values

Usage Notes:
    - Run these checks after loading the Bronze Layer (CALL bronze.load_bronze();)
    - Use findings to guide transformations in the Silver Layer
    - Each issue found here should be handled in the silver.load_silver() procedure
===============================================================================
*/

-- ====================================================================
-- Checking 'bronze.crm_cust_info'
-- ====================================================================

-- Check for Duplicates in cst_id
-- => A primary key must be unique and not null
-- Issue: Multiple records exist for the same customer ID
SELECT 
    cst_id,
    COUNT(*) as duplicate_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
ORDER BY duplicate_count DESC;

-- Example: Investigating duplicate customer
SELECT *
FROM bronze.crm_cust_info 
WHERE cst_id = 29466
ORDER BY cst_create_date DESC;

-- Records that will be filtered out (not the most recent)
-- Identify all non-latest records (these will be filtered out in Silver)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info 
) t
WHERE flag_last != 1;

-- Preview: Latest record per customer
-- Preview the cleaned dataset (only latest records)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info 
) t
WHERE flag_last = 1
LIMIT 10;

-- Check for unwanted spaces in cst_firstname
-- Issue: Leading/trailing spaces in customer names
SELECT 
    cst_firstname,
    TRIM(cst_firstname) AS cleaned_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
LIMIT 10;

-- Check distinct values in cst_marital_status
-- Check Data Standardization: Marital Status
-- Issue: Non-standardized abbreviations (S, M)
SELECT DISTINCT 
    cst_marital_status,
    COUNT(*) as count
FROM bronze.crm_cust_info
GROUP BY cst_marital_status
ORDER BY cst_marital_status;

-- Check distinct values in cst_gndr
-- Check Data Standardization: Gender
-- Issue: Non-standardized abbreviations (F, M)
SELECT DISTINCT 
    cst_gndr,
    COUNT(*) as count
FROM bronze.crm_cust_info
GROUP BY cst_gndr
ORDER BY cst_gndr;


-- ====================================================================
-- Checking 'bronze.crm_prd_info'
-- ====================================================================

-- Check for products with invalid category IDs
-- Check if extracted category IDs exist in the category table
-- Issue: Some products may have invalid category references
SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
    (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)
LIMIT 10;

-- Products without sales orders (informational)
-- Identify products without any sales orders
-- Note: This is acceptable as not all products may have been sold
SELECT
    prd_id,
    prd_key,
    SUBSTRING(prd_key, 7) AS prd_key_clean,
    COUNT(*) as product_count
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7) NOT IN (
    SELECT sls_prd_key FROM bronze.crm_sales_details
)
GROUP BY prd_id, prd_key
LIMIT 10;

-- Verify sales records don't have invalid product keys
SELECT 
    sls_prd_key,
    COUNT(*) as count
FROM bronze.crm_sales_details 
WHERE sls_prd_key LIKE 'FK%'
GROUP BY sls_prd_key;

-- Check for unwanted spaces in prd_nm
SELECT 
    prd_nm 
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)
LIMIT 10;

-- Check for NULL or negative prd_cost
-- Check for NULLs or Negative Values in Product Cost
-- Issue: Missing or invalid cost values
SELECT 
    prd_id,
    prd_key,
    prd_cost 
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL
LIMIT 10;

-- Check distinct values in prd_line
-- Check Data Standardization: Product Line
-- Issue: Non-standardized abbreviations (M, R, S, T)
SELECT DISTINCT 
    prd_line,
    COUNT(*) as count
FROM bronze.crm_prd_info
GROUP BY prd_line
ORDER BY prd_line;


-- ====================================================================
-- Checking 'bronze.crm_sales_details'
-- ====================================================================

-- Check for invalid sls_due_dt values
-- Check for Invalid Date Formats
-- Issue: Dates stored as integers, may contain invalid values
SELECT 
    sls_due_dt,
    LENGTH(sls_due_dt::TEXT) as date_length,
    COUNT(*) as count
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LENGTH(sls_due_dt::TEXT) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101
GROUP BY sls_due_dt
LIMIT 10;

-- Check for NULL or negative sls_sales
SELECT 
    sls_ord_num,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL 
   OR sls_sales <= 0
LIMIT 10;

-- Check for inconsistent sales calculations
-- Check Data Consistency: Sales = Quantity * Price
-- Issue: Calculated sales doesn't match stored value
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price,
    (sls_quantity * ABS(sls_price)) as calculated_sales,
    COUNT(*) as count
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * ABS(sls_price)
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
GROUP BY sls_sales, sls_quantity, sls_price
ORDER BY count DESC
LIMIT 10;


-- ====================================================================
-- Checking 'bronze.erp_cust_az12'
-- ====================================================================

-- Check for customer IDs with NAS prefix
-- Check for Invalid Customer ID Format
-- Issue: Some IDs may have 'NAS' prefix
SELECT 
    cid,
    SUBSTRING(cid, 4) as cleaned_cid
FROM bronze.erp_cust_az12
WHERE cid LIKE 'NAS%'
LIMIT 10;

-- Check for future birth dates
-- Issue: Birth dates in the future are invalid
SELECT 
    cid,
    bdate
FROM bronze.erp_cust_az12
WHERE bdate > CURRENT_DATE
LIMIT 10;

-- Check for birth dates outside reasonable range
SELECT 
    bdate,
    COUNT(*) as count
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > CURRENT_DATE
GROUP BY bdate
ORDER BY bdate
LIMIT 10;

-- Check distinct values in gen
-- Check Data Standardization: Gender
-- Issue: Multiple formats for gender (F, Female, M, Male)
SELECT DISTINCT 
    gen,
    COUNT(*) as count
FROM bronze.erp_cust_az12
GROUP BY gen
ORDER BY gen;


-- ====================================================================
-- Checking 'bronze.erp_loc_a101'
-- ====================================================================

-- Check for customer IDs with hyphens
-- Check for Invalid Customer ID Format
-- Issue: Some IDs may contain hyphens
SELECT 
    cid,
    REPLACE(cid, '-', '') as cleaned_cid
FROM bronze.erp_loc_a101
WHERE cid LIKE '%-%'
LIMIT 10;

-- Check distinct country values
-- Check Data Standardization: Country Codes
-- Issue: Non-standardized country codes and names
SELECT DISTINCT 
    cntry,
    COUNT(*) as count
FROM bronze.erp_loc_a101
GROUP BY cntry
ORDER BY cntry;

-- Check for NULL or empty country values
SELECT 
    cid,
    cntry
FROM bronze.erp_loc_a101
WHERE TRIM(cntry) = '' OR cntry IS NULL
LIMIT 10;


-- ====================================================================
-- Checking 'bronze.erp_px_cat_g1v2'
-- ====================================================================

-- Check for unwanted spaces in category fields
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance)
LIMIT 10;

-- Check distinct values in maintenance
-- Check Data Standardization: Maintenance Status
SELECT DISTINCT 
    maintenance,
    COUNT(*) as count
FROM bronze.erp_px_cat_g1v2
GROUP BY maintenance
ORDER BY maintenance;

-- ====================================================================
-- Bronze Layer Quality Checks Completed
-- ====================================================================
