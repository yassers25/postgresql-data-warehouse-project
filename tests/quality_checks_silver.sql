/*
===============================================================================
Quality Checks - Silver Layer
===============================================================================
Script Purpose:
    This script performs data quality validation on the 'silver' layer after
    the ETL transformation from bronze layer.
    
    These checks verify that:
    - All data quality issues from Bronze have been resolved
    - Primary keys are unique and not null
    - No unwanted spaces remain in string fields
    - Data has been properly standardized and normalized
    - Date validations are correct
    - Business rules are enforced

Usage Notes:
    - Run these checks AFTER executing: CALL silver.load_silver();
    - All checks should return NO RESULTS if data quality is good
    - Any results indicate data quality issues that need investigation
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- Check 1: Primary Key Validation
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: NO RESULTS (all duplicates removed, only latest records kept)
SELECT 
    cst_id,
    COUNT(*) as count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
-- Expected: 0 rows

-- Check 2: Unwanted Spaces in cst_firstname
-- Check for Unwanted Spaces in First Name
-- Expectation: NO RESULTS (all spaces trimmed)
SELECT 
    cst_firstname 
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
-- Expected: 0 rows

-- Check 3: Unwanted Spaces in cst_lastname
-- Check for Unwanted Spaces in Last Name
-- Expectation: NO RESULTS (all spaces trimmed)
SELECT 
    cst_lastname 
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
-- Expected: 0 rows

-- Check 4: Standardized Marital Status Values
-- Check Data Standardization: Marital Status
-- Expectation: Only 'Single', 'Married', 'n/a'
SELECT DISTINCT 
    cst_marital_status,
    COUNT(*) as count
FROM silver.crm_cust_info
GROUP BY cst_marital_status
ORDER BY cst_marital_status;
-- Expected: Only 'Married', 'Single', 'n/a'

-- Check 5: Standardized Gender Values
-- Check Data Standardization: Gender
-- Expectation: Only 'Female', 'Male', 'n/a'
SELECT DISTINCT 
    cst_gndr,
    COUNT(*) as count
FROM silver.crm_cust_info
GROUP BY cst_gndr
ORDER BY cst_gndr;
-- Expected: Only 'Female', 'Male', 'n/a'


-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

-- Check 1: Primary Key Validation
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: NO RESULTS
SELECT 
    prd_id,
    COUNT(*) as count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
-- Expected: 0 rows

-- Check 2: Unwanted Spaces in prd_nm
-- Check for Unwanted Spaces in Product Name
-- Expectation: NO RESULTS
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
-- Expected: 0 rows

-- Check 3: Product Cost Validation
-- Check for NULLs or Negative Values in Cost
-- Expectation: NO RESULTS (nulls replaced with 0)
SELECT 
    prd_id,
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
-- Expected: 0 rows

-- Check 4: Standardized Product Line Values
-- Check Data Standardization: Product Line
-- Expectation: Only 'Mountain', 'Road', 'Other Sales', 'Touring', 'n/a'
SELECT DISTINCT 
    prd_line,
    COUNT(*) as count
FROM silver.crm_prd_info
GROUP BY prd_line
ORDER BY prd_line;
-- Expected: Only 'Mountain', 'Other Sales', 'Road', 'Touring', 'n/a'

-- Check 5: Date Range Validation
-- Check for Invalid Date Orders (End Date < Start Date)
-- Expectation: NO RESULTS (end date must be after or equal to start date)
SELECT 
    prd_id,
    prd_key,
    prd_start_dt,
    prd_end_dt
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
-- Expected: 0 rows
-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

-- Check 1: Date Order Validation
-- Check for Invalid Date Orders (Order Date > Ship/Due Dates)
-- Expectation: NO RESULTS
SELECT 
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;
-- Expected: 0 rows

-- Check 2: Sales Calculation Consistency
-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: NO RESULTS (all calculations corrected)
SELECT 
    sls_ord_num,
    sls_sales,
    sls_quantity,
    sls_price,
    (sls_quantity * sls_price) as calculated_sales
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
LIMIT 10;
-- Expected: 0 rows

-- Check 3: Date Format Validation
-- Check for Invalid Date Formats
-- Expectation: All dates should be valid DATE type
SELECT 
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL
   OR sls_ship_dt IS NULL
   OR sls_due_dt IS NULL
LIMIT 10;
-- Note: Some NULLs may be acceptable if original data was invalid


-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

-- Check 1: Customer ID Format
-- Check for 'NAS' Prefix in Customer ID
-- Expectation: NO RESULTS (all 'NAS' prefixes removed)
SELECT 
    cid
FROM silver.erp_cust_az12
WHERE cid LIKE 'NAS%';
-- Expected: 0 rows

-- Check 2: Future Birth Dates
-- Check for Future Birth Dates
-- Expectation: NO RESULTS (future dates set to NULL)
SELECT 
    cid,
    bdate
FROM silver.erp_cust_az12
WHERE bdate > CURRENT_DATE;
-- Expected: 0 rows

-- Check 3: Birth Date Range
-- Check for Out-of-Range Birth Dates
-- Expectation: All dates after today (future birth dates)
SELECT 
    bdate,
    COUNT(*) AS count
FROM silver.erp_cust_az12
WHERE bdate > CURRENT_DATE
GROUP BY bdate
ORDER BY bdate;

-- Expected: 0 rows

-- Check 4: Standardized Gender Values
-- Check Data Standardization: Gender
-- Expectation: Only 'Female', 'Male', 'n/a'
SELECT DISTINCT 
    gen,
    COUNT(*) as count
FROM silver.erp_cust_az12
GROUP BY gen
ORDER BY gen;
-- Expected: Only 'Female', 'Male', 'n/a'


-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

-- Check 1: Customer ID Format
-- Check for Hyphens in Customer ID
-- Expectation: NO RESULTS (all hyphens removed)
SELECT 
    cid
FROM silver.erp_loc_a101
WHERE cid LIKE '%-%';
-- Expected: 0 rows

-- Check 2: Standardized Country Values
-- Check Data Standardization: Country
-- Expectation: Standardized country names or 'n/a'
SELECT DISTINCT 
    cntry,
    COUNT(*) as count
FROM silver.erp_loc_a101
GROUP BY cntry
ORDER BY cntry;
-- Expected: Full country names like 'Germany', 'United States', or 'n/a'

-- Check 3: Missing Country Values
-- Check for Missing Country Values
-- Expectation: NO empty strings (should be 'n/a')
SELECT 
    cid,
    cntry
FROM silver.erp_loc_a101
WHERE TRIM(cntry) = '';
-- Expected: 0 rows

-- Check 4: Unwanted Spaces in Country
-- Check for Unwanted Spaces
-- Expectation: NO RESULTS
SELECT 
    cntry
FROM silver.erp_loc_a101
WHERE cntry != TRIM(cntry);
-- Expected: 0 rows


-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Check 1: Unwanted Spaces Validation
-- Check for Unwanted Spaces
-- Expectation: NO RESULTS
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);
-- Expected: 0 rows

-- Check 2: Maintenance Status Values
-- Check Data Standardization: Maintenance Status
SELECT DISTINCT 
    maintenance,
    COUNT(*) as count
FROM silver.erp_px_cat_g1v2
GROUP BY maintenance
ORDER BY maintenance;

-- ====================================================================
-- Cross-Table Validation Checks
-- ====================================================================

-- Check 1: Sales with Invalid Customer References
-- Check: All sales records reference valid customers
SELECT 
    sls_ord_num,
    sls_cust_id
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
LIMIT 10;
-- Expected: 0 rows (or document known orphan records)

-- Check 2: Sales with Invalid Product References
-- Check: All sales records reference valid products
SELECT 
    sls_ord_num,
    sls_prd_key
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
LIMIT 10;
-- Expected: 0 rows (or document known orphan records)

-- ====================================================================
-- Silver Layer Quality Checks Completed
-- ====================================================================
-- Review any results returned above - ideally all checks return 0 rows
-- ====================================================================
