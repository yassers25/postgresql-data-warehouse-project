/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the COPY command to load data from CSV files to bronze tables.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_batch_start_time TIMESTAMP;
    v_batch_end_time TIMESTAMP;
    v_duration INTEGER;
BEGIN
    v_batch_start_time := CLOCK_TIMESTAMP();
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    -- Loading CRM Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- Table: crm_cust_info
    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM 'E:/Desktop/data warehouse/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', v_duration;
    RAISE NOTICE '>> -------------';

    -- Table: crm_prd_info
    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM 'E:/Desktop/data warehouse/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', v_duration;
    RAISE NOTICE '>> -------------';

    -- Table: crm_sales_details
    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM 'E:/Desktop/data warehouse/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', v_duration;
    RAISE NOTICE '>> -------------';

    -- Loading ERP Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- Table: erp_loc_a101
    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM 'E:/Desktop/data warehouse/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', v_duration;
    RAISE NOTICE '>> -------------';

    -- Table: erp_cust_az12
    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM 'E:/Desktop/data warehouse/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', v_duration;
    RAISE NOTICE '>> -------------';

    -- Table: erp_px_cat_g1v2
    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM 'E:/Desktop/data warehouse/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', v_duration;
    RAISE NOTICE '>> -------------';

    -- Final summary
    v_batch_end_time := CLOCK_TIMESTAMP();
    v_duration := EXTRACT(EPOCH FROM (v_batch_end_time - v_batch_start_time))::INTEGER;
    
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', v_duration;
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==========================================';
        RAISE;
END;
$$;
