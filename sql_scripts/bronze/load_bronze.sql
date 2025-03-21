/*
===========================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===========================================================
Script Purpose:
	- This stored procedure loads data into the 'bronze' schema from csv files
	- Performs the following actions:
 		- truncates the bronze tables before loading data
		- bulk inserts data into the tables
Parameters:
	None

Usage:
	CALL bronze.load_bronze();

*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time TIMESTAMP;
	stop_time TIMESTAMP;
	batch_starttime TIMESTAMP;
	batch_endtime TIMESTAMP;
BEGIN
	BEGIN
		batch_starttime := clock_timestamp();
		RAISE NOTICE '===============================';
		RAISE NOTICE 'loading bronze layer';
		RAISE NOTICE '===============================';
		RAISE NOTICE ' ';
		
		RAISE NOTICE '--------------------------------';
		RAISE NOTICE 'Loading CRM TABLES';
		RAISE NOTICE '--------------------------------';
	
		
		--  bronze.crm_cust_info
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating bronze.crm_cust_info';
		TRUNCATE bronze.crm_cust_info;
		RAISE NOTICE '>> Inserting data into bronze.crm_cust_info';
		COPY bronze.crm_cust_info
		FROM '/tmp/datasets/source_crm/cust_info.csv'
		DELIMITER ','
		CSV HEADER;
		
		stop_time := clock_timestamp();
		RAISE NOTICE 'Time taken: %s', stop_time - start_time;
		RAISE NOTICE '****************';
		RAISE NOTICE ' ';
		
		--  bronze.crm_prd_info
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating bronze.crm_prd_info';
		TRUNCATE bronze.crm_prd_info;
		RAISE NOTICE '>> Inserting data into bronze.crm_prd_info';
		COPY bronze.crm_prd_info
		FROM '/tmp/datasets/source_crm/prd_info.csv'
		DELIMITER ','
		CSV HEADER;
		
		stop_time := clock_timestamp();
		RAISE NOTICE 'Time taken: %s', stop_time - start_time;
		RAISE NOTICE '****************';
		RAISE NOTICE ' ';
		
		--  bronze.crm_sales_details
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating bronze.crm_sales_details';
		TRUNCATE bronze.crm_sales_details;
		RAISE NOTICE '>> Inserting data into bronze.crm_sales_details';
		COPY bronze.crm_sales_details
		FROM '/tmp/datasets/source_crm/sales_details.csv'
		DELIMITER ','
		CSV HEADER;
		
		stop_time := clock_timestamp();
		RAISE NOTICE 'Time taken: %s', stop_time - start_time;
		RAISE NOTICE '****************';
		RAISE NOTICE ' ';
	
	
		RAISE NOTICE '--------------------------------';
		RAISE NOTICE 'Loading ERP TABLES';
		RAISE NOTICE '--------------------------------';
		
		--  bronze.erp_cust_az12
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating bronze.erp_cust_az12';
		TRUNCATE bronze.erp_cust_az12;
		RAISE NOTICE '>> Inserting data into bronze.erp_cust_az12';
		COPY bronze.erp_cust_az12
		FROM '/tmp/datasets/source_erp/CUST_AZ12.csv'
		DELIMITER ','
		CSV HEADER;
		
		stop_time := clock_timestamp();
		RAISE NOTICE 'Time taken: %s', stop_time - start_time;
		RAISE NOTICE '****************';
		RAISE NOTICE ' ';
		
		--  bronze.erp_loc_a101
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating bronze.erp_loc_a101';
		TRUNCATE bronze.erp_loc_a101;
		RAISE NOTICE '>> Inserting data into bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101
		FROM '/tmp/datasets/source_erp/LOC_A101.csv'
		DELIMITER ','
		CSV HEADER;
		
		stop_time := clock_timestamp();
		RAISE NOTICE 'Time taken: %s', stop_time - start_time;
		RAISE NOTICE '****************';
		RAISE NOTICE ' ';
		
		--  bronze.erp_px_cat_g1v2
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating bronze.erp_px_cat_g1v2';
		TRUNCATE bronze.erp_px_cat_g1v2;
		RAISE NOTICE '>> Inserting data into bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2
		FROM '/tmp/datasets/source_erp/PX_CAT_G1V2.csv'
		DELIMITER ','
		CSV HEADER;
		
		stop_time := clock_timestamp();
		RAISE NOTICE 'Time taken: %s', stop_time - start_time;
		RAISE NOTICE '****************';
		batch_endtime := clock_timestamp();
		RAISE NOTICE ' ';
		RAISE NOTICE '==================================';
		RAISE NOTICE 'Loading bronze layer completed';
		RAISE NOTICE 'Time taken: % seconds', batch_endtime - batch_starttime;
		RAISE NOTICE '==================================';
	
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'eRROR: %', SQLERRM;
	END;
END;
$$;