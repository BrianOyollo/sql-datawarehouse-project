/*
======================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
======================================================
Script Purpose:
	- This procedure perfroms the ETL process to load silver schema tables from the bronze schema.
	- Actions performed:
		- Truncate silver tables
		- Inserts transformed & cleansed tata from Bronze
Paremeters:
	- None
Usage Example:
	- CALL silver.load_silver();
=====================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time TIMESTAMP;
	stop_time TIMESTAMP;
	batch_starttime TIMESTAMP;
	batch_stoptime TIMESTAMP;
BEGIN 
	BEGIN
		batch_starttime := CURRENT_TIMESTAMP;
		RAISE NOTICE '**************************';
		RAISE NOTICE 'LOADING SILVER LAYER';
		RAISE NOTICE '**************************';
		RAISE NOTICE ' ';
		
		RAISE NOTICE '============================';
		RAISE NOTICE 'LOADING CRM TABLES';
		RAISE NOTICE '============================';

		-- silver.crm_prd_info
		RAISE NOTICE '--------------------------';
		RAISE NOTICE 'silver.crm_prd_info';
		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating table...';
		TRUNCATE silver.crm_cust_info;
		RAISE NOTICE '>> Inserting data...';
		INSERT INTO silver.crm_cust_info(
			cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status,
			cst_gndr, cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firtname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'N/A'
			END AS cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 ELSE 'N/A'
			END AS cst_gndr,
			cst_create_date
		FROM (
			SELECT * ,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS most_recent
			FROM bronze.crm_cust_info
			) t
		WHERE most_recent = 1;
		stop_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Time taken: %s', stop_time-start_time;
		RAISE NOTICE '--------------------------';
		RAISE NOTICE ' ';

		-- silver.crm_prd_info
		RAISE NOTICE '--------------------------';
		RAISE NOTICE 'silver.crm_prd_info';
		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating table...';
		TRUNCATE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info(	prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
			SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
			TRIM(prd_nm) AS prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'N/A'
			END AS prd_line, 
			prd_start_dt,
			LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
		FROM bronze.crm_prd_info
		ORDER BY prd_key;
		stop_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Time taken: %s', stop_time-start_time;
		RAISE NOTICE '--------------------------';
		RAISE NOTICE ' ';

		-- silver.crm_sales_details
		RAISE NOTICE '--------------------------';
		RAISE NOTICE 'silver.crm_sales_details';
		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating table...';
		TRUNCATE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,sls_due_dt,sls_sales,sls_price,sls_quantity
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			-- sales
			CASE  
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity*ABS(sls_price)
					THEN sls_quantity*sls_price
				ELSE sls_sales
			END AS sls_sales,
			-- price
			CASE 
				WHEN sls_price IS NULL OR sls_price <=0
					THEN sls_sales/NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price, 
			-- quantity
			CASE 
				WHEN sls_quantity IS NULL OR sls_quantity <= 0
					THEN sls_sales/NULLIF(sls_price, 0)
				ELSE sls_quantity
			END AS sls_quantity
		FROM bronze.crm_sales_details;
		stop_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Time taken: %s', stop_time-start_time;
		RAISE NOTICE '--------------------------';
		RAISE NOTICE ' ';

		RAISE NOTICE '============================';
		RAISE NOTICE 'LOADING ERP TABLES';
		RAISE NOTICE '============================';
		-- silver.erp_cust_az12
		RAISE NOTICE '--------------------------';
		RAISE NOTICE 'silver.erp_cust_az12';
		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating table...';
		TRUNCATE silver.erp_cust_az12;
		INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
		SELECT 
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
				ELSE cid
			END cid,
			CASE WHEN bdate > CURRENT_DATE THEN NULL
				 ELSE bdate
			END bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'N/A'
			END AS gen
		FROM bronze.erp_cust_az12;
		stop_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Time taken: %s', stop_time-start_time;
		RAISE NOTICE '--------------------------';

		-- silver.erp_loc_a101
		RAISE NOTICE '--------------------------';
		RAISE NOTICE 'silver.erp_loc_a101';
		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating table...';
		TRUNCATE silver.erp_loc_a101;
		INSERT INTO silver.erp_loc_a101(cid, cntry)
		SELECT
			REPLACE(cid, '-', '') AS cid, -- remove '-' from cid
			CASE
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) = 'UK' THEN 'United Kingdom'
				WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
				ELSE TRIM(cntry)
			END AS cntry -- standardize data, set missing, null values to 'n/a', remove unwanted spaces
		FROM bronze.erp_loc_a101;
		stop_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Time taken: %s', stop_time-start_time;
		RAISE NOTICE '--------------------------';
		RAISE NOTICE ' ';

		-- silver.erp_px_cat_g1v2
		RAISE NOTICE '--------------------------';
		RAISE NOTICE 'silver.erp_px_cat_g1v2';
		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Truncating table...';
		TRUNCATE silver.erp_px_cat_g1v2;
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		SELECT 
			TRIM(id) AS id, 
			TRIM(cat) AS cat,
			TRIM(subcat) AS subcat,
			TRIM(maintenance) AS maintenance
		FROM bronze.erp_px_cat_g1v2;
		stop_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Time taken: %s', stop_time-start_time;
		RAISE NOTICE '--------------------------';
		RAISE NOTICE ' ';


		batch_stoptime := CURRENT_TIMESTAMP;
		RAISE NOTICE 'Loading silver layer complete';
		RAISE NOTICE 'Time taken: %s', batch_stoptime - batch_starttime;
	EXCEPTION
		WHEN OTHERS THEN 
			RAISE NOTICE 'Error: %', SQLERRM;
	END;
END;
$$;