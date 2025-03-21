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
FROM bronze.crm_sales_details