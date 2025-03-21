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
	FROM bronze.erp_loc_a101