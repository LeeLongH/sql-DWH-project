-- Check for invalid dates
SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) !=8
OR sls_order_dt > 20250921
OR sls_order_dt < 19010921

-- Check for invalid date
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- Check data consistency: sales = quantitiy * price > 0
SELECT DISTINCT 
sls_sales AS old_sls_sales, 
sls_quantity, 
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
	THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM silver.crm_sales_details

WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0 
ORDER BY sls_sales, sls_quantity, sls_price

SELECT DISTINCT 
sls_quantity, 
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details




-- Check unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintance != TRIM(maintance)

SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2











-- Check for Nulls or duplicates in primary key
-- Expectation: No results
SELECT cst_id FROM silver.crm_cust_info
GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id is NULL


-- Find primary key repeatitions
-- Find nulls
SELECT * FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info3
)t WHERE flag_last = 1

-- Check unwanted spaces
-- first names
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- last names
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- gndr
SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

-- Expectation: No results
SELECT cst_key
FROM bronze.crm_cust_info3
WHERE cst_key != TRIM(cst_key)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

-- Insert into Silver
INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'n/a'
    END,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         ELSE 'n/a'
    END,
    cst_create_date   -- 👈 added comma above
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    FROM bronze.crm_cust_info3
) t 
WHERE flag_last = 1;







SELECT *
FROM silver.crm_cust_info


-- prd nm
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check Invalid Date orders
SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dat < prd_start_dt








