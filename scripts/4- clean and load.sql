--CLEAN AND LOAD DATA (crm_cust_info)

SELECT 
	cst_id,
	COUNT(*)  
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL

-- select only the latest non repeated data
SELECT * 
FROM (
SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_data DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last =1

-- check for unwanted spaces
-- Expectation: No results
SELECT cst_first_name
FROM bronze.crm_cust_info
WHERE cst_first_name !=TRIM(cst_first_name)

-- all spaces removed
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_first_name) AS cst_firstname,
	TRIM(cst_last_name) AS cst_lastname,
	cst_materials_status,
	CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
	WHEN upper(cst_gndr) = 'M' THEN 'Male'
	ELSE 'n/a'
	END cst_gndr,
	cst_create_data
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_data DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1

INSERT INTO silver.crm_cust_info (
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
			TRIM(cst_first_name) AS cst_firstname,
			TRIM(cst_last_name) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_materials_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_materials_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_data
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_data DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1;

		SELECT * FROM silver.crm_cust_info