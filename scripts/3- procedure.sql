--  a PROCEDURE is like a function so that we don't have to do all that again

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		SET @start_time = GETDATE();
		PRINT '=================================================';
		PRINT '-------------------------------------------------';
		PRINT '             Loading the Bronze Layer';
		PRINT '-------------------------------------------------';
		-- SO THAT DATA IS NOT FED TWICE
		TRUNCATE TABLE bronze.crm_cust_info;
		TRUNCATE TABLE bronze.crm_prd_info;
		TRUNCATE TABLE bronze.crm_sales_details;
		TRUNCATE TABLE bronze.erp_cust_az12;
		TRUNCATE TABLE bronze.erp_loc_a101;
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT 'Loading the bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\placements\ZS\warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' 
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		PRINT '-------------------------------------------------';
		PRINT 'Loading the bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\placements\ZS\warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' 
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT '-------------------------------------------------';
		PRINT 'Loading the bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\placements\ZS\warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' 
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT '-------------------------------------------------';
		PRINT 'Loading the bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\placements\ZS\warehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' 
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT '-------------------------------------------------';
		PRINT 'Loading the bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\placements\ZS\warehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' 
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT '-------------------------------------------------';
		PRINT 'Loading the bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\placements\ZS\warehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' 
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration = '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)
	END TRY
	
	BEGIN CATCH
		 PRINT 'ERROR! in loading the bronze layer'
	END CATCH

END

