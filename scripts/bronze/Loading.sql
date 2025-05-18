/*
 =====================================================================================
 Stored Procedure: Load Bronze Layer (source -> Bronze
 =====================================================================================

	ScriptPurpose:
		This stored procedure loads data into the 'bronze' schema from external CSV files.
		It performs the following actions:
			- Truncates the bronze tables before loading the data.
			-Uses the 'BULK INSERT' command to load data from csv files to bronze tables.
 =====================================================================================
*/

EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT '====================================';
		PRINT 'Loading Bronze Layer';
		PRINT '====================================';
		
	    PRINT '------------------------------------';
		PRINT 'Loading CRM tables';
	    PRINT '------------------------------------';
	    
	    SET @start_time = GETDATE();
	    PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	    
	    PRINT '>> Inserting Data Into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM '/var/opt/mssql/cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	    SET @end_time = GETDATE();
	    PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	    
	    PRINT '====================================';
	    
	    
		SET @start_time = GETDATE();
	    PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
	    
	    PRINT '>> Inserting Data Into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM '/var/opt/mssql/prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	    SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	    PRINT '========================================';
	    
	    SET @start_time = GETDATE();
	    PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
	    
	    PRINT '>> Inserting Data Into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM '/var/opt/mssql/sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	    
	    PRINT '------------------------------------';
		PRINT 'Loading ERP tables';
	    PRINT '------------------------------------';
		
	    SET @start_time = GETDATE();
	    PRINT '>> Truncating Table: bronze.er_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;
	    
	    PRINT '>> Inserting Data Into Table: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM '/var/opt/mssql/cust_az12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	    
	    PRINT '=========================================';
	    
	    SET @start_time = GETDATE();
	    PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	    
	    PRINT '>> Inserting Data Into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM '/var/opt/mssql/loc_a101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	    
	    PRINT '============================================';
	    
	    SET @start_time = GETDATE();
	    PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	    
	    PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM '/var/opt/mssql/px_cat_g1v2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	    SET @end_time = GETDATE();
	    PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds;'
    END TRY
    BEGIN CATCH
    	PRINT '===========================================';
    	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	    PRINT 'Error Message' + ERROR_MESSAGE();
	    PRINT 'ERROR Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
	    PRINT '===========================================';
    END CATCH
END 
