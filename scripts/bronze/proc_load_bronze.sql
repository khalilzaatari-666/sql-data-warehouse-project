/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================

Example usage: EXEC bronze.load_bronze

===============================================================================
*/

create or alter PROCEDURE bronze.load_bronze as 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_b DATETIME, @end_time_b DATETIME; 
    BEGIN TRY
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables'
        PRINT '------------------------------------------------';
        set @start_time = GETDATE();
        set @start_time_b = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info'
        TRUNCATE table bronze.crm_cust_info; 
        PRINT '>> Inserting Data Into: bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        from '/var/opt/mssql/data/datasets/source_crm/cust_info.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        print '------------------------------------------------'

        set @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info'
        TRUNCATE table bronze.crm_prd_info; 
        PRINT '>> Inserting Data Into: bronze.crm_prd_info'
        BULK INSERT bronze.crm_prd_info
        from '/var/opt/mssql/data/datasets/source_crm/prd_info.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        print '------------------------------------------------'

        set @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details'
        TRUNCATE table bronze.crm_sales_details; 
        PRINT '>> Inserting Data Into: bronze.crm_sales_details'
        BULK INSERT bronze.crm_sales_details
        from '/var/opt/mssql/data/datasets/source_crm/sales_details.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';

        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables'
        PRINT '------------------------------------------------';
        set @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12'
        TRUNCATE table bronze.erp_cust_az12; 
        PRINT '>> Inserting Data Into: bronze.erp_cust_az12'
        BULK INSERT bronze.erp_cust_az12
        from '/var/opt/mssql/data/datasets/source_erp/CUST_AZ12.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        print '------------------------------------------------'

        set @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101'
        TRUNCATE table bronze.erp_loc_a101; 
        PRINT '>> Inserting Data Into: bronze.erp_loc_a101'
        BULK INSERT bronze.erp_loc_a101
        from '/var/opt/mssql/data/datasets/source_erp/LOC_A101.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        print '------------------------------------------------'

        set @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
        TRUNCATE table bronze.erp_px_cat_g1v2; 
        PRINT '>> Inserting Data Into: bronze.erp_cat_g1v2'
        BULK INSERT bronze.erp_px_cat_g1v2
        from '/var/opt/mssql/data/datasets/source_erp/PX_CAT_G1V2.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = GETDATE();
        set @end_time_b = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------'
        PRINT '>> Bronze Layer Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time_b, @end_time_b) as NVARCHAR) + ' seconds'
    END TRY
    BEGIN CATCH
        PRINT '================================================'
        print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        print 'Error Message' + ERROR_MESSAGE();
        print 'Error Message' + Cast (ERROR_NUMBER() as NVARCHAR);
        print 'Error Message' + Cast (ERROR_STATE() as NVARCHAR);
        PRINT '================================================'
    END CATCH
END
