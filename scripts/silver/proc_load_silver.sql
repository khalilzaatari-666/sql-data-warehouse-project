/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================

Example usage: EXEC silver.load_silver

===============================================================================
*/

create or alter procedure silver.load_silver as
begin
  declare @start_time DATETIME, @end_time DATETIME, @start_time_b DATETIME, @end_time_b DATETIME;
	begin try
		print '================================================';
		print 'Loading Silver Layer';
		print '================================================';
		set @start_time = GETDATE();
		set @start_time_b = GETDATE();
		print '>> Truncating table "silver.crm_cust_info"'
		truncate table silver.crm_cust_info;
		print '>> Inserting data into "silver.crm_cust_info"'
		insert into silver.crm_cust_info(
			cst_id, 
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		select cst_id, cst_key, trim(cst_lastname) as cst_firstname, trim(cst_firstname) as cst_firstname, 
		case 
			when upper(trim(csT_marital_status)) = 'S' then 'Single'
			when upper(trim(cst_marital_status)) = 'M' then 'Married'
			else 'N/A'
		end cst_gndr,   -- normalize marital status values to readable format
		case 
			when upper(trim(cst_gndr)) = 'F' then 'Female'
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'N/A'
		end cst_gndr,   -- normalize gender values to readable format
		cst_create_date
		from (
			select 
				*, 
				ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		)t 
		where flag_last = 1
		set @end_time = GETDATE();
		print '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		print '------------------------------------------------'

		set @start_time = GETDATE();
		print '>> Truncating table "silver.crm_prd_info"'
		truncate table silver.crm_prd_info;
		print '>> Truncating data into "silver.crm_prd_info"'
		insert into silver.crm_prd_info(
			prd_id,	 
			cat_id,	 
			prd_key,		 
			prd_nm,	 
			prd_cost,	 
			prd_line,
			prd_start_dt, 
			prd_end_dt
		)
		select 
		prd_id,
		replace(substring(prd_key, 1, 5), '-', '_') as cat_id,   -- extract category id
		SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,			 -- extract product key
		prd_nm,
		isnull(prd_cost, 0) as prd_cost,
		case UPPER(trim(prd_line))
			 when 'M' then 'Mountain'
			 when 'R' then 'Road'
			 when 'S' then 'Other Sales'
			 when 'T' then 'Touring'
			 else 'N/A'
		end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(dateadd(day, -1, lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as date) as prd_end_dt
		from bronze.crm_prd_info
		set @end_time = GETDATE();
		print '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		print '------------------------------------------------'

		set @start_time = GETDATE();
		print '>> Truncating table "silver.crm_sales_details"'
		truncate table silver.crm_sales_details;
		print '>> Inserting data into "silver.crm_sales_details"'
		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)

		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
			else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
			else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
		case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
			 else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <= 0
				then sls_sales / nullif(sls_quantity,0)
			 else sls_price
		end as sls_price
		from bronze.crm_sales_details
		set @end_time = GETDATE();
		print '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		print '------------------------------------------------'

		set @start_time = GETDATE();
		print '>> Truncating table "silver.erp_cust_az12"'
		truncate table silver.erp_cust_az12;
		print '>> Inserting data into "silver.erp_cust_az12"'
		insert into silver.erp_cust_az12 (cid, bdate, gen)
		select 
		case when cid like 'NAS%' then substring(cid, 4, len(cid))  -- remove the 'NAS' prefix
			 else cid
		end cid,
		case when bdate > GETDATE() then null   -- set future birthdates to NULL
			 else bdate
		end as bdate,
		case when upper(trim(gen)) in ('F', 'MALE') then 'Male'
			 when upper(trim(gen)) in ('M', 'FEMALE') then 'Female'
			 else 'N/A'
		end as gen  -- normalize gender values
		from bronze.erp_cust_az12
		set @end_time = GETDATE();
		print '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		print '------------------------------------------------'

		set @start_time = GETDATE();
		print '>> Truncating table "silver.erp_loc_a101"'
		truncate table silver.erp_loc_a101;
		print '>> Inserting data into "silver.erp_loc_a101"'
		insert into silver.erp_loc_a101 (cid, cntry)
		select 
		replace(cid, '-', ''),
		case when trim(cntry) = 'DE' then 'Germany'
			 when trim(cntry) in ('US', 'USA') then 'United States'
			 when trim(cntry) = '' or cntry is null then 'N/A'
			 else cntry
		end as cntry
		from bronze.erp_loc_a101
		set @end_time = GETDATE();
		print '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		print '------------------------------------------------'

		set @start_time = GETDATE()
		print '>> Truncating table "silver.erp_px_cat_g1v2"'
		truncate table silver.erp_px_cat_g1v2;
		print '>> Inserting data into "silver.erp_px_cat_g1v2"'
		insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		set @end_time = GETDATE();
		set @end_time_b = GETDATE()
		print '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		print '------------------------------------------------'
		print '>> Silver Layer Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time_b, @end_time_b) as NVARCHAR) + ' seconds'
	end try
	begin catch
		print '================================================'
        print 'ERROR OCCURED DURING LOADING SILVER LAYER'
        print 'Error Message' + ERROR_MESSAGE();
        print 'Error Message' + Cast (ERROR_NUMBER() as NVARCHAR);
        print 'Error Message' + Cast (ERROR_STATE() as NVARCHAR);
        print '================================================'
	end catch
end
