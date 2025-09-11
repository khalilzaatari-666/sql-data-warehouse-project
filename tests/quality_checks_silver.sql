/*
===============================================================================
Quality Checks
===============================================================================
*/

-- ====================================================================
-- Checking "silver.crm_cust_info"
-- ====================================================================
-- Check for nulls or duplicates in primary key
-- Expectation: no eesults
select cst_id, COUNT(*) 
from silver.crm_cust_info
group by cst_id
having COUNT(*) > 1 or cst_id is null;

-- Check for unwanted spaces
-- Expectation: no results
select cst_key
from silver.crm_cust_info
where cst_key != trim(cst_key);

-- Data standardization and consistency
select distinct cst_gndr
from silver.crm_cust_info
  
select distinct cst_marital_status 
from silver.crm_cust_info;

-- ====================================================================
-- Checking "silver.crm_prd_info"
-- ====================================================================
-- Check for nulls or duplicates in primary key
-- Expectation: no results
select prd_id, COUNT(*) 
from silver.crm_prd_info
group by prd_id
having COUNT(*) > 1 or prd_id is null;

-- Check for unwanted spaces
-- Expectation: no results
select prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm);

-- Check for nulls or negative values in cost
-- Expectation: no eesults
select prd_cost 
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data standardization and consistency
select distinct prd_line
from silver.crm_prd_info;

-- Check for invalid date orders (Start Date > End Date)
-- Expectation: no results
select * 
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking "silver.crm_sales_details"
-- ====================================================================
-- Check for invalid dates
-- Expectation: no invalid dates
select nullif(sls_due_dt, 0) as sls_due_dt 
from bronze.crm_sales_details
where sls_due_dt <= 0 
    or len(sls_due_dt) != 8 
    or sls_due_dt > 20500101 
    or sls_due_dt < 19000101;

-- Check for invalid date orders (Order Date > Shipping/Due Dates)
-- Expectation: no results
select * 
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt 
   or sls_order_dt > sls_due_dt;

-- Check data consistency: Sales = Quantity * Price
-- Expectation: no results
select distinct sls_sales, sls_quantity, sls_price 
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
   or sls_sales is null 
   or sls_quantity is null 
   or sls_price is null
   or sls_sales <= 0 
   or sls_quantity <= 0 
   or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking "silver.erp_cust_az12"
-- ====================================================================
-- Identify out-of-range dates
-- Expectation: birthdates between 1900-01-01 and today
select distinct bdate 
from silver.erp_cust_az12
where bdate < '1925-01-01' 
   or bdate > GETDATE();

-- Data standardization and consistency
select distinct gen 
from silver.erp_cust_az12;

-- ====================================================================
-- Checking "silver.erp_loc_a101"
-- ====================================================================
-- Data standardization and consistency
select distinct cntry 
from silver.erp_loc_a101
order by cntry;

-- ====================================================================
-- Checking "silver.erp_px_cat_g1v2"
-- ====================================================================
-- Check for unwanted spaces
-- Expectation: no results
select * 
from silver.erp_px_cat_g1v2
where cat != TRIM(cat) 
   or subcat != TRIM(subcat) 
   or maintenance != TRIM(maintenance);

-- Data standardization and consistency
select distinct maintenance 
from silver.erp_px_cat_g1v2;
