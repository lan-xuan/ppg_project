/*
目标表：fine_dw.dw_bodyshop_distributor_all
来源表：

fine_ods.ods_bodyshop_distributor_list_df
fine_ods.ods_bodyshop_distributor_byd_df
更新方式：全量更新


*/
WITH RankedData AS (
    SELECT 
        ship_to_code,
        REPLACE(customer_code, 'CN', '') as customer_code,
        -- vendor_code,
        REPLACE(vendor_code, 'CN', '') as vendor_code,
        is_default,
        ROW_NUMBER() OVER (
            PARTITION BY ship_to_code
            ORDER BY CASE WHEN is_default = 1 THEN 1 ELSE 2 END
        ) AS rn
    FROM 
        fine_ods.ods_bodyshop_distributor_list_df
    WHERE 
        REPLACE(customer_code, 'CN', '') IN ('207196', '210481','183492')
-- 				and  ship_to_code = '499239'
)
select 
DISTINCT
ship_to_code,
warehouse_code,
customer_code,
vendor_code,
'fine_ods.ods_bodyshop_distributor_list_df/fine_ods.ods_bodyshop_distributor_byd_df' as data_resource,
 SYSDATE() as etl_time
from 
(
select DISTINCT null as ship_to_code,warehouse_code,null customer_code,REPLACE(vendor_code, 'CN', '') as vendor_code from fine_ods.ods_bodyshop_distributor_list_df where 1=1  and REPLACE(customer_code, 'CN', '') not in ('207196','210481','183492' )-- others
UNION ALL
select DISTINCT ship_to_code,null as warehouse_code,customer_code,vendor_code from RankedData WHERE  rn = 1 -- byd
UNION ALL
select DISTINCT ship_to_code,null,REPLACE(customer_code, 'CN', '') as customer_code,REPLACE(vendor_code, 'CN', '') as vendor_code from fine_ods.ods_bodyshop_distributor_byd_df where  REPLACE(customer_code, 'CN', '') in ('207196','210481','183492' ) -- byd link

union all
select DISTINCT ship_to_code,warehouse_code,REPLACE(customer_code, 'CN', '') as customer_code,REPLACE(vendor_code, 'CN', '') as vendor_code 
from fine_ods.ods_bodyshop_distributor_list_excel_df 
) s
where 1=1