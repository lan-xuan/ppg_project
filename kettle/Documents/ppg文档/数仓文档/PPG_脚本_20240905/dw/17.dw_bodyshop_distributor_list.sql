/*目标表：fine_dw.dw_bodyshop_distributor_list
  来源表：
fine_ods.ods_bodyshop_distributor_list_df 

更新方式：全量更新
*/
with temp_001 as (
select 
distinct 
replace(customer_code,'CN','') AS customer_code,
ship_to_code,
vendor_code,
null warehouse_code,
is_default
from 
fine_ods.ods_bodyshop_distributor_list_df
where customer_name = '深圳比亚迪'
UNION all
select 
DISTINCT
replace(customer_code,'CN','') AS customer_code,
null ship_to_code,
vendor_code,
warehouse_code,
is_default
from 
fine_ods.ods_bodyshop_distributor_list_df
where customer_name <> '深圳比亚迪'

) 

select 
replace(customer_code,'CN','') AS customer_code,
ship_to_code,
vendor_code,
null warehouse_code,
is_default,
'fine_ods.ods_bodyshop_distributor_list_df' as data_resource,
SYSDATE() as etl_time
from temp_001
