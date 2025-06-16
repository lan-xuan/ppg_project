
select  
upper(channel) as channel 
,proj_name
,c_rebate
,rebate
,adjusted_fee
,adjusted_fee_desc
,adjusted_date
-- ,adjusted_month
,substr(adjusted_date,1,6) AS adjusted_month
,now() as etl_time
,'fine_ods.ods_adjusted_fee_df ' as data_resource
,adjusted_service_fee
,adjusted_commision_fee
from fine_ods.ods_adjusted_fee_df 
