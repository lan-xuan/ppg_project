
select  
upper(channel) as channel 
,proj_name
,c_rebate
,rebate
,adjusted_fee
,adjusted_fee_desc
,adjusted_date
,adjusted_month
,now() as etl_time
,'fine_ods.ods_adjusted_fee_df ' as data_resource
from fine_ods.ods_adjusted_fee_df 
