
/*
目标表：fine_dw.dw_order_filter
来源表：
fine_ods.ods_order_filter_df

更新方式：全量更新

*/

select 
order_type
,order_type_name
,'fine_ods.ods_order_filter_df' as data_resource
, SYSDATE() as etl_time
from fine_ods.ods_order_filter_df