/*
目标表：fine_dw.dw_item_flag
来源表：
fine_ods.ods_item_flag_df

更新方式：全量更新

*/

select 
 item_code
,is_flag
,'fine_ods.ods_item_flag' as data_resource
, SYSDATE() as etl_time
 from fine_ods.ods_item_flag_df