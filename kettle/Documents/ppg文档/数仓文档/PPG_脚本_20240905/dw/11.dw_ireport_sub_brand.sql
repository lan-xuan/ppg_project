/*
目标表：fine_dw.dw_ireport_sub_brand
来源表：
fine_ods.ods_ireport_sub_brand_df

更新方式：全量更新

*/


select 
DISTINCT
UPPER(agreement_target_name) as agreement_target_name,
UPPER(sales_target_name) as sales_target_name,
UPPER(report_brand_group) as report_brand_group,
UPPER(report_brand_name) as report_brand_name,
UPPER(category_brand) as category_brand,
UPPER(item_code) as item_code,
UPPER(category) as category,
'fine_ods.ods_ireport_sub_brand_df' as data_resource,
 SYSDATE() as etl_time
 from fine_ods.ods_ireport_sub_brand_df	