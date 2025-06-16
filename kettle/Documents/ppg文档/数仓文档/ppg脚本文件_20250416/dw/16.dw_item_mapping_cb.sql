/*
目标表：fine_dw.dw_item_mapping_cb
来源表：
fine_ods.ods_item_mapping_cb_df

更新方式：全量更新

*/

SELECT 
upper(item_code) as item_code,
upper(item_code_ppg) as item_code_ppg,
upper(channel) as channel,
upper(proj_name) as proj_name,
upper(category) as category,
upper(category_brand) as category_brand,
upper(category_product_type) as category_product_type,
upper(oracle_brand) as oracle_brand,
upper(oracle_sub_band) as oracle_sub_band,
upper(oracle_product_type) as oracle_product_type,
upper(product_family) as product_family,
cb_price,
bs_price,
service_type,
warehouse_code,
service_fee,
date_format(cb_starting_date,'%Y%m%d') as cb_starting_date,
date_format(cb_ending_date,'%Y%m%d') as cb_ending_date,
date_format(bs_starting_date,'%Y%m%d') as bs_starting_date,
date_format(bs_ending_date,'%Y%m%d') as bs_ending_date,
date_format(service_fee_starting_date,'%Y%m%d') as service_fee_starting_date,
date_format(service_fee_ending_date,'%Y%m%d') as service_fee_ending_date,
rebate_rate,
service_rate,
commision_fee_rate,
reward_rate,
date_format(rebate_rate_starting_date,'%Y%m%d') as rebate_rate_starting_date,
date_format(rebate_rate_ending_date,'%Y%m%d') as rebate_rate_ending_date,
date_format(service_rate_starting_date,'%Y%m%d') as service_rate_starting_date,
date_format(service_rate_ending_date,'%Y%m%d') as service_rate_ending_date,
date_format(commision_fee_starting_date,'%Y%m%d') as commision_fee_starting_date,
date_format(commision_fee_ending_date,'%Y%m%d') as commision_fee_ending_date,
date_format(reward_rate_starting_date,'%Y%m%d') as reward_rate_starting_date,
date_format(reward_rate_ending_date,'%Y%m%d') as reward_rate_ending_date,
date_format(update_time,'%Y%m%d') as update_time,
update_user,
'fine_ods.ods_item_mapping_cb_df' as data_resource,
SYSDATE() as etl_time
FROM  fine_ods.ods_item_mapping_cb_df