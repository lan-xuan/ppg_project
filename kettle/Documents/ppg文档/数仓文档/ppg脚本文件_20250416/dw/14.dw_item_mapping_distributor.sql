
/*
目标表：fine_dw.dw_item_mapping_distributor
来源表：
fine_ods.ods_item_mapping_distributor_df

更新方式：全量更新

*/

SELECT 
item_code,
category,
category_brand,
category_product_type,
oracle_brand,
oracle_sub_band,
oracle_product_type,
chinese_description,
product_family,
price,
distributor_price,
starting_date ,
ending_date ,
update_time ,
update_user,
'fine_ods.ods_item_mapping_distributor_df' as data_resource,
 SYSDATE() as etl_time
FROM  fine_ods.ods_item_mapping_distributor_df