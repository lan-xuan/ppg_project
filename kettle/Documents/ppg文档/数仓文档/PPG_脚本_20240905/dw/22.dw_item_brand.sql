
/*
目标表：fine_dw.dw_item_brand
来源表：
fine_ods.ods_item_mapping_cb_df
fine_ods.ods_item_mapping_distributor_df

更新方式：全量更新

*/


SELECT DISTINCT item_code,category,category_brand,category_product_type,update_time,update_user,
     'ods_item_mapping_cb_df/ods_item_mapping_distributor_df' AS data_resource,
     SYSDATE() as etl_time
		FROM(
SELECT 
    UPPER(item_code) as item_code,
    UPPER(item_code_ppg) as item_code_ppg,
    UPPER(channel) as channel,
    proj_name,
    UPPER(category) as category,
    UPPER(category_brand) as category_brand,
    UPPER(category_product_type) as category_product_type,
    update_time,
    update_user
   -- 'ods_item_mapping_cb_df' AS data_resource,
    -- SYSDATE() as etl_time
FROM
    fine_ods.ods_item_mapping_cb_df
-- 		WHERE item_code = 'T400.MB/2L-C3'

union
SELECT 
    UPPER(item_code) as item_code,
	null as item_code_ppg,
    null as channel,
    null as proj_name,
    UPPER(category) as category,
    UPPER(category_brand) as category_brand,
    UPPER(category_product_type) as category_product_type,
    update_time,
    update_user
   -- 'ods_item_mapping_distributor_df' AS data_resource,
    -- SYSDATE() as etl_time
FROM
    fine_ods.ods_item_mapping_distributor_df
-- 		WHERE item_code = 'T400.MB/2L-C3'

	) s		