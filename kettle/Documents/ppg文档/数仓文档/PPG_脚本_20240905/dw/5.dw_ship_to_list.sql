/*
目标表：fine_dw.dw_ship_to_list
来源表：
fine_ods.ods_ship_to_list_df

更新方式：全量更新

*/

SELECT   
    REPLACE(customer_code, 'CN', '') AS customer_code,  
    customer_name,  
    ship_to_code,  
    upper(district) as district,  
    upper(channel) as channel,  
    DATE_FORMAT(starting_date, '%Y%m%d') AS starting_date,  
    CASE   
        WHEN ending_date IS NOT NULL THEN DATE_FORMAT(ending_date, '%Y%m%d')  
        ELSE date_format(concat(year(CURDATE()),'1231'),'%Y%m%d')
    END AS ending_date,  
    'fine_ods.ods_ship_to_list_df' as data_resource,  
    SYSDATE() as etl_time
FROM fine_ods.ods_ship_to_list_df
