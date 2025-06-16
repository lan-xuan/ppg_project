
/*
目标：fine_dw.dw_cs_detail

来源表：fine_dw.dw_transaction_detail_report   
        fine_dw.dw_item_mapping_cb 

更新方式：增量更新 month,去除当月数据

*/
with temp_001 as (
select invoice_no
       ,customer_code
       ,customer_name
       ,item_code
       ,category
       ,category_brand
       ,category_product_type
       ,sales_month
       ,sales_value
       ,sales_qty 
      from fine_dw.dw_transaction_detail_report dtdr 
      where brand_name ='CENTRAL SUPPLY'
      AND channel='DISTRIBUTOR'

     --  and  SUBSTRING(sales_month,1,4) IN ('2024','2023','2022')
  and  SUBSTRING(sales_month,1,6) =  ${mysql_yesterday_l_month} 
    -- and  SUBSTRING(sales_month,1,6) = '202407'
      
),

temp_002 as (
select distinct      item_code
                    ,case when proj_name in ('GEELY LYNK&CO','GEELY ZEEKR') then 'DF VOYAH' else proj_name end as proj_name 
                    ,channel
    from fine_dw.dw_item_mapping_cb 
)


select b.channel
      ,a.sales_month
      ,'是'as is_flag
      ,b.proj_name
      ,a.customer_code as vendor_code
      ,a.customer_name as vendor_name
      ,a.item_code
      ,a.category
      ,a.category_brand
      ,a.category_product_type
      ,a.sales_value
      ,a.sales_qty
      ,now() as etl_time 
      ,'fine_dw.dw_transaction_detail_report/fine_dw.dw_item_mapping_cb' as data_resource
      ,STR_TO_DATE(CONCAT(a.sales_month ,'01') , '%Y%m%d')  as report_date
from temp_001 a 
left join temp_002 b 
on a.item_code = b.item_code