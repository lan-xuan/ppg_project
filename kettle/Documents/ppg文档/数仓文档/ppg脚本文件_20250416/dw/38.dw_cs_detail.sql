select 'MM' as channel,a.sales_month,'是'as is_flag,b.proj_name,a.customer_code as vendor_code,a.customer_name as vendor_name,
a.item_code,a.category,a.category_brand,a.category_product_type,a.sales_value,a.sales_qty
      ,now() as etl_time 
      ,'fine_dw.dw_transaction_detail_report/fine_dw.dw_item_mapping_cb' as data_resource
      ,STR_TO_DATE(CONCAT(a.sales_month ,'01') , '%Y%m%d')  as report_date
      
from 
(select invoice_no,customer_code,customer_name,item_code,category,category_brand,category_product_type,sales_month,sales_value,sales_qty 
from fine_dw.dw_transaction_detail_report dtdr 
where brand_name ='CENTRAL SUPPLY' 
AND channel='DISTRIBUTOR'
AND   SUBSTRING(sales_month,1,6) =  ${mysql_yesterday_l_month} 

) a left join 
(SELECT item_code,
case when proj_name like '%,%' then substring_index(proj_name,',',1) else proj_name end as proj_name
FROM (select item_code,GROUP_CONCAT(distinct proj_name) as proj_name  from (select distinct item_code,case when proj_name in ('GEELY LYNK&CO','GEELY ZEEKR') then 'DF VOYAH'
else proj_name end AS proj_name
from fine_dw.dw_item_mapping_cb dimc where channel='MM') c group by item_code) d) b 
on a.item_code=b.item_code
