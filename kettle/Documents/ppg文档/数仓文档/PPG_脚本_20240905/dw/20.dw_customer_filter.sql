/*目标表：fine_dw.dw_customer_filter
  来源表：
fine_ods.ods_customer_filter_df 

更新方式：全量更新
*/

select
 REPLACE(customer_code,'CN','')AS customer_code,
'fine_ods.ods_customer_filter_df' as data_resource,
SYSDATE() as etl_time
from fine_ods.ods_customer_filter_df