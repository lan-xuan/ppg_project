/*目标表：fine_dw.dw_item_cost_detail
  来源表：
fine_ods.ods_item_cost_detail_df 

更新方式：全量更新
*/
 
select 
update_date,
item_code,
uom,
uom_kg,
uom_ltr,
'fine_ods.ods_item_cost_detail_df' as data_resource,
SYSDATE() as etl_time
from fine_ods.ods_item_cost_detail_df