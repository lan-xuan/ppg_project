/*
目标表：fine_dw.dw_bodyshop_distributor_byd
来源表：
fine_ods.ods_bodyshop_distributor_byd_df

更新方式：全量更新

*/

select 
 vendor_code	  -- 所属服务商编码	
,vendor_name	-- 所属服务商名称	
,customer_code	-- 所属主机厂编码
,customer_name	-- 所属主机厂名称	
,ship_to_code	-- Shipto	
,'fine_ods.ods_bodyshop_distributor_byd_df' as data_resource
, SYSDATE() as etl_time
from fine_ods.ods_bodyshop_distributor_byd_df