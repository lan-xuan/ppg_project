/*目标表：fine_dw.dw_organization_info
  来源表：
fine_ods.ods_organization_info_df 

更新方式：全量更新
*/

select 
role_type	 -- 角色类型
,employee_id	-- 员工号
,full_name	-- 全名
,worker_manager	-- 经理
,manager_of_manager1	-- 经理1
,manager_of_manager2	-- 经理2
,manager_of_manager3	-- 经理3
,work_country	-- 国家
,region	-- 地区
,department	-- 部门
,email	-- email
,'fine_ods.ods_organization_info_df' as data_resource
,SYSDATE() as etl_time
from fine_ods.ods_organization_info_df