/*
目标表：fine_dw.dw_rebate_import_query
来源表：
fine_ods.ods_rebate_import_query
fine_dw.dw_cs_relationship_info
更新方式：增量更新
更新字段：sales_year
*/

-- 处理sales_year
with temp_001 as (
select  
       rebateno
      ,u_customer_code as vendor_code
       ,vendor_name
       ,rebatetype
       ,rebatestate
       ,frozenstate
       ,lockstate
       ,entry
       ,rebatemoney
       ,decmoneysum
       ,remainomney
       ,frozenmoney
       ,rebatedate
       ,opeempname
       ,opedate
       ,sales_month
       ,available_amount
       ,rebatechannel
       ,rebateitem
       ,rebateperiod
       ,rebateclassification
       ,data_resource
       ,etl_time
       ,SUBSTRING(A.rebateperiod,1,4) as sales_year
       -- ,A.vendor_code
       -- ,B.u_customer_code
from (select * 
      from fine_ods.ods_rebate_import_query A 
      where SUBSTRING(A.rebateperiod,1,4) >='2022'
      ) A
      LEFT JOIN 
      (select DISTINCT customer_code,u_customer_code
        from fine_dw.dw_customer_master_list 
        ) B
     on upper(REPLACE(A.vendor_code, 'CN', '')) = upper(B.customer_code)

),


-- 20240823 修改逻辑使用最新的配置表
temp_dw_cs_relationship_info as (
select * 
 from fine_dw.dw_cs_relationship_info
 where  s_year = (select max(s_year) 
                   from fine_dw.dw_cs_relationship_info ) 
),

-- 1.rebatechannel =‘PREMIUM-DISTRIBUTOR’
temp_002 as (
select  A.*
       ,CONCAT(COALESCE(B.sales_person_id,'NULL_DISTRIBUTOR'),',')  as sales_person_id
from (select *
         from temp_001 
         where rebatechannel ='PREMIUM-DISTRIBUTOR') A 
LEFT join
     (select    customer_code,replace(GROUP_CONCAT(sales_person_id),',',',|' )  as sales_person_id
         from temp_dw_cs_relationship_info
          where channel = 'DISTRIBUTOR' 
          group by customer_code
          
          
          ) B
on REPLACE(A.vendor_code, 'CN', '')  = B.customer_code 
   
),


-- 2.rebatechannel =‘PREMIUM-MM’
temp_003 as (
select  A.*
       ,CONCAT(COALESCE(B.sales_person_id,'NULL_MM'),',|',COALESCE(C.sales_person_id,'NULL_DISTRIBUTOR'),',')  as sales_person_id
from (select *
         from temp_001 
         where rebatechannel ='PREMIUM-MM') A 
LEFT join
     (select proj_name,replace(GROUP_CONCAT(sales_person_id),',',',|' ) as sales_person_id
         from temp_dw_cs_relationship_info
          where channel = 'MM' 
          group by proj_name
          ) B
on A.rebateitem  = B.proj_name 
LEFT join
     (select    customer_code,replace(GROUP_CONCAT(sales_person_id),',',',|' )  as sales_person_id
         from temp_dw_cs_relationship_info
          where channel = 'DISTRIBUTOR'
          group by customer_code
          
          ) C
on REPLACE(A.vendor_code, 'CN', '')  = C.customer_code 
),
-- 3.rebatechannel =‘PREMIUM-MSO’
temp_004 as (
select  A.*
       ,CONCAT(COALESCE(B.sales_person_id,'NULL_MSO'),',')  as sales_person_id
from (select *
         from temp_001  
         where rebatechannel ='PREMIUM-MSO'
         
         ) A 
LEFT join
     ( select proj_name,replace(GROUP_CONCAT(sales_person_id),',',',|' )  as sales_person_id
         from temp_dw_cs_relationship_info
          where channel = 'MSO' 
          group by proj_name 
          ) B
on A.rebateitem  = B.proj_name 
),

-- 4.其他渠道数据
/*
rebatechannel 不属于（‘PREMIUM-DISTRIBUTOR’，‘PREMIUM-MM’，‘PREMIUM-MSO’）的渠道。
例如‘PREMIUM-SCHOOL’，‘CT’，‘LIC’，‘MID-TIER’，sales_person_id给空值
*/
temp_005 as (
select  A.*
       ,NULL   as sales_person_id
from (select *
         from temp_001  
         where rebatechannel not in ('PREMIUM-DISTRIBUTOR','PREMIUM-MM','PREMIUM-MSO')
                 or rebatechannel is null   ) A 
),

-- 合并数据
temp_006 as 
(
select 
rebateno
,vendor_code
,vendor_name
,rebatetype
,rebatestate
,frozenstate
,lockstate
,entry
,rebatemoney
,decmoneysum
,remainomney
,frozenmoney
,rebatedate
,opeempname
,opedate
,sales_month
,available_amount
,rebatechannel
,rebateitem
,rebateperiod
,rebateclassification
,sales_person_id
,sales_year
 from temp_002
union all 
select 
rebateno
,vendor_code
,vendor_name
,rebatetype
,rebatestate
,frozenstate
,lockstate
,entry
,rebatemoney
,decmoneysum
,remainomney
,frozenmoney
,rebatedate
,opeempname
,opedate
,sales_month
,available_amount
,rebatechannel
,rebateitem
,rebateperiod
,rebateclassification
,sales_person_id
,sales_year
 from temp_003
union all 
select 
rebateno
,vendor_code
,vendor_name
,rebatetype
,rebatestate
,frozenstate
,lockstate
,entry
,rebatemoney
,decmoneysum
,remainomney
,frozenmoney
,rebatedate
,opeempname
,opedate
,sales_month
,available_amount
,rebatechannel
,rebateitem
,rebateperiod
,rebateclassification
,sales_person_id
,sales_year
 from temp_004
union all 
select 
rebateno
,vendor_code
,vendor_name
,rebatetype
,rebatestate
,frozenstate
,lockstate
,entry
,rebatemoney
,decmoneysum
,remainomney
,frozenmoney
,rebatedate
,opeempname
,opedate
,sales_month
,available_amount
,rebatechannel
,rebateitem
,rebateperiod
,rebateclassification
,sales_person_id
,sales_year
 from temp_005
 )
 
 
 -- STR_TO_DATE(rebatedate,'%Y-%m-%d')
 
 
 select
 rebateno
,vendor_code
,vendor_name
,rebatetype
,rebatestate
,frozenstate
,lockstate
,entry
,rebatemoney
,decmoneysum
,remainomney
,frozenmoney
,DATE_FORMAT(STR_TO_DATE(rebatedate,'%Y-%m-%d'), '%Y%m%d') as rebatedate
,opeempname
,DATE_FORMAT(STR_TO_DATE(opedate,'%Y-%m-%d'), '%Y%m%d') as opedate
,sales_month
,available_amount
,rebatechannel
,rebateitem
,rebateperiod
,rebateclassification
,sales_person_id
,sales_year
, 'fine_ods.ods_rebate_import_query/fine_dw.dw_cs_relationship_info' as data_resource
,now() as etl_time
from temp_006 A


