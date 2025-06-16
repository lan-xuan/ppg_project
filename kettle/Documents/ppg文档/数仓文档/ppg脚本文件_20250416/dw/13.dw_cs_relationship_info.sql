/*
目标表：fine_dw.dw_cs_relationship_info
来源表：
fine_ods.ods_cs_relationship_info_df

更新方式：增量更新 year 

参数 ：${mysql_yesterday_d_year_end}/${mysql_yesterday_d_year}

*/
	
with temp_0011 as (
SELECT
replace(customer_code,'CN','') as customer_code,
district,
upper(channel) as channel ,
proj_name,
team_owner,
team_owner_id,
sales_person,
sales_person_id,
date_format(starting_date,'%Y%m%d') as starting_date,

CASE   
	WHEN ending_date IS NOT NULL THEN DATE_FORMAT(ending_date, '%Y%m%d')  
	-- ELSE ${mysql_yesterday_d_year}
	ELSE ${mysql_yesterday_d_year_end}
	END AS ending_date,  	
data_resource,
etl_time
from fine_ods.ods_cs_relationship_info_df
),

-- starting_date/ending_date 空值处理
 temp_001 as (
select 
customer_code
,district
,channel
,proj_name
,team_owner
,team_owner_id
,sales_person
,sales_person_id
,starting_date
,ending_date
,case when starting_date is null  then '00000000'  else  starting_date end as starting_date_2
,case when ending_date is null OR SUBSTRING(ending_date,1,4) =  ${mysql_yesterday_d_year}   then '99999999'  else  ending_date end as ending_date_2
from temp_0011
),
 
-- 当年最后一天（相对与昨天） 是否存在between starting_date_2 and ending_date_2
-- 存在即有效
temp_002 as (
select 
customer_code
,district
,channel
,proj_name
,team_owner
,team_owner_id
,sales_person
,sales_person_id
,starting_date
,ending_date
,starting_date_2
,ending_date_2
,s_year
from  (select a.*
         ,case when ${mysql_yesterday_d_year_end}  between starting_date_2 and ending_date_2
               then ${mysql_yesterday_d_year}  else  null end as s_year 
         from temp_001 a )  b 
where
  -- 当年有效a
  s_year is not null 
)

-- 如果存在多个包括当年最后一天的数据，取开始日期最大的
-- 分组需要确认下哪些字段？
select 
 customer_code
,district
,channel
,proj_name
,team_owner
,team_owner_id
,sales_person
,sales_person_id
,starting_date
,ending_date
,s_year
,"fine_ods.ods_cs_relationship_info_df"  as data_resource
,now()                       as etl_time 
from 
(select a.*
       ,row_number() OVER(PARTITION BY  customer_code
                          ,district
                          ,channel
                          ,proj_name
                         --  ,team_owner
                         -- ,team_owner_id
                         --  ,sales_person
                         --  ,sales_person_id
                          -- ,starting_date
                          -- ,ending_date
                          ,s_year 
                          order by starting_date_2 desc ) as seq 
from temp_002 a
) b 
where seq = 1 
