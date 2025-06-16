
/*
目标表：fine_dw.dw_customer_master_list
来源表：
ods_customer_master_list_df

更新方式:增量更新 year 
参数：${mysql_yesterday_d_year_end} / ${mysql_yesterday_d_year}

*/
with temp_001 as (
SELECT 
REPLACE(customer_code,'CN','') as customer_code,
customer_name,
u_customer_code,
u_customer_name,
upper(district) as district ,
upper(channel) as channel,
proj_name,
is_flag,
proj_name_en,
date_format(starting_date,'%Y%m%d') as starting_date ,
CASE   
	WHEN ending_date IS NOT NULL THEN DATE_FORMAT(ending_date, '%Y%m%d')  
	-- ELSE date_format(concat(year(CURDATE()),'1231'),'%Y%m%d')
	 ELSE ${mysql_yesterday_d_year_end}
	END AS ending_date,  
'ods_customer_master_list_df' as data_resource,
SYSDATE() as etl_time
FROM  fine_ods.ods_customer_master_list_df
)
-- 1月
,temp_1 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     --  ,case when '${mysql_yesterday_d_year}0115' between starting_date and ending_date  -- 月中
     --        then '${mysql_yesterday_d_year}01' else  null end as sales_month 
     ,case when CONCAT(${mysql_yesterday_d_year},'0115')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'01')else  null end as sales_month 
from temp_001
)
-- 2月
,temp_2 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'0215')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'02')else  null end as sales_month 
from temp_001
)
-- 3月
,temp_3 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'0315')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'03')else  null end as sales_month 
from temp_001
)
-- 4月
,temp_4 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'0415')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'04')else  null end as sales_month 
from temp_001
)
-- 5月
,temp_5 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'0515')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'05')else  null end as sales_month 
from temp_001
)
-- 6月
,temp_6 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'0615')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'06')else  null end as sales_month 
from temp_001
)
-- 7月
,temp_7 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'0715')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'07')else  null end as sales_month 
from temp_001
)
-- 8月
,temp_8 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'0815')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'08')else  null end as sales_month 
from temp_001
)
-- 9月
,temp_9 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'0915')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'09')else  null end as sales_month 
from temp_001
)
-- 10 月
,temp_10 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'1015')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'10')else  null end as sales_month 
from temp_001
)
-- 11月
,temp_11 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'1115')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'11')else  null end as sales_month 
from temp_001
)
-- 12月
,temp_12 as (
select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
     ,case when CONCAT(${mysql_yesterday_d_year},'1215')  between starting_date and ending_date  -- 月中
             then  CONCAT(${mysql_yesterday_d_year},'12')else  null end as sales_month 
from temp_001
) ,
temp_sum as (

select * from temp_1 
union all 
select * from temp_2 
union all 
select * from temp_3 
union all 
select * from temp_4 
union all 
select * from temp_5 
union all 
select * from temp_6
union all 
select * from temp_7
union all 
select * from temp_8
union all 
select * from temp_9
union all 
select * from temp_10 
union all 
select * from temp_11 
union all 
select * from temp_12
 )

select customer_code
       ,customer_name
       ,u_customer_code
       ,u_customer_name
       ,district
       ,channel
       ,proj_name
       ,is_flag
       ,proj_name_en
       ,starting_date
       ,ending_date
       ,data_resource
       ,etl_time
      , sales_month 
from temp_sum a
where sales_month is not null 