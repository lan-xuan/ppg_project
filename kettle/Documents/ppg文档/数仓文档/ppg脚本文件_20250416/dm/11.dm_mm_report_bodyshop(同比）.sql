SELECT DISTINCT
a.channel,
case when b.sales_value is null or b.sales_value = '0' then 0 else (a.sales_value - b.sales_value)/b.sales_value end as  sales_value ,
a.sales_month,
a.sales_quarter,
a.sales_year,
'YOY GROWTH%' as sec_1,
a.sec_2,
a.proj_name,
a.proj_name_en,
a.team_owner_id,
a.team_owner,
a.sales_person,
a.sales_person_id,
'dw_transaction_detail_sh' as data_resource,
NOW() as etl_time,
STR_TO_DATE(CONCAT(a.sales_year ,'0101') , '%Y%m%d') as report_date
FROM 
(
SELECT
channel
,sum(sales_value) as sales_value
,sales_month
,sales_quarter
,sales_year
,sec_1
,sec_2
,proj_name
,proj_name_en
,team_owner_id
,team_owner
,sales_person
,sales_person_id
,data_resource
FROM fine_dm.dm_mm_report_bodyshop 
GROUP BY
channel
,sales_month
,sales_quarter
,sales_year
,sec_1
,sec_2
,proj_name
,proj_name_en
,team_owner_id
,team_owner
,sales_person
,sales_person_id
,data_resource
)a 
LEFT JOIN 
(
SELECT
channel
,sum(sales_value) as sales_value
,sales_month
,sales_quarter
,sales_year
,sec_1
,sec_2
,proj_name
,proj_name_en
,team_owner_id
,team_owner
,sales_person
,sales_person_id
,data_resource
FROM fine_dm.dm_mm_report_bodyshop 
GROUP BY
channel
,sales_month
,sales_quarter
,sales_year
,sec_1
,sec_2
,proj_name
,proj_name_en
,team_owner_id
,team_owner
,sales_person
,sales_person_id
,data_resource
) b
ON a.proj_name = b.proj_name
and a.sales_year = b.sales_year
and a.sec_2 = b.sec_2
AND a.channel = b.channel
and left(a.sales_month,4) = left(b.sales_month,4) +1
ORDER BY a.sales_month