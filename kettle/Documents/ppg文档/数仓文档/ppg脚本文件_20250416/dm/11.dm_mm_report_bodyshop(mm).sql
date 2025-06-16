with  t1 as (
select * from fine_dw.dw_customer_master_list 
where channel ='MM' and left(sales_month,4)>='2022' 
) 
,t2 as(
 SELECT shop_code, shop_name, sid FROM fine_ods.ods_store_management where sid is not null )
,t3 as (
	select  	
		min(sales_month) as sales_month,
		sum(mainpart_price*sales_qty) as amountmain, 
		customer_code,
		customer_name,
		shop_code,
		shop_name
	FROM
		fine_dw.dw_transaction_detail_sh 
WHERE left(sales_month,4) >= '2022' 
group by 
	customer_code,
	customer_name,
	shop_code,
	shop_name
)
,t4 as (
SELECT 
		min(a.sales_month) as sales_month,
		sum(amountmain)  as amountmain,
		c.sid, -- 门店的唯一编码0
		CASE
	  WHEN RIGHT(min(a.sales_month),2) ='01' THEN 'JAN'
	  WHEN RIGHT(min(a.sales_month),2) ='02' THEN 'FEB'
	  WHEN RIGHT(min(a.sales_month),2) ='03' THEN 'MAR'
	  WHEN RIGHT(min(a.sales_month),2) ='04' THEN 'APR'
	  WHEN RIGHT(min(a.sales_month),2) ='05' THEN 'MAY'
	  WHEN RIGHT(min(a.sales_month),2) ='06' THEN 'JUN'
	  WHEN RIGHT(min(a.sales_month),2) ='07' THEN 'JUL'
	  WHEN RIGHT(min(a.sales_month),2) ='08' THEN 'AUG'
	  WHEN RIGHT(min(a.sales_month),2) ='09' THEN 'SEP'
	  WHEN RIGHT(min(a.sales_month),2) ='10' THEN 'OCT'
	  WHEN RIGHT(min(a.sales_month),2) ='11' THEN 'NOV'
	  WHEN RIGHT(min(a.sales_month),2) ='12' THEN 'DEC'
  END AS  sales_month1,
    CONCAT(left(min(a.sales_month),4),
		CASE
	  WHEN RIGHT(min(a.sales_month),2) BETWEEN 1 AND 3 THEN 'Q1'
	  WHEN RIGHT(min(a.sales_month),2) BETWEEN 4 AND 6 THEN 'Q2'
	  WHEN RIGHT(min(a.sales_month),2) BETWEEN 7 AND 9 THEN 'Q3'
	  WHEN RIGHT(min(a.sales_month),2) BETWEEN 10 AND 12 THEN 'Q4'
  END ) AS sales_quarter
	FROM
		t3 a  -- 数据主表		
		JOIN t1 b ON a.customer_code = b.customer_code  and a.sales_month = b.sales_month -- 区分 MM MSO
		JOIN t2 c ON a.shop_code = c.shop_code AND a.shop_name = c.shop_name 
	group by 
		c.sid
)
,t5 as (
SELECT
      sid,
			sales_quarter,
			sum(amountmain) as amountmain
FROM t4
GROUP BY sales_quarter,sid
)

,t7 as (
SELECT
      sid,
			sales_month,
			sales_quarter,
			sales_month1,
			sum(amountmain) as amountmain
FROM t4
GROUP BY sales_month,sid,sales_quarter,sales_month1
)

,t10 as (
SELECT
 sid,
sales_month,
sales_quarter,
amountmain,
CONCAT('FY',SUBSTR(sales_month,3,2),' ',sales_month1) as type,
sales_month1 as type2
FROM t7
UNION ALL
SELECT
 sid,
sales_quarter as sales_month,
sales_quarter,
amountmain,
CONCAT('FY',SUBSTR(sales_quarter,3,2),' ',RIGHT(sales_quarter,2)),
RIGHT(sales_quarter,2)
FROM t5

UNION ALL
SELECT
sid,
CONCAT(LEFT(sales_month,4),'','FULL YEAR'),
CONCAT(LEFT(sales_month,4),'','FULL'),
SUM(amountmain) AS amountmain,
CONCAT('FY',SUBSTR(sales_month,3,2),' ','FULL'),
'FULL YEAR'
FROM t7
GROUP BY
LEFT(sales_month,4),sid
 )
,t11 as ( 
select * from  fine_dw.dw_transaction_detail_sh a 
	where  left(a.sales_month,4) >='2022'
-- and left(a.sales_month,4 ) = '2024'
	) 
,t12 as (
select * from  fine_dw.dw_customer_master_list a 
	where left(a.sales_month,4 ) >='2022') 
,t13 as (
	SELECT DISTINCT
		b.proj_name,
		b.proj_name_en,
		b.channel,
		c.sid,
		a.shoporcal_code as ship_to_code
	FROM
		t11 a
		LEFT JOIN t12 b ON a.customer_code = b.customer_code and a.sales_month = b.sales_month
		LEFT JOIN ( SELECT distinct shop_code,maincode,ship_to_code,sid FROM fine_ods.ods_store_management where sid is not null ) c ON a.shop_code = c.shop_code  and a.customer_code =c.maincode 
	WHERE b.channel in('MM') and c.sid is not null 
	)
,t15 as 
( 
SELECT
  proj_name,
	s_year,
  GROUP_CONCAT( team_owner_id SEPARATOR ',' ) AS team_owner_id,
	GROUP_CONCAT( team_owner SEPARATOR ',' ) AS team_owner,
  GROUP_CONCAT( sales_person SEPARATOR ',' ) AS sales_person,
	GROUP_CONCAT( sales_person_id SEPARATOR ',' ) AS sales_person_id
FROM ( SELECT DISTINCT s_year,proj_name,team_owner_id,team_owner,sales_person,sales_person_id FROM fine_dw.dw_cs_relationship_info )a
WHERE proj_name is not null
GROUP BY proj_name,s_year

)

/*,t14 as 
(
SELECT DISTINCT
	a.channel,
	a.proj_name,
	a.proj_name_en,
	a.sid,
	b.team_owner_id,
	b.team_owner,
	b.sales_person,
	b.sales_person_id
	FROM t13 a
	LEFT JOIN t15 b
	on a.proj_name = b.proj_name
)
*/

SELECT
CONCAT('Y',SUBSTRING(a.sales_month,3,2)," NEW WIN") as channel,
a.amountmain as sales_value,
a.sales_month,
a.sales_quarter,
left(a.sales_month,4) as sales_year,
a.type as sec_1,
a.type2 as sec_2,
b.proj_name,
b.proj_name_en,
c.team_owner_id,
c.team_owner,
c.sales_person,
c.sales_person_id,
'dw_transaction_detail_sh' as data_resource,
NOW() as etl_time,
STR_TO_DATE(CONCAT(left(a.sales_month,4) ,'0101') , '%Y%m%d') as report_date
FROM t13 b
JOIN t10 a
ON a.sid = b.sid
LEFT JOIN t15 c
ON b.proj_name = c.proj_name
AND left(a.sales_month,4) = c.s_year
UNION ALL
SELECT
CONCAT('Y',SUBSTRING(a.sales_month,3,2) +1," NEW WIN") as channel,
a.amountmain as sales_value,
a.sales_month,
a.sales_quarter,
left(a.sales_month,4) + 1 as sales_year,
a.type as sec_1,
a.type2 as sec_2,
b.proj_name,
b.proj_name_en,
c.team_owner_id,
c.team_owner,
c.sales_person,
c.sales_person_id,
'dw_transaction_detail_sh' as data_resource,
NOW() as etl_time,
STR_TO_DATE(CONCAT(left(a.sales_month,4) +1 ,'0101') , '%Y%m%d') as report_date
FROM t13 b
JOIN t10 a
ON a.sid = b.sid
LEFT JOIN t15 c
ON b.proj_name = c.proj_name
AND left(a.sales_month,4)+1 = c.s_year
WHERE left(a.sales_month,4) is not null
AND left(a.sales_month,4) < DATE_FORMAT( NOW(), '%Y')