with tt1 as ( 
select * from  fine_dw.dw_transaction_detail_sh a 
 where left(a.sales_month,6) = ${mysql_yesterday_d_month}  -- 时间参数
	) 
,tt5 as (
select * from  fine_dw.dw_customer_master_list a 
	where left(a.sales_month,6) <= ${mysql_yesterday_d_month}) -- 时间参数
,t1 as (
	SELECT
		a.sales_month,
		a.create_date,
		a.lastupload_date,
		a.customer_code,
		a.customer_name,
		a.shop_code,
		a.shop_name,
		a.mainpart_price,
		a.ppgmainpart_price,
		a.sales_qty,
		a.mainpart_num,
		a.ppgpart_num,
		b.proj_name,
		b.proj_name_en,
		b.channel,
		c.sid,
		a.shoporcal_code as ship_to_code,
		a.vendor_name		
	FROM
		tt1 a
		LEFT JOIN tt5 b ON a.customer_code = b.customer_code and a.sales_month = b.sales_month
		LEFT JOIN ( SELECT distinct shop_code,maincode,ship_to_code,sid FROM fine_ods.ods_store_management where sid is not null ) c ON a.shop_code = c.shop_code  and a.customer_code =c.maincode 
	WHERE b.channel in('MM','MSO') and c.sid is not null 
	) 
,ss5 as (
select * from  fine_dw.dw_customer_master_list a 
	where left(a.sales_month,6) <= ${mysql_yesterday_d_month})  
,ss6 as (
SELECT DISTINCT sales_month,customer_code,customer_name,shop_code,shop_name,shoporcal_code,vendor_name from fine_dw.dw_transaction_detail_sh WHERE left(sales_month,4) >= '2022'
)
,ss7 as (
	SELECT
		a.sales_month,
		a.customer_code,
		a.customer_name,
		a.shop_code,
		a.shop_name,
		b.proj_name,
		b.proj_name_en,
		b.channel,
		c.sid,
		a.shoporcal_code as ship_to_code,
		a.vendor_name		
	FROM
		ss6 a
		LEFT JOIN ss5 b ON a.customer_code = b.customer_code  and a.sales_month = b.sales_month
		LEFT JOIN ( SELECT distinct shop_code,maincode,ship_to_code,sid FROM fine_ods.ods_store_management where sid is not null ) c ON a.shop_code = c.shop_code  and a.customer_code =c.maincode 
	WHERE b.channel in('MM','MSO') and c.sid is not null 
)
	-- 将数据都拉出来

,tt2 as (
select distinct sid from t1  where channel ='MM'
) 
, tt3 as (
select distinct
	sales_month,
	CONCAT( LEFT ( sales_month, 4 ), '年', RIGHT ( sales_month, 2 ), '月' ) AS sec_1
FROM
	t1 
where sales_month = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y%m') -- 时间参数
)
,t2 as (
select DISTINCT 
	sales_month,
	sid,
	sec_1,
	sec_2,
	sec_3
from tt2
 join tt3
 on 1= 1
 join
(
SELECT 
	'MM Gross Sales' as sec_2,
	'@PPG Price'  as sec_3
union all 
SELECT 
	'MM Gross Sales' as sec_2,
	'@Bodyshop Price'  as sec_3
union ALL
SELECT 
	'MSO Gross Sales' as sec_2,
	'@PPG Price'  as sec_3) a 
on 1=1 
union all 
select DISTINCT 
	DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y%m') as sales_month,
	sid,
	DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y年%m月') AS sec_1,
	sec_2,
	sec_3
from tt2
join
(
SELECT 
	'MM Gross Sales' as sec_2,
	'Open + Closed Order'  as sec_3
union all 
SELECT 
	'MM Gross Sales' as sec_2,
	'Closed Order'  as sec_3
union ALL
SELECT 
	'MSO Gross Sales' as sec_2,
	'Open + Closed Order'  as sec_3) a
on 1=1 
)
,t3 as (
SELECT 
	LEFT( sales_month, 4 ) AS _year,
	RIGHT ( sales_month, 2 ) AS _month,
	sid,
	 CASE
	  when sales_month = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y%m') then 'MTD'
	  WHEN RIGHT(sales_month,2) ='01' THEN 'JAN'
	  WHEN RIGHT(sales_month,2) ='02' THEN 'FEB'
	  WHEN RIGHT(sales_month,2) ='03' THEN 'MAR'
	  WHEN RIGHT(sales_month,2) ='04' THEN 'APR'
	  WHEN RIGHT(sales_month,2) ='05' THEN 'MAY'
	  WHEN RIGHT(sales_month,2) ='06' THEN 'JUN'
	  WHEN RIGHT(sales_month,2) ='07' THEN 'JUL'
	  WHEN RIGHT(sales_month,2) ='08' THEN 'AUG'
	  WHEN RIGHT(sales_month,2) ='09' THEN 'SEP'
	  WHEN RIGHT(sales_month,2) ='10' THEN 'OCT'
	  WHEN RIGHT(sales_month,2) ='11' THEN 'NOV'
	  WHEN RIGHT(sales_month,2) ='12' THEN 'DEC'
	END as sec_1,
	sec_2,
	sec_3 
FROM
	t2
union all 
select
	LEFT(sales_month,4) AS YEAR,
		CASE
	  WHEN RIGHT(sales_month,2) BETWEEN 1 AND 3 THEN 'Q1'
	  WHEN RIGHT(sales_month,2) BETWEEN 4 AND 6 THEN 'Q2'
	  WHEN RIGHT(sales_month,2) BETWEEN 7 AND 9 THEN 'Q3'
	  WHEN RIGHT(sales_month,2) BETWEEN 10 AND 12 THEN 'Q4'
  END AS _month,
	sid,
	CASE
	  WHEN RIGHT(sales_month,2) BETWEEN 1 AND 3 THEN 'Q1'
	  WHEN RIGHT(sales_month,2) BETWEEN 4 AND 6 THEN 'Q2'
	  WHEN RIGHT(sales_month,2) BETWEEN 7 AND 9 THEN 'Q3'
	  WHEN RIGHT(sales_month,2) BETWEEN 10 AND 12 THEN 'Q4'
  END AS sec_1,
	sec_2,
	sec_3
FROM
	t2 )
,t4 as (
SELECT
	LEFT(sales_month,4) AS _year,
	right(sales_month,2) as _month,
	sid,
	'MM Gross Sales' as sec_2,
	'@PPG Price'  as sec_3,
	sum(mainpart_price *sales_qty)  as sz
FROM
	t1 
WHERE
	channel = 'MM' 
	AND CONCAT( LEFT ( sales_month, 4 ), '-', RIGHT ( sales_month, 2 ) ) = DATE_FORMAT( DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y-%m' ) -- 时间参数
group by sales_month,sid
--  之前月份 MM ppg价格
union ALL
SELECT
	LEFT(sales_month,4) AS _year,
	right(sales_month,2) as _month,
	sid,
	'MM Gross Sales' as sec_2,
	'@Bodyshop Price'  as sec_3,
	sum(ppgmainpart_price *sales_qty)  as sz
FROM
	t1 
WHERE
	channel = 'MM' 
	AND CONCAT( LEFT ( sales_month, 4 ), '-', RIGHT ( sales_month, 2 ) ) = DATE_FORMAT( DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y-%m' ) -- 时间参数
group by sales_month,sid
union all 
SELECT
	LEFT(sales_month,4) AS _year,
	right(sales_month,2) as _month,
	sid,
	'MSO Gross Sales' as sec_2,
	'@PPG Price'  as sec_3,
	sum(mainpart_price *sales_qty)  as sz
FROM
	t1 
WHERE
	channel = 'MSO' 
	AND CONCAT( LEFT ( sales_month, 4 ), '-', RIGHT ( sales_month, 2 ) ) = DATE_FORMAT( DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y-%m' ) -- 时间参数
group by sales_month,sid
UNION ALL 
select 
		LEFT(sales_month,4) AS _year,
		right(sales_month,2) as _month,
		sid,
		sec_2,
		sec_3,
		sum(sz) as sz
	from (
	SELECT
		DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y%m') as sales_month,
		sid,
		proj_name_en,
		'MM Gross Sales' as sec_2,
		'Open + Closed Order'  as sec_3,
		case when proj_name_en = 'SVW'
			THEN sum(mainpart_price*ppgpart_num) 
			else sum(mainpart_price*mainpart_num) 
		end as sz
	FROM
		t1 
	WHERE
		channel = 'MM' 
		AND left(create_date,7) = DATE_FORMAT( DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y-%m' ) -- 时间参数
	group by sid,proj_name_en) a
group by 
		sales_month,
		sid,
		sec_2,
		sec_3
union all
select 
		LEFT(sales_month,4) AS _year,
		right(sales_month,2) as _month,
		sid,
		sec_2,
		sec_3,
		sum(sz) as sz
	from (
	SELECT
		DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y%m') as sales_month,
		sid,
		proj_name_en,
		'MM Gross Sales' as sec_2,
		'Closed Order'  as sec_3,
		case when proj_name_en = 'SVW'
			THEN sum(mainpart_price*ppgpart_num) 
			else sum(mainpart_price*mainpart_num) 
		end as sz
	FROM
		t1 
	WHERE
		channel = 'MM' 
		AND left(lastupload_date,7) = DATE_FORMAT( DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y-%m' ) -- 时间参数 
	group by sid,proj_name_en) a
group by 
		sales_month,
		sid,
		sec_2,
		sec_3
union ALL
	SELECT
		LEFT(DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y%m'),4) AS _year,
		right(DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y%m'),2) as _month,
		sid,
		'MSO Gross Sales' as sec_2,
		'Open + Closed Order'  as sec_3,
		sum(mainpart_price*mainpart_num) sz
	FROM
		t1 
	WHERE
		channel = 'MSO' 
		AND left(create_date,7) = CONCAT(DATE_FORMAT( DATE_ADD(CURDATE(), INTERVAL -1 DAY), '%Y-%m' ),'') -- 时间参数
	group by sid)
,t5 as (
select _year,_month,sid,sec_2,sec_3,sz from t4
union all 
select
	_year,
	CASE
	  WHEN _month BETWEEN 1 AND 3 THEN 'Q1'
	  WHEN _month BETWEEN 4 AND 6 THEN 'Q2'
	  WHEN _month BETWEEN 7 AND 9 THEN 'Q3'
	  WHEN _month BETWEEN 10 AND 12 THEN 'Q4'
  END AS _month,
	sid,
	sec_2,
	sec_3,
	sum(sz) as  sz
FROM
	t4
group by 
	_year,
	sid,
	CASE
	  WHEN _month BETWEEN 1 AND 3 THEN 'Q1'
	  WHEN _month BETWEEN 4 AND 6 THEN 'Q2'
	  WHEN _month BETWEEN 7 AND 9 THEN 'Q3'
	  WHEN _month BETWEEN 10 AND 12 THEN 'Q4'
  END,
	sec_2,
	sec_3
	) -- 数据汇总
,t6 as (
SELECT
	sid,
	GROUP_CONCAT( proj_name SEPARATOR ',' ) AS proj_name_en 
FROM (
select DISTINCT sid,proj_name from ss7 WHERE channel = 'MM' ORDER BY sid,proj_name) a
group by sid
)	-- MM proj_name
,t14 as(
SELECT
	sid,
	GROUP_CONCAT( shop_code SEPARATOR ',' ) AS mm_shop_code 
FROM (
select DISTINCT sid,shop_code from ss7 WHERE channel = 'MM' ORDER BY sid,shop_code) a
group by sid
)-- MM shop_code
,t7 as (
SELECT
	sid,
	GROUP_CONCAT( ship_to_code SEPARATOR ',' ) AS ship_to_code 
FROM
	( SELECT DISTINCT sid, ship_to_code FROM ss7 WHERE channel = 'MM' ORDER BY sid, ship_to_code) a 
GROUP BY sid
)  -- MM ship_to_code
,t8 as (
SELECT
	sid,
	GROUP_CONCAT( proj_name SEPARATOR ',' ) AS proj_name 
FROM (
select DISTINCT sid,proj_name from ss7 WHERE channel = 'MSO' ORDER BY sid,proj_name) a
group by sid
) -- MSO proj_name
,t15 as ( 
SELECT
	sid,
	GROUP_CONCAT( shop_code SEPARATOR ',' ) AS mso_shop_code 
FROM (
select DISTINCT sid,shop_code from ss7 WHERE channel = 'MSO' ORDER BY sid,shop_code) a
group by sid
) -- MSO shop_code
,t9 as (
SELECT
	sid,
	GROUP_CONCAT( ship_to_code SEPARATOR ',' ) AS ship_to_code 
FROM
	( SELECT DISTINCT sid, ship_to_code FROM ss7 WHERE channel = 'MSO' ORDER BY sid, ship_to_code) a 
GROUP BY sid
)-- MSO ship_to_code
,t10 as (
SELECT distinct
	a.dawn_id,
	b.body_shop_name,
	REPLACE(REPLACE(REPLACE(REPLACE(CONCAT( COALESCE(b.ppg_brand1, ''), ',',
					COALESCE(b.ppg_brand2, ''), ',', 
					COALESCE(b.ppg_brand3, ''), ',', 
					COALESCE(b.ppg_brand4, ''), ',', 
					COALESCE(b.ppg_brand5, ''), ',', 
					COALESCE(b.ppg_brand6, '') ) 
	,',,,,,',''),',,,,',''),',,,',''),',,','') AS ppg_brand,
	b.main_employee_responsible,
	b.sales_manager,
	b.distributor_name_sel
FROM
 (select dawn_id,max(updateTime) as updateTime   from fine_dw.dw_pipeline_analysis group by dawn_id) a 
 left join  fine_dw.dw_pipeline_analysis b on a.dawn_id = b.dawn_id and a.updateTime = b.updateTime
	
) 
-- 简道云数据
,t12 as (
SELECT
	sid,
	_year ,
	GROUP_CONCAT( sales_person_id,',|' ) AS mm_sales_id
FROM (
select DISTINCT sid,LEFT(a.sales_month,4) as _year,cs.sales_person_id from ss7 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON LEFT ( a.sales_month, 4 ) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel = 'MM'
 ORDER BY sid,LEFT(a.sales_month,4),cs.sales_person_id
 ) a
group by sid,_year
)		-- MM 销售人员
,t20 as (
SELECT
	sid,
	_year ,
	GROUP_CONCAT( sales_person,',' ) AS sales_person
FROM (
select DISTINCT sid,LEFT(a.sales_month,4) as _year,cs.sales_person from ss7 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON LEFT ( a.sales_month, 4 ) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel in ('MM','MSO')
 ORDER BY sid,LEFT(a.sales_month,4),cs.sales_person
 ) a
group by sid,_year
)		-- MM 销售人员
,t21 as (
SELECT
	sid,
	_year ,
	GROUP_CONCAT( team_owner,',' ) AS team_owner
FROM (
select DISTINCT sid,LEFT(a.sales_month,4) as _year,team_owner from ss7 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON LEFT ( a.sales_month, 4 ) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel in ('MM','MSO')
 ORDER BY sid,LEFT(a.sales_month,4),team_owner
 ) a
group by sid,_year
)		-- MM 销售人员
,t22 as (
SELECT
	sid,
	_year ,
	GROUP_CONCAT( vendor_name,',' ) AS vendor_name
FROM (
select DISTINCT sid,LEFT(a.sales_month,4) as _year,a.vendor_name from ss7 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON LEFT ( a.sales_month, 4 ) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel in ('MM','MSO')
 ORDER BY sid,LEFT(a.sales_month,4),a.vendor_name
 ) a
group by sid,_year
)		-- MM 销售人员
,t13 as (
SELECT
	sid,
	_year ,
	GROUP_CONCAT( sales_person_id,',|' ) AS mso_sales_id
FROM (
select DISTINCT sid,LEFT(a.sales_month,4) as _year,cs.sales_person_id from ss7 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON LEFT ( a.sales_month, 4 ) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel = 'MSO'
 ORDER BY sid,LEFT(a.sales_month,4),cs.sales_person_id
 ) a
group by sid,_year
)	-- MM 销售人员

SELECT
	a.sid as dawn_id,
	g.body_shop_name,
	c.proj_name_en as mm_proj_name,
	e.proj_name as mso_proj_name,
	d.ship_to_code as mm_ship_to,
	f.ship_to_code as mso_ship_to,
	g.ppg_brand,
	left(l.sales_person,CHAR_LENGTH(l.sales_person)-1) as sales,
	left(m.team_owner,CHAR_LENGTH(m.team_owner)-1) as  sales_manager,
	left(n.vendor_name,CHAR_LENGTH(n.vendor_name)-1) as distributor_name,
	a._year as year,
	a._month as month,
	a.sec_1,
	a.sec_2,
	a.sec_3,
	b.sz as sales_value,
	case 
		when a._month='01' then 1
		when a._month='02' then 2
		when a._month='03' then 3
		when a._month='Q1' then 4
		when a._month='04' then 5
		when a._month='05' then 6
		when a._month='06' then 7
		when a._month='Q2' then 8
		when a._month='07' then 9
		when a._month='08' then 10
		when a._month='09' then 11
		when a._month='Q3' then 12
		when a._month='10' then 13
		when a._month='11' then 14
		when a._month='12' then 15
		when a._month='Q4' then 16
	END AS seq ,
	case when left(j.mm_sales_id,CHAR_LENGTH(j.mm_sales_id)-1) is not null and left(k.mso_sales_id,CHAR_LENGTH(k.mso_sales_id)-1) is not NULL
	     then 'MM/MSO'
			 when left(j.mm_sales_id,CHAR_LENGTH(j.mm_sales_id)-1) is not null and left(k.mso_sales_id,CHAR_LENGTH(k.mso_sales_id)-1) is NULL
	     then 'MM'
			 when left(j.mm_sales_id,CHAR_LENGTH(j.mm_sales_id)-1) is null and left(k.mso_sales_id,CHAR_LENGTH(k.mso_sales_id)-1) is not NULL
	     then 'MSO' end as channel,
	left(j.mm_sales_id,CHAR_LENGTH(j.mm_sales_id)-1) AS mm_sales_id,
	left(k.mso_sales_id,CHAR_LENGTH(k.mso_sales_id)-1) AS mso_sales_id,
	h.mm_shop_code,
	i.mso_shop_code,
	'dw_transaction_detail_sh' as data_resource,
	 now() as etl_time,
	STR_TO_DATE(CONCAT(a._year ,'0101') , '%Y%m%d') as report_date
FROM
	t3 a
	LEFT JOIN t5 b ON a.sid = b.sid 
	AND a._year = b._year 
	AND a._month = b._month 
	AND a.sec_2 = b.sec_2 
	AND a.sec_3 = b.sec_3
	left join t6 c on a.sid = c.sid
	left join t7 d on a.sid = d.sid
	left join t8 e on a.sid = e.sid
	left join t9 f on a.sid = f.sid
	left join t14 h on 	a.sid = h.sid
	left join t15 i on 	a.sid = i.sid
	left join t10 g on 	a.sid = g.dawn_id
	left join t12 j on a.sid = j.sid and a._year = j._year
	left join t20 l on a.sid = l.sid and a._year = l._year
	left join t21 m on a.sid = m.sid and a._year = m._year
	left join t22 n on a.sid = n.sid and a._year = n._year
	left join t13 k on a.sid = k.sid and a._year = k._year
WHERE concat(a._year,a._month) = ${mysql_yesterday_d_month} -- 时间参数
