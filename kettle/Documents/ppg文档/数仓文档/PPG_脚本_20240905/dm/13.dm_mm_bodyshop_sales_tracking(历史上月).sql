with s1 as ( 
select * from  
(
SELECT
uuid() as _id
,mainname
,maincode
,shopcode as shop_code
,shopname
,shoporcalcode
,servicecode
,servicename
,`year_month`
,sum(amountshop) as amountshop
,sum(amountmain) as amountmain
,maintype
,salearea
,salesman
,etl_time
FROM
	fine_dw.dw_sales_tracking_table_lzh 
		where left(`year_month`,7) = concat(left(${mysql_yesterday_l_month},4),'-',right(${mysql_yesterday_l_month},2)) -- 时间参数
group by 
mainname
,maincode
,shopcode
,shopname
,shoporcalcode
,servicecode
,servicename
,`year_month`
,maintype
,salearea
,salesman
,etl_time 
) a )
,s3 as (
select * from  fine_dw.dw_customer_master_list a 
	where left(a.sales_month,6) <= ${mysql_yesterday_d_month}
	   )  -- 时间参数
,s2 as (
SELECT DISTINCT sales_month,customer_code,customer_name,shop_code,shop_name,shoporcal_code,vendor_name from fine_dw.dw_transaction_detail_sh WHERE left(sales_month,4) >= '2022' 
)
,s4 as (
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
		s2 a
		LEFT JOIN s3 b ON a.customer_code = b.customer_code  and a.sales_month = b.sales_month
		LEFT JOIN ( SELECT distinct shop_code,maincode,ship_to_code,sid FROM fine_ods.ods_store_management where sid is not null ) c ON a.shop_code = c.shop_code  and a.customer_code =c.maincode 
	WHERE b.channel in('MM','MSO') and c.sid is not null 
)
,t1 as (
	SELECT
		a.sid,
		'MM Gross Sales' as sec_1,
		left(a.`year_month`,4) as sales_year,
		right(a.`year_month`,2) as sales_month,
		sum(a.amountmain) as sz
	FROM 
	( SELECT DISTINCT a.`year_month`,a.amountmain,b.channel,c.sid
	FROM 
		s1 a
		LEFT JOIN (SELECT * from fine_dw.dw_customer_master_list a where left(a.sales_month,6 ) <= DATE_FORMAT( NOW(), '%Y%m')) b ON a.maincode = b.customer_code and a.`year_month` = CONCAT(left(b.sales_month,4),'-',right(b.sales_month,2)) 
		LEFT JOIN ( SELECT shop_code, shop_name, sid FROM fine_ods.ods_store_management where sid is not null ) c on a.shop_code = c.shop_code -- and a.shopname = c.shop_name 
		) a
	WHERE  a.channel in ('MM')
  -- and a.sid is not null 
 	and left(a.`year_month`,7) = concat(left(${mysql_yesterday_l_month},4),'-',right(${mysql_yesterday_l_month},2)) -- 时间参数
	group by a.sid,left(a.`year_month`,4),right(a.`year_month`,2) 

union all	
	SELECT
		a.sid,
		'MSO Gross Sales' as sec_1,       
		left(a.`year_month`,4) as sales_year,
		right(a.`year_month`,2) as sales_month,
		sum(a.amountmain) as sz
	FROM
  ( SELECT DISTINCT a.`year_month`,a.amountmain,b.channel,c.sid
	FROM
		s1 a
		LEFT JOIN (SELECT * from fine_dw.dw_customer_master_list a where left(a.sales_month,6 ) <= DATE_FORMAT( NOW(), '%Y%m')) b ON a.maincode = b.customer_code and a.`year_month` = CONCAT(left(b.sales_month,4),'-',right(b.sales_month,2)) 
		LEFT JOIN ( SELECT shop_code, shop_name, sid FROM fine_ods.ods_store_management where sid is not null ) c on a.shop_code = c.shop_code -- and a.shopname = c.shop_name 
		) a
	WHERE a.channel in('MSO')
	and a.sid is not null 
	and left(a.`year_month`,7) = concat(left(${mysql_yesterday_l_month},4),'-',right(${mysql_yesterday_l_month},2)) -- 时间参数
	group by  a.sid,a.`year_month`
union all	
	select dawn_id,'Distributor Gross Sales' as sec_1,year as sales_year,month as sales_month,sales_volume as sz from fine_dw.dw_outsourced_sales
	where  CONCAT(year,case when length(month) = 1 then concat('0',month) else month end) = ${mysql_yesterday_l_month}
) 
,t2 as (
select * from t1
union all
select sid,sec_1,sales_year,'FY' AS sales_month,sum(sz) AS sz  from  t1  group by  sid,sec_1,sales_year)
,t3 as (
select * from t2
union ALL
select sid,'Total Gross Sales' as sec_1,sales_year,sales_month,sum(sz) as sz from  t2  GROUP BY sid,sales_year,sales_month
) -- 数据汇总
,t5 as (
		SELECT
		c.sid
		,a.shop_code
		,b.proj_name
		,b.proj_name_en
		,a.shoporcal_code as ship_to_code
		,b.channel
		,d.servicename as vendor_name
	FROM
	  s2 a
		LEFT JOIN (SELECT * from fine_dw.dw_customer_master_list a where left(a.sales_month,6 ) <= ${mysql_yesterday_d_month} -- 时间参数 
		) b 
		ON  a.customer_code = b.customer_code
  -- a.`year_month` = CONCAT(left(b.sales_month,4),'-',right(b.sales_month,2))
		LEFT JOIN ( SELECT shop_code,maincode,shop_name, sid FROM fine_ods.ods_store_management where sid is not null ) c ON a.shop_code = c.shop_code and a.customer_code =c.maincode
		LEFT JOIN s1 d ON d.`year_month` = CONCAT(left(b.sales_month,4),'-',right(b.sales_month,2)) 
		 and d.shop_code = c.shop_code 
		 and d.shopname = c.shop_name

	WHERE b.channel in('MM','MSO') and c.sid is not null 
 and left(d.`year_month`,7) = concat(left(${mysql_yesterday_l_month},4),'-',right(${mysql_yesterday_l_month},2)) -- 时间参数
	) 
,t6 as (
SELECT DISTINCT
	sid,
	GROUP_CONCAT( shop_code SEPARATOR ',' ) AS mm_shop_code 
FROM
	(select distinct sid,shop_code from s4  WHERE channel = 'MM' ORDER BY sid,shop_code)  a 
GROUP BY sid
)
-- mm shop_code
,t7 as (
SELECT DISTINCT
	sid,
	GROUP_CONCAT( proj_name SEPARATOR ',' ) AS mm_proj_name 
FROM
	(select distinct sid,proj_name from s4  WHERE channel = 'MM' ORDER BY sid,proj_name)  a 
GROUP BY sid
)
-- mm proj_name
,t8 as (
SELECT DISTINCT
	sid,
	GROUP_CONCAT( ship_to_code SEPARATOR ',' ) AS mm_ship_to_code
FROM
	(select distinct sid,ship_to_code from s4  WHERE channel = 'MM' ORDER BY sid,ship_to_code)  a 
GROUP BY sid
) -- mm ship_to_code
,t9 as (
SELECT DISTINCT
	sid,
	GROUP_CONCAT( shop_code SEPARATOR ',' ) AS mso_shop_code 
FROM
	(select distinct sid,shop_code from s4  WHERE channel = 'MSO' ORDER BY sid,shop_code)  a 
GROUP BY sid
)
-- mso shop_code
,t10 as (
SELECT DISTINCT
	sid,
	GROUP_CONCAT( proj_name SEPARATOR ',' ) AS mso_proj_name 
FROM
	(select distinct sid,proj_name from s4  WHERE channel = 'MSO' ORDER BY sid,proj_name)  a 
GROUP BY sid
)
-- mso proj_name
,t11 as (
SELECT DISTINCT
	sid,
	GROUP_CONCAT( ship_to_code SEPARATOR ',' ) AS mso_ship_to_code 
FROM
	(select distinct sid,ship_to_code from s4  WHERE channel = 'MSO' ORDER BY sid,ship_to_code)  a 
GROUP BY sid
) -- mso mso_ship_to_code
,t12 as (
	SELECT 
		c.sid,
		min(a.sales_month) as won_sales_month
	FROM
		fine_dw.dw_transaction_detail_sh a
		LEFT JOIN fine_dw.dw_customer_master_list b ON a.customer_code = b.customer_code and a.sales_month = b.sales_month
		LEFT JOIN ( SELECT distinct shop_code,maincode,ship_to_code,sid FROM fine_ods.ods_store_management where sid is not null ) c ON a.shop_code = c.shop_code  and a.customer_code =c.maincode 
	WHERE b.channel in('MM','MSO')
	and c.sid is not null 
group by c.sid
)
-- 赢取时间
,t13 as (
SELECT distinct
	a.dawn_id,
	b.body_shop_name,
	b.province,
	b.city,
	b.detail,
	REPLACE(REPLACE(REPLACE(REPLACE(CONCAT( COALESCE(b.coating_supplier_sel1, ''), ',',
					COALESCE(b.coating_supplier_sel2, ''), ',', 
					COALESCE(b.coating_supplier_sel3, '')) 
	,',,,,,',''),',,,,',''),',,,',''),',,','') AS coating_supplier,
	b.wb_sb,
	REPLACE(REPLACE(REPLACE(REPLACE(CONCAT( COALESCE(b.ppg_brand1, ''), ',',
					COALESCE(b.ppg_brand2, ''), ',', 
					COALESCE(b.ppg_brand3, ''), ',', 
					COALESCE(b.ppg_brand4, ''), ',', 
					COALESCE(b.ppg_brand5, ''), ',', 
					COALESCE(b.ppg_brand6, '') ) 
	,',,,,,',''),',,,,',''),',,,',''),',,','') AS ppg_brand,
	b.distributor_name_sel,
	b.rmb_expected_value,
	b.main_employee_responsible,
	b.sales_manager
FROM
 (select dawn_id,max(createTime) as createTime   from fine_dw.dw_pipeline_analysis group by dawn_id) a 
 left join  fine_dw.dw_pipeline_analysis b on a.dawn_id = b.dawn_id and a.createTime = b.createTime
) -- 简道云 pipeline_analysis
,t14 as (
SELECT
	a.dawn_id,
	b._id,
	c.con_start_time,
	c.con_over_time,
	TIMESTAMPDIFF(YEAR, c.con_start_time, c.con_over_time) AS con_cycle,
	d.total as sales_volume,
	c.win_reason,
	c.ds_name,
	c.ebit_ros as ebit,
	c.createTime,
	e.province,
	e.city,
	e.detail
FROM
	(
	SELECT
		a.dawn_id,
		min( b.createTime ) AS createTime 
	FROM
		fine_dw.dw_bia_store_ratio a
		LEFT JOIN fine_dw.dw_bia_invest_result b ON a._id = b._id 
	WHERE
		dawn_id IS NOT NULL
		and flowState ='1' 
	GROUP BY
	dawn_id 
	)a 
	left join 
	(
	SELECT
		a.dawn_id,
		a._id,
		b.createTime
	FROM
		fine_dw.dw_bia_store_ratio a
		LEFT JOIN fine_dw.dw_bia_invest_result b ON a._id = b._id 
	WHERE
		dawn_id IS NOT NULL 
	)b 
	on a.dawn_id = b.dawn_id and a.createTime = b.createTime
	left join (select _id,con_start_time,con_over_time,win_reason,ds_name,ebit_ros,createTime from fine_dw.dw_bia_invest_result ) c on b._id = c._id
	left join (SELECT _id,total FROM fine_dw.dw_bia_count where name ='MM Sales @ PPG  Selling Price (excl. VAT)') d on b._id = d._id
	left join (select dawn_id,_id,province,city,detail 	from 	fine_dw.dw_bia_store_ratio WHERE dawn_id IS NOT NULL) e on  b._id = e._id and a.dawn_id = e.dawn_id
)-- 简道云 BIA
,t15 as (
SELECT
	sid,
	_year ,
	GROUP_CONCAT( sales_person_id,',|' ) AS mm_sales_id
FROM (
select DISTINCT sid,left(${mysql_yesterday_l_month},4)  as _year,cs.sales_person_id from s4 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON left(${mysql_yesterday_l_month},4) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel = 'MM'
 ORDER BY sid,cs.sales_person_id
 ) a
group by sid,_year
)	-- MM 销售人员
,t16 as (
SELECT
	sid,
	_year ,
	GROUP_CONCAT( sales_person_id,',|' ) AS mso_sales_id
FROM (
select DISTINCT sid,left(${mysql_yesterday_l_month},4)  as  _year,cs.sales_person_id from s4 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON  left(${mysql_yesterday_l_month},4) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel = 'MSO'
 ORDER BY sid,cs.sales_person_id
 ) a
group by sid,_year
)	-- MM 销售人员
,t17 as (
SELECT
	dawn_id as sid,
	year as _year ,
	GROUP_CONCAT( sales_person_id,',|' ) AS wc_sales_id
FROM (
	SELECT DISTINCT
		a.dawn_id,
		a.year,
		cs.sales_person_id
	FROM
		fine_dw.dw_outsourced_sales a
		LEFT JOIN fine_dw.dw_cs_relationship_info cs ON a.YEAR = cs.s_year 
		AND a.sales_code = cs.sales_person_id
		ORDER BY a.dawn_id,
		a.year,
		cs.sales_person_id
)a
group by dawn_id,year
)
-- 外采销售人员
,t18 as (
SELECT
	sid,
	_year ,
	GROUP_CONCAT( sales_person,',' ) AS sales_person-- ,
-- GROUP_CONCAT( team_owner,',' ) AS team_owner,
-- GROUP_CONCAT( vendor_name,',' ) AS vendor_name
FROM (
select DISTINCT sid,left(${mysql_yesterday_l_month},4) as _year,cs.sales_person -- ,cs.team_owner,a.vendor_name 
from s4 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON left(${mysql_yesterday_l_month},4) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel = 'MM'
 union all 
 select DISTINCT sid,left(${mysql_yesterday_l_month},4) as _year,cs.sales_person -- ,cs.team_owner,a.vendor_name 
 from s4 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON  left(${mysql_yesterday_l_month},4) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel = 'MSO'
 union all 
	SELECT DISTINCT
		a.dawn_id,
		a.year,
		cs.sales_person-- ,
		-- cs.team_owner,
		-- a.distributor_name
	FROM
		fine_dw.dw_outsourced_sales a
		LEFT JOIN fine_dw.dw_cs_relationship_info cs ON a.YEAR = cs.s_year 
		AND a.sales_code = cs.sales_person_id
		ORDER BY sid,sales_person-- ,team_owner,vendor_name
 ) a
 WHERE sales_person is not null
group by sid,_year
)
,t19 as (
SELECT
	sid,
	_year ,
-- GROUP_CONCAT( sales_person,',' ) AS sales_person-- ,
 GROUP_CONCAT( team_owner,',' ) AS team_owner
-- GROUP_CONCAT( vendor_name,',' ) AS vendor_name
FROM (
select DISTINCT sid,left(${mysql_yesterday_l_month},4) as _year,cs.team_owner 
from s4 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON left(${mysql_yesterday_l_month},4) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel = 'MM'
 union all 
 select DISTINCT sid,left(${mysql_yesterday_l_month},4) as _year,cs.team_owner 
 from s4 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON  left(${mysql_yesterday_l_month},4) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel = 'MSO'
 union all 
	SELECT DISTINCT
		a.dawn_id,
		a.year,
		-- cs.sales_person-- ,
		cs.team_owner
		-- a.distributor_name
	FROM
		fine_dw.dw_outsourced_sales a
		LEFT JOIN fine_dw.dw_cs_relationship_info cs ON a.YEAR = cs.s_year 
		AND a.sales_code = cs.sales_person_id
		ORDER BY sid,team_owner
 ) a
group by sid,_year
)
,t20 as (
SELECT
	sid,
	_year ,
  GROUP_CONCAT( vendor_name,',' ) AS vendor_name
FROM (
select DISTINCT sid,left(${mysql_yesterday_l_month},4) as _year,a.vendor_name 
from s4 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON left(${mysql_yesterday_l_month},4) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel = 'MM'
 union all 
 select DISTINCT sid,left(${mysql_yesterday_l_month},4) as _year,a.vendor_name 
 from s4 a
	LEFT JOIN fine_dw.dw_cs_relationship_info cs ON  left(${mysql_yesterday_l_month},4) = cs.s_year 
	AND a.proj_name = cs.proj_name
 WHERE a.channel = 'MSO'
 union all 
	SELECT DISTINCT
		a.dawn_id,
		a.year,
    a.distributor_name
	FROM
		fine_dw.dw_outsourced_sales a
		LEFT JOIN fine_dw.dw_cs_relationship_info cs ON a.YEAR = cs.s_year 
		AND a.sales_code = cs.sales_person_id
		ORDER BY sid,vendor_name
 ) a

group by sid,_year
)
	select 
	a.sid,
	a.sec_1,
	a.sales_year,
	CASE
	  WHEN  a.sales_month ='01' THEN 'JAN'
	  WHEN  a.sales_month ='02' THEN 'FEB'
	  WHEN  a.sales_month ='03' THEN 'MAR'
	  WHEN  a.sales_month ='04' THEN 'APR'
	  WHEN  a.sales_month ='05' THEN 'MAY'
	  WHEN  a.sales_month ='06' THEN 'JUN'
	  WHEN  a.sales_month ='07' THEN 'JUL'
	  WHEN  a.sales_month ='08' THEN 'AUG'
	  WHEN  a.sales_month ='09' THEN 'SEP'
	  WHEN  a.sales_month ='10' THEN 'OCT'
	  WHEN  a.sales_month ='11' THEN 'NOV'
	  WHEN  a.sales_month ='12' THEN 'DEC'
	else a.sales_month
	END AS sales_month,
	a.sales_month as sales_month2,
	a.sz as salse_value,
	b.mm_shop_code,
	c.mm_proj_name,
	d.mm_ship_to_code,
	e.mso_shop_code,
	f.mso_proj_name,
	g.mso_ship_to_code,
	CONCAT(LEFT(h.won_sales_month,4),'-',RIGHT(h.won_sales_month,2)) AS won_sales_month,
	left(k.mm_sales_id,CHAR_LENGTH(k.mm_sales_id)-1) as mm_sales_id,
	left(l.mso_sales_id,CHAR_LENGTH(l.mso_sales_id)-1) as mso_sales_id,
	left(m.wc_sales_id,CHAR_LENGTH(m.wc_sales_id)-1)as wc_sales_id,
	i.body_shop_name,
	case when IFNULL(j.province,0) =0 then  i.province else j.province end province,
	case when IFNULL(j.city,0) =0 then  i.city else j.city end city,
	case when IFNULL(j.detail,0) =0 then  i.detail else j.detail end detail,
	i.coating_supplier,
	i.wb_sb,
	i.ppg_brand,
	left(p.vendor_name,CHAR_LENGTH(p.vendor_name)-1)  as distributor_name,
	i.rmb_expected_value as expected_value,
	left(n.sales_person,CHAR_LENGTH(n.sales_person)-1) as salse_name,
	left(o.team_owner,CHAR_LENGTH(o.team_owner)-1) as sales_manager,
	case when IFNULL(j.dawn_id,0)='0' then '否' else '是' end as bia_sf,
	j.con_start_time,
	j.con_over_time,
	j.con_cycle,
	j.sales_volume as con_sales_volume,
	j.win_reason,
	j.ebit,
	left(j.createTime,10) as createTime,
	'dw_transaction_detail_lzh' as data_resource,
	now() as etl_time,
	STR_TO_DATE(CONCAT(a.sales_year ,'0101') , '%Y%m%d') as report_date
	from t3 a
	left join t6 b on a.sid = b.sid
	left join t7 c on a.sid = c.sid
	left join t8 d on a.sid = d.sid
	left join t9 e on a.sid = e.sid
	left join t10 f on a.sid = f.sid
	left join t11 g on a.sid = g.sid
	left join t12 h on a.sid = h.sid
	left join t13 i on a.sid = i.dawn_id
	left join t14 j on a.sid = j.dawn_id
	left join t15 k on a.sid = k.sid and a.sales_year = k._year
	left join t16 l on a.sid = l.sid and a.sales_year = l._year
	left join t17 m on a.sid = m.sid and a.sales_year = m._year
	left join t18 n on a.sid = n.sid and a.sales_year = n._year
	left join t19 o on a.sid = o.sid and a.sales_year = o._year
	left join t20 p on a.sid = p.sid and a.sales_year = p._year
WHERE (concat(a.sales_year,a.sales_month) = ${mysql_yesterday_l_month} -- 时间参数
OR a.sec_1 = 'Distributor Gross Sales') AND a.sales_month != 'FY'