


/*
目标表：fine_dw.dw_bia_product_distribution
来源表：fine_ods.BIA1
*/



with t1 as (
SELECT
	a._id,
	'占比'as fl,
	mm_products.mm_products_name as name ,
	mm_products.mm_products_gdsxq as senior_water_paint,
	mm_products.mm_products_zdsxq as intermediate_water_paint,
	mm_products.mm_products_gdyxq as senior_oil_paint,
	mm_products.mm_products_zdyxq as intermediate_oil_paint
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( mm_products, "$[*]" COLUMNS (mm_products_name VARCHAR ( 255 ) PATH "$.mm_products_name",mm_products_gdsxq VARCHAR ( 255 ) PATH "$.mm_products_gdsxq",mm_products_zdsxq VARCHAR ( 255 ) PATH "$.mm_products_zdsxq",mm_products_gdyxq VARCHAR ( 255 ) PATH "$.mm_products_gdyxq",mm_products_zdyxq VARCHAR ( 255 ) PATH "$.mm_products_zdyxq")) AS mm_products
)
,t2 as (
SELECT
	a._id,
	'占比'as fl,
	products.products_name,
	products.products_gdsxq,
	products.products_zdsxq,
	products.products_gdyxq,
	products.products_zdyxq
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( products, "$[*]" COLUMNS (products_name VARCHAR ( 255 ) PATH "$.products_name",products_gdsxq VARCHAR ( 255 ) PATH "$.products_gdsxq",products_zdsxq VARCHAR ( 255 ) PATH "$.products_zdsxq",products_gdyxq VARCHAR ( 255 ) PATH "$.products_gdyxq",products_zdyxq VARCHAR ( 255 ) PATH "$.products_zdyxq")) AS products
)
,t3 as (
SELECT
	a._id,
	'产品'as fl,
	'MM主机厂' as fl2,
	mm_pro_name.mm_pro_gdsx,
	mm_pro_name.mm_pro_zdsx,
	mm_pro_name.mm_pro_gdyx,
	mm_pro_name.mm_pro_zdyx
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( mm_pro_name, "$[*]" COLUMNS (mm_pro_gdsx VARCHAR ( 255 ) PATH "$.mm_pro_gdsx",mm_pro_zdsx VARCHAR ( 255 ) PATH "$.mm_pro_zdsx",mm_pro_gdyx VARCHAR ( 255 ) PATH "$.mm_pro_gdyx",mm_pro_zdyx VARCHAR ( 255 ) PATH "$.mm_pro_zdyx")) AS mm_pro_name
)
,t4 as (
SELECT
	a._id,
	'产品'as fl,
	'MSO集团'  as fl2,
	mso_pro_name.mso_pro_gdsx,
	mso_pro_name.mso_pro_zdsx,
	mso_pro_name.mso_pro_gdyx,
	mso_pro_name.mso_pro_zdyx
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( mso_pro_name, "$[*]" COLUMNS (mso_pro_gdsx VARCHAR ( 255 ) PATH "$.mso_pro_gdsx",mso_pro_zdsx VARCHAR ( 255 ) PATH "$.mso_pro_zdsx",mso_pro_gdyx VARCHAR ( 255 ) PATH "$.mso_pro_gdyx",mso_pro_zdyx VARCHAR ( 255 ) PATH "$.mso_pro_zdyx")) AS mso_pro_name
)
,t5  as (
SELECT
	a._id,
	'产品'as fl,
	'DISTRIBUTOR 经销商' as fl2,
	dis_pro.dis_pro_gdsx,
	dis_pro.dis_pro_zdsx,
	dis_pro.dis_pro_gdyx,
	dis_pro.dis_pro_zdyx
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( dis_pro, "$[*]" COLUMNS (dis_pro_gdsx VARCHAR ( 255 ) PATH "$.dis_pro_gdsx",dis_pro_zdsx VARCHAR ( 255 ) PATH "$.dis_pro_zdsx",dis_pro_gdyx VARCHAR ( 255 ) PATH "$.dis_pro_gdyx",dis_pro_zdyx VARCHAR ( 255 ) PATH "$.dis_pro_zdyx")) AS dis_pro
)

SELECT
	_id,
	fl as sec,
	NAME,
	senior_water_paint,
	intermediate_water_paint,
	senior_oil_paint,
	intermediate_oil_paint,
	'fine_ods.BIA1' as data_resource,
	now() as etl_time
FROM
	(
	SELECT * FROM t1 
		UNION ALL                                                                                                                       
	SELECT * FROM t2 
		UNION ALL
	SELECT * FROM t3 
		UNION ALL
	SELECT * FROM t4 
		UNION ALL
	SELECT * FROM t5 
	) a 
ORDER BY
	_id ASC,
	fl DESC
