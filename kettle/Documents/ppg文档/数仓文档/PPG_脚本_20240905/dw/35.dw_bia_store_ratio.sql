

/*
目标表：fine_dw.dw_bia_store_ratio
来源表：fine_ods.BIA1
*/



with t1 as (
SELECT
    _id,
		REPLACE(fzl24, ',', '') as mso_name
FROM
    BIA1
)
,t2 as (
SELECT
	a._id,
	bsab.bsab_name,
	bsab.bsab_store_name,
	bsab.province,
	bsab.city,
	bsab.district,
	bsab.detail,
	bsab.bsab_mm,
	bsab.bsab_mm1,
	bsab.bsab_mm2,
	bsab.bsab_mso,
	bsab.bsab_mso1,
	bsab.bsab_mso2,
	bsab.bsab_distributor
FROM
	BIA1 a,
	JSON_TABLE ( bsab, "$[*]" COLUMNS ( bsab_name VARCHAR ( 255 ) PATH "$.bsab_name", bsab_store_name VARCHAR ( 255 ) PATH "$.bsab_store_name"
	, province VARCHAR ( 255 ) PATH "$.bsab_store_address.province", city VARCHAR ( 255 ) PATH "$.bsab_store_address.city", district VARCHAR ( 255 ) PATH "$.bsab_store_address.district"
	, detail VARCHAR ( 255 ) PATH "$.bsab_store_address.detail",bsab_mm VARCHAR ( 255 ) PATH "$.bsab_mm", bsab_mm1 VARCHAR ( 255 ) PATH "$.bsab_mm1", bsab_mm2 VARCHAR ( 255 ) PATH "$.bsab_mm2"
	, bsab_mso VARCHAR ( 255 ) PATH "$.bsab_mso", bsab_mso1 VARCHAR ( 255 ) PATH "$.bsab_mso1", bsab_mso2 VARCHAR ( 255 ) PATH "$.bsab_mso2",bsab_distributor VARCHAR ( 255 ) PATH "$.bsab_distributor")) AS bsab
)
,t3 as (
SELECT
	a._id,
	dawn.dawn_bsab_name,
	dawn.dawn_id,
	dawn.dawn_address,
	dawn.dawn_store_id
FROM
	BIA1 a,
	JSON_TABLE ( dawn, "$[*]" COLUMNS ( dawn_bsab_name VARCHAR ( 255 ) PATH "$.dawn_bsab_name",dawn_id VARCHAR ( 255 ) PATH "$.dawn_id",dawn_address VARCHAR ( 255 ) PATH "$.dawn_address"
	,dawn_store_id VARCHAR ( 255 ) PATH "$.dawn_store_id" )) AS dawn
)
,t4 as (
SELECT
	a._id,
	mm_rait.mm_rait_name,
	mm_rait.mm_rait_fwf_jxsj,
	mm_rait.mm_rait_fwf_xmj,
	mm_rait.mm_rait_ppgjs,
	mm_rait.mm_rait_zjc_sx,
	mm_rait.mm_rait_zjc_yx,
	mm_rait.mm_rait_zjc_fl,
	mm_rait.mm_rait_zfwf_xmj,
	mm_rait.mm_rait_xmjs,
	mm_rait.mm_rait_pc
FROM
	BIA1 a,
	JSON_TABLE ( mm_rait, "$[*]" COLUMNS (mm_rait_name VARCHAR ( 255 ) PATH "$.mm_rait_name",mm_rait_fwf_jxsj VARCHAR ( 255 ) PATH "$.mm_rait_fwf_jxsj",mm_rait_fwf_xmj VARCHAR ( 255 ) PATH "$.mm_rait_fwf_xmj",mm_rait_ppgjs VARCHAR ( 255 ) PATH "$.mm_rait_ppgjs",mm_rait_zjc_sx VARCHAR ( 255 ) PATH "$.mm_rait_zjc_sx",mm_rait_zjc_yx VARCHAR ( 255 ) PATH "$.mm_rait_zjc_yx",mm_rait_zjc_fl VARCHAR ( 255 ) PATH "$.mm_rait_zjc_fl" ,mm_rait_zfwf_xmj VARCHAR ( 255 ) PATH "$.mm_rait_zfwf_xmj",mm_rait_xmjs VARCHAR ( 255 ) PATH "$.mm_rait_xmjs",mm_rait_pc VARCHAR ( 255 ) PATH "$.mm_rait_pc")) AS mm_rait
)
,t5 as (
SELECT
	a._id,
	mso_rait.mso_rait_mapping,
	mso_rait.mso_rait_mso,
	mso_rait.mso_rait_fwfl_jxsj,
	mso_rait.mso_rait_fwfl_xmj,
	mso_rait.mso_rait_ppgjs,
	mso_rait.mso_rait_xmfl_xmj,
	mso_rait.mso_rait_xmfl_qdmdj,
	mso_rait.mso_rait_xmfl_zjcj,
	mso_rait.mso_rait_zfwf_xmj,
	mso_rait.mso_rait_zfwf_jxsj,
	mso_rait.mso_rait_zfwf_mm,
	mso_rait.mso_rait_xmjsj,
	mso_rait.mso_rait_pc
FROM
	BIA1 a,
	JSON_TABLE ( mso_rait, "$[*]" COLUMNS (mso_rait_mapping VARCHAR ( 255 ) PATH "$.mso_rait_mapping",mso_rait_mso VARCHAR ( 255 ) PATH "$.mso_rait_mso",mm_rait_fwf_jxsj VARCHAR ( 255 ) PATH "$.mm_rait_fwf_jxsj",mso_rait_fwfl_jxsj VARCHAR ( 255 ) PATH "$.mso_rait_fwfl_jxsj",mso_rait_fwfl_xmj VARCHAR ( 255 ) PATH "$.mso_rait_fwfl_xmj",mso_rait_ppgjs VARCHAR ( 255 ) PATH "$.mso_rait_ppgjs",mso_rait_xmfl_xmj VARCHAR ( 255 ) PATH "$.mso_rait_xmfl_xmj",mso_rait_xmfl_qdmdj VARCHAR ( 255 ) PATH "$.mso_rait_xmfl_qdmdj" ,mso_rait_xmfl_zjcj VARCHAR ( 255 ) PATH "$.mso_rait_xmfl_zjcj",mso_rait_zfwf_xmj VARCHAR ( 255 ) PATH "$.mso_rait_zfwf_xmj",mso_rait_zfwf_jxsj VARCHAR ( 255 ) PATH "$.mso_rait_zfwf_jxsj",mso_rait_zfwf_mm VARCHAR ( 255 ) PATH "$.mso_rait_zfwf_mm",mso_rait_xmjsj VARCHAR ( 255 ) PATH "$.mso_rait_xmjsj",mso_rait_pc VARCHAR ( 255 ) PATH "$.mso_rait_pc")) AS mso_rait
)
,t6 as (
SELECT
	t1._id,
	t1.mso_name,
	t2.bsab_name,
	t2.bsab_store_name,
	t2.province,
	t2.city,
	t2.district,
	t2.detail,
	t2.bsab_mm as bsab_mm_det,
	case when t2.bsab_mm= '是'
		then t2.bsab_mm1 
		else t2.bsab_mm2
	end as bsab_mm,
	t2.bsab_mso as bsab_mso_det,
	case when t2.bsab_mso = '是'
		then t2.bsab_mso1 
		else t2.bsab_mso2
	end as bsab_mso,
	t2.bsab_distributor,
	t3.dawn_id,
	t3.dawn_address,
	t3.dawn_store_id
FROM
	t1
	LEFT JOIN t2 ON t1._id = t2._id
	LEFT JOIN t3 ON t1._id = t3._id 
	AND t2.bsab_name = t3.dawn_bsab_name
	)
SELECT
	t6._id,
	bsab_name,
	bsab_store_name,
	province,
	city,
	district,
	detail,
	bsab_mm_det,
	bsab_mm,
	bsab_mso_det,
	bsab_mso,
	bsab_distributor,
	dawn_id,
	dawn_address as dawn_latitude ,
	dawn_address as dawn_longitude,
	dawn_store_id,
	mm_rait_name,
	mm_rait_fwf_jxsj,
	mm_rait_fwf_xmj,
	mm_rait_ppgjs,
	mm_rait_zjc_sx,
	mm_rait_zjc_yx,
	mm_rait_zjc_fl,
	mm_rait_zfwf_xmj,
	mm_rait_xmjs,
	mm_rait_pc,
	mso_rait_mapping,
	mso_rait_mso,
	mso_rait_fwfl_jxsj,
	mso_rait_fwfl_xmj,
	mso_rait_ppgjs,
	mso_rait_xmfl_xmj,
	mso_rait_xmfl_qdmdj,
	mso_rait_xmfl_zjcj,
	mso_rait_zfwf_xmj,
	mso_rait_zfwf_jxsj,
	mso_rait_zfwf_mm,
	mso_rait_xmjsj,
	mso_rait_pc,
	'fine_ods.BIA1' as data_resource,
	now() as etl_time
FROM
	t6
	LEFT JOIN t4 ON t6.bsab_mm = t4.mm_rait_name 
	AND t6._id = t4._id
	LEFT JOIN t5 ON CONCAT( t6.mso_name, t6.bsab_mm ) = t5.mso_rait_mapping 
	AND t6._id = t5._id

-- 商机门店信息 -以及mm比率和ms比率