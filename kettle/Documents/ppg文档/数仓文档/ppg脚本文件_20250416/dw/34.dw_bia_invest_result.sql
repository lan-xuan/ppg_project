

/*
目标表：fine_dw.dw_bia_invest_result
来源表：fine_ods.BIA1
*/

SELECT
	_id,
	bia_code,
	bia_name,
	bia_type,
	rfi,
	mso_name as mso_name_det,
	case when mso_name ='是'
		then mso_name1
		else mso_name2
	end as mso_name,
	com_bg_detail,
	-- market_industry_ranking,
	-- est_time,
	ppg_time,
	-- main_brand,
	-- mila,
	ppg_share,
	ds,
	ds1 as ds_det,
	case when ds1 ='是'
		then ds2
		else ds3
	end as ds_name,
	con_f,
	con_t,
	con_y,
	con_reason,	
	email,
	REPLACE(REPLACE(REPLACE(inv_type, '"', ''),'[',''),']','') as inv_type,
	tt_inv,
	set_met,
	dis_share,
	dis_share_per,
	app_doc,
	cfcf_year,
	ebit_ros,
	tt_ppg_sales,
	pay,
	act_pc,
	mso_project,
	ros,
	mm_project,
	con_per,
	irr,
	ppg_net_sales,
	pre_inv,
	inv_ppg_gross,
	reb_inv,
	inv_ppg_net,
	sales_com,
	ebit_zhz,
	con_start_time,
	con_over_time,
	mm_purchase,
	mso_purchase, 	
	win_reason,
	con_file,
	app_email,
	REPLACE(JSON_EXTRACT(deleter, '$.name'), '"', '')  as deleter,
	REPLACE(JSON_EXTRACT(updater, '$.name'), '"', '')  as updater,
	REPLACE(JSON_EXTRACT(creator, '$.name'), '"', '')  as creator,
	deleteTime,
	updateTime,
	createTime,
	flowState,
	'fine_ods.BIA1' as data_resource,
	now() as etl_time
FROM 
	fine_ods.BIA1
--   投资公司信息，及文件链接，计算结果值，流程节点等信息