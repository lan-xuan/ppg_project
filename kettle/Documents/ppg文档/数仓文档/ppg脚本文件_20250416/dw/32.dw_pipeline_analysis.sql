SELECT
	_id,
	opportunity_name,
	opportunity_type,
	REPLACE(JSON_EXTRACT(main_employee_responsible, '$.name'), '"', '')  as main_employee_responsible,
	REPLACE(JSON_EXTRACT(sales_manager, '$.name'), '"', '')  as sales_manager,
	REPLACE(JSON_EXTRACT(core_segment, '$.name'), '"', '')  as core_segment,
	sub_segmen as sub_segment,
	estimated_close_date,
	sales_phase,
	case when fzl2 is not null then fzl2 else  lifecycle_status end as lifecycle_status,
	reason_for_pipeline_close,
	closed_date,
	closed_date_act,
	loss,
	account_det,
	case when account_det = '是'
		then account_sel
		else account_wrt
	end as account_sel,
	mm_brand_det,
	case when mm_brand_det = '是'
		then mm_brand_sel
		else mm_brand_wrt
	end as mm_brand_sel,
	distributor_name_det,
	case when distributor_name_det = '是'
		then distributor_name_sel
		else distributor_name_wrt
	end as distributor_name_sel,
	distributor_code,
	body_shop_name,
	REPLACE(JSON_EXTRACT(address, '$.province'), '"', '') as province,
	REPLACE(JSON_EXTRACT(address, '$.city'), '"', '')  as city,
	REPLACE(JSON_EXTRACT(address, '$.district') , '"', '') as district,
	REPLACE(JSON_EXTRACT(address, '$.detail') , '"', '') as detail,
	ppg_brand1,
	ppg_brand2,
	ppg_brand3,
	ppg_brand4,
	ppg_brand5,
	ppg_brand6,
	wb_sb,
	wpercent_wrt,
	coating_supplier_det1,
	case when coating_supplier_det1 = '是'
		then coating_supplier_sel1
		else coating_supplier_wrt1
	end as coating_supplier_sel1,
	coating_supplier_per1,
	coating_supplier_det2,
	case when coating_supplier_det2 = '是'
		then coating_supplier_sel2
		else coating_supplier_wrt2
	end as coating_supplier_sel2,
	coating_supplier_per2,
	coating_supplier_det3,
	case when coating_supplier_det3 = '是'
		then coating_supplier_sel3
		else coating_supplier_wrt3
	end as coating_supplier_sel3,
	coating_supplier_per3
	model,
	ppg_share_target,
	annual_demand,
	expected_value,
	estimated_close_year_sales,
	carry_over_year_sales,
	probability_of_commercial_success,
	customer_geography,
	technical_support_level,
	growth_potential,
	payment_term,
	social_credit_code,
	baidu_latitude_and_longitude as latitude,
	baidu_latitude_and_longitude as longitude,
	dawn_id,
	deleteTime,
	REPLACE(JSON_EXTRACT(deleter, '$.name'), '"', '')  as deleter,
	REPLACE(JSON_EXTRACT(updater, '$.name'), '"', '')  as updater,
	REPLACE(JSON_EXTRACT(creator, '$.name'), '"', '')  as creator,
	updateTime,
	flowState,
	createTime,
	rmb_expected_value,
	rmb_estimated_close_year_sales,
	'pipeline_analysis' as data_resource,
	now() as etl_time
FROM
	pipeline_analysis