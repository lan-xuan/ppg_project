



/*
目标表：fine_dw.dw_bia_product_dw_bia_countdistribution
来源表：fine_ods.BIA1
*/



with t1 as (
SELECT
	a._id,
	'承诺含税销售额-MM' as fl,
	mm_cb.mm_cb_mm  as name ,
	'' as type,
	'' as per,
	'' as _explain,
	mm_cb.mm_cb_yr1 as yr1 ,
	mm_cb.mm_cb_yr2 as yr2 ,
	mm_cb.mm_cb_yr3 as yr3 ,
	mm_cb.mm_cb_yr4 as yr4 ,
	mm_cb.mm_cb_yr5 as yr5 ,
	mm_cb.mm_cb_total as total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( mm_cb, "$[*]" COLUMNS (mm_cb_mm VARCHAR ( 255 ) PATH "$.mm_cb_mm",mm_cb_total VARCHAR ( 255 ) PATH "$.mm_cb_total",mm_cb_yr1 VARCHAR ( 255 ) PATH "$.mm_cb_yr1",mm_cb_yr2 VARCHAR ( 255 ) PATH "$.mm_cb_yr2",mm_cb_yr3 VARCHAR ( 255 ) PATH "$.mm_cb_yr3",mm_cb_yr4 VARCHAR ( 255 ) PATH "$.mm_cb_yr4",mm_cb_yr5 VARCHAR ( 255 ) PATH "$.mm_cb_yr5"
	)) AS mm_cb
)
,t2 as (
SELECT
	a._id,
	'承诺含税销售额' as fl,
	sale_com.sale_com_name,
	sale_com.sale_com_type,
	sale_com.sale_com_per,
	'' as _explain,
	sale_com.sale_com_yr1,
	sale_com.sale_com_yr2,
	sale_com.sale_com_yr3,
	sale_com.sale_com_yr4,
	sale_com.sale_com_yr5,
	sale_com.sale_com_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( sale_com, "$[*]" COLUMNS (sale_com_name VARCHAR ( 255 ) PATH "$.sale_com_name",sale_com_type VARCHAR ( 255 ) PATH "$.sale_com_type",sale_com_per VARCHAR ( 255 ) PATH "$.sale_com_per",sale_com_total VARCHAR ( 255 ) PATH "$.sale_com_total",sale_com_yr1 VARCHAR ( 255 ) PATH "$.sale_com_yr1",sale_com_yr2 VARCHAR ( 255 ) PATH "$.sale_com_yr2",sale_com_yr3 VARCHAR ( 255 ) PATH "$.sale_com_yr3",sale_com_yr4 VARCHAR ( 255 ) PATH "$.sale_com_yr4",sale_com_yr5 VARCHAR ( 255 ) PATH "$.sale_com_yr5")) AS sale_com
)
,t3 as (
SELECT
	a._id,
	'总承诺含税销售额' as fl,
	dis_chan.dis_chan_name,
	'' as type,
	'' as per,
	'' as _explain,
	dis_chan.dis_chan_yr1,
	dis_chan.dis_chan_yr2,
	dis_chan.dis_chan_yr3,
	dis_chan.dis_chan_yr4,
	dis_chan.dis_chan_yr5,
	dis_chan.dis_chan_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( dis_chan, "$[*]" COLUMNS (dis_chan_name VARCHAR ( 255 ) PATH "$.dis_chan_name",dis_chan_total VARCHAR ( 255 ) PATH "$.dis_chan_total",dis_chan_yr1 VARCHAR ( 255 ) PATH "$.dis_chan_yr1",dis_chan_yr2 VARCHAR ( 255 ) PATH "$.dis_chan_yr2",dis_chan_yr3 VARCHAR ( 255 ) PATH "$.dis_chan_yr3",dis_chan_yr4 VARCHAR ( 255 ) PATH "$.dis_chan_yr4",dis_chan_yr5 VARCHAR ( 255 ) PATH "$.dis_chan_yr5")) AS dis_chan
)
,t4 as (
SELECT
	a._id,
	'Mix Machine' as fl,
	mix_mac.mix_mac_name,
	'' as type,
	'' as per,
	'' as _explain,
	mix_mac.mix_mac_yr1,
	'' as yr2,
	'' as yr3,
	'' as yr4,
	'' as yr5,
	mix_mac.mix_mac_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( mix_mac, "$[*]" COLUMNS (mix_mac_name VARCHAR ( 255 ) PATH "$.mix_mac_name",mix_mac_yr1 VARCHAR ( 255 ) PATH "$.mix_mac_yr1",mix_mac_total VARCHAR ( 255 ) PATH "$.mix_mac_total")) AS mix_mac
)
,t5 as (
SELECT
	a._id,
	'MM PPG investment' as fl,
	mm_ppg_inv.mm_ppg_inv_name,
	'' as type,
	'' as per,
	'' as _explain,
	mm_ppg_inv.mm_ppg_inv_yr1,
	'' as yr2,
	'' as yr3,
	'' as yr4,
	'' as yr5,
	mm_ppg_inv.mm_ppg_inv_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( mm_ppg_inv, "$[*]" COLUMNS (mm_ppg_inv_name VARCHAR ( 255 ) PATH "$.mm_ppg_inv_name",mm_ppg_inv_total VARCHAR ( 255 ) PATH "$.mm_ppg_inv_total",mm_ppg_inv_yr1 VARCHAR ( 255 ) PATH "$.mm_ppg_inv_yr1")) AS mm_ppg_inv
)
,t6 as (
SELECT
	a._id,
	'MM Distribution share %' as fl,
	mm_dis_share.mm_dis_share_name,
	'' as type,
	'' as per,
	'' as _explain,
	mm_dis_share.mm_dis_share_yr1,
	'' as yr2,
	'' as yr3,
	'' as yr4,
	'' as yr5,
	mm_dis_share.mm_dis_share_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( mm_dis_share, "$[*]" COLUMNS (mm_dis_share_name VARCHAR ( 255 ) PATH "$.mm_dis_share_name",mm_dis_share_total VARCHAR ( 255 ) PATH "$.mm_dis_share_total",mm_dis_share_yr1 VARCHAR ( 255 ) PATH "$.mm_dis_share_yr1")) AS mm_dis_share
)
,t7 as (
SELECT
	a._id,
	'Cash Investment (prebate) （excl VAT）' as fl,
	ci_pre.ci_pre_name,
	'' as type,
	'' as per,
	'' as _explain,
	ci_pre.ci_pre_yr1,
	ci_pre.ci_pre_yr2,
	ci_pre.ci_pre_yr3,
	ci_pre.ci_pre_yr4,
	ci_pre.ci_pre_yr5,
	ci_pre.ci_pre_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( ci_pre, "$[*]" COLUMNS (ci_pre_name VARCHAR ( 255 ) PATH "$.ci_pre_name",ci_pre_total VARCHAR ( 255 ) PATH "$.ci_pre_total",ci_pre_yr1 VARCHAR ( 255 ) PATH "$.ci_pre_yr1",ci_pre_yr2 VARCHAR ( 255 ) PATH "$.ci_pre_yr2",ci_pre_yr3 VARCHAR ( 255 ) PATH "$.ci_pre_yr3",ci_pre_yr4 VARCHAR ( 255 ) PATH "$.ci_pre_yr4",ci_pre_yr5 VARCHAR ( 255 ) PATH "$.ci_pre_yr5")) AS ci_pre
)
,t8 as (
SELECT
	a._id,
	'CI PPG share' as fl,
	ci_ppg_share.ci_ppg_share_name,
	'' as type,
	'' as per,
	'' as _explain,
	ci_ppg_share.ci_ppg_share_yr1,
	ci_ppg_share.ci_ppg_share_yr2,
	ci_ppg_share.ci_ppg_share_yr3,
	ci_ppg_share.ci_ppg_share_yr4,
	ci_ppg_share.ci_ppg_share_yr5,
	ci_ppg_share.ci_ppg_share_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( ci_ppg_share, "$[*]" COLUMNS (ci_ppg_share_name VARCHAR ( 255 ) PATH "$.ci_ppg_share_name",ci_ppg_share_total VARCHAR ( 255 ) PATH "$.ci_ppg_share_total",ci_ppg_share_yr1 VARCHAR ( 255 ) PATH "$.ci_ppg_share_yr1",ci_ppg_share_yr2 VARCHAR ( 255 ) PATH "$.ci_ppg_share_yr2",ci_ppg_share_yr3 VARCHAR ( 255 ) PATH "$.ci_ppg_share_yr3",ci_ppg_share_yr4 VARCHAR ( 255 ) PATH "$.ci_ppg_share_yr4",ci_ppg_share_yr5 VARCHAR ( 255 ) PATH "$.ci_ppg_share_yr5")) AS ci_ppg_share
)
,t9 as (
SELECT
	a._id,
	'CI Distribution share %' as fl,
	ci_dis_share.ci_dis_share_name,
	'' as type,
	'' as per,
	'' as _explain,
	ci_dis_share.ci_dis_share_yr1,
	ci_dis_share.ci_dis_share_yr2,
	ci_dis_share.ci_dis_share_yr3,
	ci_dis_share.ci_dis_share_yr4,
	ci_dis_share.ci_dis_share_yr5,
	ci_dis_share.ci_dis_share_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( ci_dis_share, "$[*]" COLUMNS (ci_dis_share_name VARCHAR ( 255 ) PATH "$.ci_dis_share_name",ci_dis_share_total VARCHAR ( 255 ) PATH "$.ci_dis_share_total",ci_dis_share_yr1 VARCHAR ( 255 ) PATH "$.ci_dis_share_yr1",ci_dis_share_yr2 VARCHAR ( 255 ) PATH "$.ci_dis_share_yr2",ci_dis_share_yr3 VARCHAR ( 255 ) PATH "$.ci_dis_share_yr3",ci_dis_share_yr4 VARCHAR ( 255 ) PATH "$.ci_dis_share_yr4",ci_dis_share_yr5 VARCHAR ( 255 ) PATH "$.ci_dis_share_yr5")) AS ci_dis_share
)
,t10 as (
SELECT
	a._id,
	'Sales target Bodyshop (incl 13% VAT)' as fl,
	sales_tar_body.sales_tar_body_name,
	'' as type,
	'' as per,
	'' as _explain,
	sales_tar_body.sales_tar_body_yr1,
	sales_tar_body.sales_tar_body_yr2,
	sales_tar_body.sales_tar_body_yr3,
	sales_tar_body.sales_tar_body_yr4,
	sales_tar_body.sales_tar_body_yr5,
	sales_tar_body.sales_tar_body_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( sales_tar_body, "$[*]" COLUMNS (sales_tar_body_name VARCHAR ( 255 ) PATH "$.sales_tar_body_name",sales_tar_body_yr1 VARCHAR ( 255 ) PATH "$.sales_tar_body_yr1",sales_tar_body_yr2 VARCHAR ( 255 ) PATH "$.sales_tar_body_yr2",sales_tar_body_yr3 VARCHAR ( 255 ) PATH "$.sales_tar_body_yr3",sales_tar_body_yr4 VARCHAR ( 255 ) PATH "$.sales_tar_body_yr4",sales_tar_body_yr5 VARCHAR ( 255 ) PATH "$.sales_tar_body_yr5",sales_tar_body_total VARCHAR ( 255 ) PATH "$.sales_tar_body_total")) AS sales_tar_body
)

,t11 as (
SELECT
	a._id,
	'后付返利投入' as fl,
	rtb_exc.rtb_exc_name,
	'' as type,
	'' as per,
	'' as _explain,
	rtb_exc.rtb_exc_yr1,
	rtb_exc.rtb_exc_yr2,
	rtb_exc.rtb_exc_yr3,
	rtb_exc.rtb_exc_yr4,
	rtb_exc.rtb_exc_yr5,
	rtb_exc.rtb_exc_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( rtb_exc, "$[*]" COLUMNS (rtb_exc_name VARCHAR ( 255 ) PATH "$.rtb_exc_name",rtb_exc_total VARCHAR ( 255 ) PATH "$.rtb_exc_total",rtb_exc_yr1 VARCHAR ( 255 ) PATH "$.rtb_exc_yr1",rtb_exc_yr2 VARCHAR ( 255 ) PATH "$.rtb_exc_yr2",rtb_exc_yr3 VARCHAR ( 255 ) PATH "$.rtb_exc_yr3",rtb_exc_yr4 VARCHAR ( 255 ) PATH "$.rtb_exc_yr4",rtb_exc_yr5 VARCHAR ( 255 ) PATH "$.rtb_exc_yr5")) AS rtb_exc
)
,t12 as (
SELECT
	a._id,
	'项目投资总计' as fl,
	pro_inv.pro_inv_name,
	'' as type,
	'' as per,
	'' as _explain,
	pro_inv.pro_inv_yr1,
	pro_inv.pro_inv_yr2,
	pro_inv.pro_inv_yr3,
	pro_inv.pro_inv_yr4,
	pro_inv.pro_inv_yr5,
	pro_inv.pro_inv_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( pro_inv, "$[*]" COLUMNS (pro_inv_name VARCHAR ( 255 ) PATH "$.pro_inv_name",pro_inv_total VARCHAR ( 255 ) PATH "$.pro_inv_total",pro_inv_yr1 VARCHAR ( 255 ) PATH "$.pro_inv_yr1",pro_inv_yr2 VARCHAR ( 255 ) PATH "$.pro_inv_yr2",pro_inv_yr3 VARCHAR ( 255 ) PATH "$.pro_inv_yr3",pro_inv_yr4 VARCHAR ( 255 ) PATH "$.pro_inv_yr4",pro_inv_yr5 VARCHAR ( 255 ) PATH "$.pro_inv_yr5")) AS pro_inv
)
,t13 as (
SELECT
	a._id,
	'项目投资总计' as fl,
	dis_pro_inv.dis_pro_inv_name,
	'' as type,
	'' as per,
	'' as _explain,
	dis_pro_inv.dis_pro_inv_yr1,
	dis_pro_inv.dis_pro_inv_yr2,
	dis_pro_inv.dis_pro_inv_yr3,
	dis_pro_inv.dis_pro_inv_yr4,
	dis_pro_inv.dis_pro_inv_yr5,
	dis_pro_inv.dis_pro_inv_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( dis_pro_inv, "$[*]" COLUMNS (dis_pro_inv_name VARCHAR ( 255 ) PATH "$.dis_pro_inv_name",dis_pro_inv_total VARCHAR ( 255 ) PATH "$.dis_pro_inv_total",dis_pro_inv_yr1 VARCHAR ( 255 ) PATH "$.dis_pro_inv_yr1",dis_pro_inv_yr2 VARCHAR ( 255 ) PATH "$.dis_pro_inv_yr2",dis_pro_inv_yr3 VARCHAR ( 255 ) PATH "$.dis_pro_inv_yr3",dis_pro_inv_yr4 VARCHAR ( 255 ) PATH "$.dis_pro_inv_yr4",dis_pro_inv_yr5 VARCHAR ( 255 ) PATH "$.dis_pro_inv_yr5")) AS dis_pro_inv
)
,t14 as (
SELECT
	a._id,
	'MM Sales @ Body Shop Selling Price (incl. VAT)' as fl,
	mm_sabs.mm_sabs_name,
	'' as type,
	'' as per,
	'' as _explain,
	mm_sabs.mm_sabs_yr1,
	mm_sabs.mm_sabs_yr2,
	mm_sabs.mm_sabs_yr3,
	mm_sabs.mm_sabs_yr4,
	mm_sabs.mm_sabs_yr5,
	mm_sabs.mm_sabs_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( mm_sabs, "$[*]" COLUMNS (mm_sabs_name VARCHAR ( 255 ) PATH "$.mm_sabs_name",mm_sabs_yr1 VARCHAR ( 255 ) PATH "$.mm_sabs_yr1",mm_sabs_yr2 VARCHAR ( 255 ) PATH "$.mm_sabs_yr2",mm_sabs_yr3 VARCHAR ( 255 ) PATH "$.mm_sabs_yr3",mm_sabs_yr4 VARCHAR ( 255 ) PATH "$.mm_sabs_yr4",mm_sabs_yr5 VARCHAR ( 255 ) PATH "$.mm_sabs_yr5",mm_sabs_total VARCHAR ( 255 ) PATH "$.mm_sabs_total")) AS mm_sabs
)
,t15 as (
SELECT
	a._id,
	'MM Sales @ PPG Selling Price (incl. VAT)' as fl,
	mm_saps.mm_saps_name,
	'' as type,
	'' as per,
	'' as _explain,
	mm_saps.mm_saps_yr1,
	mm_saps.mm_saps_yr2,
	mm_saps.mm_saps_yr3,
	mm_saps.mm_saps_yr4,
	mm_saps.mm_saps_yr5,
	mm_saps.mm_saps_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( mm_saps, "$[*]" COLUMNS (mm_saps_name VARCHAR ( 255 ) PATH "$.mm_saps_name",mm_saps_yr1 VARCHAR ( 255 ) PATH "$.mm_saps_yr1",mm_saps_yr2 VARCHAR ( 255 ) PATH "$.mm_saps_yr1",mm_saps_yr3 VARCHAR ( 255 ) PATH "$.mm_saps_yr1",mm_saps_yr4 VARCHAR ( 255 ) PATH "$.mm_saps_yr1",mm_saps_yr5 VARCHAR ( 255 ) PATH "$.mm_saps_yr1",mm_saps_total VARCHAR ( 255 ) PATH "$.mm_saps_total")) AS mm_saps
)
,t16 as (
SELECT
	a._id,
	'合同签约承诺含税销售额' as fl,
	scc.scc_name,
	'' as type,
	'' as per,
	scc.scc_explain,
	scc.scc_yr1,
	scc.scc_yr2,
	scc.scc_yr3,
	scc.scc_yr4,
	scc.scc_yr5,
	scc.scc_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( scc, "$[*]" COLUMNS (scc_name VARCHAR ( 255 ) PATH "$.scc_name",scc_explain VARCHAR ( 255 ) PATH "$.scc__explain",scc_yr1 VARCHAR ( 255 ) PATH "$.scc_yr1",scc_yr2 VARCHAR ( 255 ) PATH "$.scc_yr2",scc_yr3 VARCHAR ( 255 ) PATH "$.scc_yr3",scc_yr4 VARCHAR ( 255 ) PATH "$.scc_yr4",scc_yr5 VARCHAR ( 255 ) PATH "$.scc_yr5",scc_total VARCHAR ( 255 ) PATH "$.scc_total")) AS scc
)
,t17 as (
SELECT
	a._id,
	'Sales Adjustment' as fl,
	sale_agj.sale_agj_name,
	'' as type,
	'' as per,
	'' as _explain,
	sale_agj.sale_agj_yr1,
	sale_agj.sale_agj_yr2,
	sale_agj.sale_agj_yr3,
	sale_agj.sale_agj_yr4,
	sale_agj.sale_agj_yr5,
	sale_agj.sale_agj_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( sale_agj, "$[*]" COLUMNS (sale_agj_name VARCHAR ( 255 ) PATH "$.sale_agj_name",sale_agj_yr1 VARCHAR ( 255 ) PATH "$.sale_agj_yr1",sale_agj_yr2 VARCHAR ( 255 ) PATH "$.sale_agj_yr2",sale_agj_yr3 VARCHAR ( 255 ) PATH "$.sale_agj_yr3",sale_agj_yr4 VARCHAR ( 255 ) PATH "$.sale_agj_yr4",sale_agj_yr5 VARCHAR ( 255 ) PATH "$.sale_agj_yr5",sale_agj_total VARCHAR ( 255 ) PATH "$.sale_agj_total")) AS sale_agj
)
,t18 as (
SELECT
	a._id,
	'ebate Adjustment' as fl,
	rtb_dis_amo.rtb_dis_amo_name,
	'' as type,
	'' as per,
	'' as _explain,
	rtb_dis_amo.rtb_dis_amo_yr1,
	rtb_dis_amo.rtb_dis_amo_yr2,
	rtb_dis_amo.rtb_dis_amo_yr3,
	rtb_dis_amo.rtb_dis_amo_yr4,
	rtb_dis_amo.rtb_dis_amo_yr5,
	rtb_dis_amo.rtb_dis_amo_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( rtb_dis_amo, "$[*]" COLUMNS (
	rtb_dis_amo_name VARCHAR ( 255 ) PATH "$.rtb_dis_amo_name",rtb_dis_amo_yr1 VARCHAR ( 255 ) PATH "$.rtb_dis_amo_yr1",rtb_dis_amo_yr2 VARCHAR ( 255 ) PATH "$.rtb_dis_amo_yr2",rtb_dis_amo_yr3 VARCHAR ( 255 ) PATH "$.rtb_dis_amo_yr3",rtb_dis_amo_yr4 VARCHAR ( 255 ) PATH "$.rtb_dis_amo_yr4",rtb_dis_amo_yr5 VARCHAR ( 255 ) PATH "$.rtb_dis_amo_yr5",rtb_dis_amo_total VARCHAR ( 255 ) PATH "$.rtb_dis_amo_total")) AS rtb_dis_amo
)
,t19 as (
SELECT
	a._id,
	'PPG Net Sales (excl. VAT)' as fl,
	ppg_net.ppg_net_name,
	'' as type,
	'' as per,
	'' as _explain,
	ppg_net.ppg_net_yr1,
	ppg_net.ppg_net_yr2,
	ppg_net.ppg_net_yr3,
	ppg_net.ppg_net_yr4,
	ppg_net.ppg_net_yr5,
	ppg_net.ppg_net_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( ppg_net, "$[*]" COLUMNS (ppg_net_name VARCHAR ( 255 ) PATH "$.ppg_net_name",ppg_net_yr1 VARCHAR ( 255 ) PATH "$.ppg_net_yr1",ppg_net_yr2 VARCHAR ( 255 ) PATH "$.ppg_net_yr2",ppg_net_yr3 VARCHAR ( 255 ) PATH "$.ppg_net_yr3",ppg_net_yr4 VARCHAR ( 255 ) PATH "$.ppg_net_yr4",ppg_net_yr5 VARCHAR ( 255 ) PATH "$.ppg_net_yr5",ppg_net_total VARCHAR ( 255 ) PATH "$.ppg_net_total")) AS ppg_net
)
,t20 as (
SELECT
	a._id,
	'Product Cost' as fl,
	pro_cost.pro_cost_name,
	'' as type,
	'' as per,
	'' as _explain,
	pro_cost.pro_cost_yr1,
	pro_cost.pro_cost_yr2,
	pro_cost.pro_cost_yr3,
	pro_cost.pro_cost_yr4,
	pro_cost.pro_cost_yr5,
	pro_cost.pro_cost_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( pro_cost, "$[*]" COLUMNS (pro_cost_name VARCHAR ( 255 ) PATH "$.pro_cost_name",pro_cost_yr1 VARCHAR ( 255 ) PATH "$.pro_cost_yr1",pro_cost_yr2 VARCHAR ( 255 ) PATH "$.pro_cost_yr2",pro_cost_yr3 VARCHAR ( 255 ) PATH "$.pro_cost_yr3",pro_cost_yr4 VARCHAR ( 255 ) PATH "$.pro_cost_yr4",pro_cost_yr5 VARCHAR ( 255 ) PATH "$.pro_cost_yr5",pro_cost_total VARCHAR ( 255 ) PATH "$.pro_cost_total")) AS pro_cost
)
,t21 as (
SELECT
	a._id,
	'PC Adjustment' as fl,
	pc_adj.pc_adj_name,
	'' as type,
	'' as per,
	'' as _explain,
	pc_adj.pc_adj_yr1,
	pc_adj.pc_adj_yr2,
	pc_adj.pc_adj_yr3,
	pc_adj.pc_adj_yr4,
	pc_adj.pc_adj_yr5,
	pc_adj.pc_adj_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( pc_adj, "$[*]" COLUMNS (
	pc_adj_name VARCHAR ( 255 ) PATH "$.pc_adj_name",pc_adj_yr1 VARCHAR ( 255 ) PATH "$.pc_adj_yr1",pc_adj_yr2 VARCHAR ( 255 ) PATH "$.pc_adj_yr2",pc_adj_yr3 VARCHAR ( 255 ) PATH "$.pc_adj_yr3",pc_adj_yr4 VARCHAR ( 255 ) PATH "$.pc_adj_yr4",pc_adj_yr5 VARCHAR ( 255 ) PATH "$.pc_adj_yr5",pc_adj_total VARCHAR ( 255 ) PATH "$.pc_adj_total")) AS pc_adj
)
,t22 as (
SELECT
	a._id,
	'PC' as fl,
	pc.pc_name,
	'' as type,
	'' as per,
	'' as _explain,
	pc.pc_yr1,
	pc.pc_yr2,
	pc.pc_yr3,
	pc.pc_yr4,
	pc.pc_yr5,
	pc.pc_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( pc, "$[*]" COLUMNS (pc_name VARCHAR ( 255 ) PATH "$.pc_name",pc_yr1 VARCHAR ( 255 ) PATH "$.pc_yr1",pc_yr2 VARCHAR ( 255 ) PATH "$.pc_yr2",pc_yr3 VARCHAR ( 255 ) PATH "$.pc_yr3",pc_yr4 VARCHAR ( 255 ) PATH "$.pc_yr4",pc_yr5 VARCHAR ( 255 ) PATH "$.pc_yr5",pc_total VARCHAR ( 255 ) PATH "$.pc_total"
	)) AS pc
)
,t23 as (
SELECT
	a._id,
	'Direct Overhead' as fl,
	dir_over.dir_over_name,
	'' as type,
	'' as per,
	'' as _explain,
	dir_over.dir_over_yr1,
	dir_over.dir_over_yr2,
	dir_over.dir_over_yr3,
	dir_over.dir_over_yr4,
	dir_over.dir_over_yr5,
	dir_over.dir_over_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( dir_over, "$[*]" COLUMNS (
	dir_over_name VARCHAR ( 255 ) PATH "$.dir_over_name",
	dir_over_yr1 VARCHAR ( 255 ) PATH "$.dir_over_yr1",
	dir_over_yr2 VARCHAR ( 255 ) PATH "$.dir_over_yr2",
	dir_over_yr3 VARCHAR ( 255 ) PATH "$.dir_over_yr3",
	dir_over_yr4 VARCHAR ( 255 ) PATH "$.dir_over_yr4",
	dir_over_yr5 VARCHAR ( 255 ) PATH "$.dir_over_yr5",
	dir_over_total VARCHAR ( 255 ) PATH "$.dir_over_total"
	)) AS dir_over
)
,t24 as (
SELECT
	a._id,
	'Overhead Adjustment' as fl,
	over_adj.over_adj_name,
	'' as type,
	'' as per,
	'' as _explain,
	over_adj.over_adj_yr1,
	over_adj.over_adj_yr2,
	over_adj.over_adj_yr3,
	over_adj.over_adj_yr4,
	over_adj.over_adj_yr5,
	over_adj.over_adj_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( over_adj, "$[*]" COLUMNS (over_adj_name VARCHAR ( 255 ) PATH "$.over_adj_name",over_adj_yr1 VARCHAR ( 255 ) PATH "$.over_adj_yr1",over_adj_yr2 VARCHAR ( 255 ) PATH "$.over_adj_yr2",over_adj_yr3 VARCHAR ( 255 ) PATH "$.over_adj_yr3",over_adj_yr4 VARCHAR ( 255 ) PATH "$.over_adj_yr4",over_adj_yr5 VARCHAR ( 255 ) PATH "$.over_adj_yr5",over_adj_total VARCHAR ( 255 ) PATH "$.over_adj_total")) AS over_adj	
)
,t25 as (
SELECT
	a._id,
	'Total Overhead' as fl,
	tt_over.tt_over_name,
	'' as type,
	'' as per,
	'' as _explain,
	tt_over.tt_over_yr1,
	tt_over.tt_over_yr2,
	tt_over.tt_over_yr3,
	tt_over.tt_over_yr4,
	tt_over.tt_over_yr5,
	tt_over.tt_over_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( tt_over, "$[*]" COLUMNS (tt_over_name VARCHAR ( 255 ) PATH "$.tt_over_name",tt_over_yr1 VARCHAR ( 255 ) PATH "$.tt_over_yr1",tt_over_yr2 VARCHAR ( 255 ) PATH "$.tt_over_yr2",tt_over_yr3 VARCHAR ( 255 ) PATH "$.tt_over_yr3",tt_over_yr4 VARCHAR ( 255 ) PATH "$.tt_over_yr4",tt_over_yr5 VARCHAR ( 255 ) PATH "$.tt_over_yr5",tt_over_total VARCHAR ( 255 ) PATH "$.tt_over_total")) AS tt_over
)
,t26 as (
SELECT
	a._id,
	'Transfer Premium Expense' as fl,
	tpe.tpe_name,
	'' as type,
	'' as per,
	'' as _explain,
	tpe.tpe_yr1,
	tpe.tpe_yr2,
	tpe.tpe_yr3,
	tpe.tpe_yr4,
	tpe.tpe_yr5,
	tpe.tpe_totla
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( tpe, "$[*]" COLUMNS (tpe_name VARCHAR ( 255 ) PATH "$.tpe_name",tpe_yr1 VARCHAR ( 255 ) PATH "$.tpe_yr1",tpe_yr2 VARCHAR ( 255 ) PATH "$.tpe_yr2",tpe_yr3 VARCHAR ( 255 ) PATH "$.tpe_yr3",tpe_yr4 VARCHAR ( 255 ) PATH "$.tpe_yr4",tpe_yr5 VARCHAR ( 255 ) PATH "$.tpe_yr5",tpe_totla VARCHAR ( 255 ) PATH "$.tpe_totla")) AS tpe
)
,t27 as (
SELECT
	a._id,
	'Other Adjustment' as fl,
	tt_other_exp.tt_other_exp_name,
	'' as type,
	'' as per,
	'' as _explain,
	tt_other_exp.tt_other_exp_yr1,
	tt_other_exp.tt_other_exp_yr2,
	tt_other_exp.tt_other_exp_yr3,
	tt_other_exp.tt_other_exp_yr4,
	tt_other_exp.tt_other_exp_yr5,
	tt_other_exp.tt_other_exp_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE (tt_other_exp, "$[*]" COLUMNS (tt_other_exp_name VARCHAR ( 255 ) PATH "$.tt_other_exp_name",tt_other_exp_yr1 VARCHAR ( 255 ) PATH "$.tt_other_exp_yr1",tt_other_exp_yr2 VARCHAR ( 255 ) PATH "$.tt_other_exp_yr2",tt_other_exp_yr3 VARCHAR ( 255 ) PATH "$.tt_other_exp_yr3",tt_other_exp_yr4 VARCHAR ( 255 ) PATH "$.tt_other_exp_yr4",tt_other_exp_yr5 VARCHAR ( 255 ) PATH "$.tt_other_exp_yr5",tt_other_exp_total VARCHAR ( 255 ) PATH "$.tt_other_exp_total")) AS tt_other_exp
)
,t28 as (
SELECT
	a._id,
	'EBIT' as fl,
	ebit.ebit_name,
	'' as type,
	'' as per,
	'' as _explain,
	ebit.ebit_yr1,
	ebit.ebit_yr2,
	ebit.ebit_yr3,
	ebit.ebit_yr4,
	ebit.ebit_yr5,
	ebit.ebit_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE ( ebit, "$[*]" COLUMNS (ebit_name VARCHAR ( 255 ) PATH "$.ebit_name",ebit_yr1 VARCHAR ( 255 ) PATH "$.ebit_yr1",ebit_yr2 VARCHAR ( 255 ) PATH "$.ebit_yr2",ebit_yr3 VARCHAR ( 255 ) PATH "$.ebit_yr3",ebit_yr4 VARCHAR ( 255 ) PATH "$.ebit_yr4",ebit_yr5 VARCHAR ( 255 ) PATH "$.ebit_yr5",ebit_total VARCHAR ( 255 ) PATH "$.ebit_total"
	)) AS ebit
)
,t29 as (
SELECT
	a._id,
	'Cash Flow' as fl,
	cf.cf_name,
	'' as type,
	'' as per,
	'' as _explain,
	cf.cf_yr1,
	cf.cf_yr2,
	cf.cf_yr3,
	cf.cf_yr4,
	cf.cf_yr5,
	cf.cf_total
FROM
	fine_ods.BIA1 a,
	JSON_TABLE (cf, "$[*]" COLUMNS (cf_name VARCHAR ( 255 ) PATH "$.cf_name",cf_yr1 VARCHAR ( 255 ) PATH "$.cf_yr1",cf_yr2 VARCHAR ( 255 ) PATH "$.cf_yr2",cf_yr3 VARCHAR ( 255 ) PATH "$.cf_yr3",cf_yr4 VARCHAR ( 255 ) PATH "$.cf_yr4",cf_yr5 VARCHAR ( 255 ) PATH "$.cf_yr5",cf_total VARCHAR ( 255 ) PATH "$.cf_total")) AS cf
)
SELECT
	_id,
	fl as sec,
	name,
	type,
	per,
	_explain,
	yr1,
	yr2,
	yr3,
	yr4,
	yr5,
	total,
	'fine_ods.BIA1' AS data_resource,
	now() AS etl_time 
FROM(
select * from t1 
union all 
select * from t2
union all 
select * from t3 
union all 
select * from t4 
union all 
select * from t5 
union all 
select * from t6
union all 
select * from t7 
union all 
select * from t8 
union all 
select * from t9 
union all 
select * from t10 
union all 
select * from t11
union all 
select * from t12 
union all 
select * from t13
union all 
select * from t14
union all 
select * from t15
union all 
select * from t16
union all
select * from t17 
union all 
select * from t18
union all 
select * from t19
union all 
select * from t20
union all 
select * from t21
union all 
select * from t22
union all 
select * from t23
union all 
select * from t24
union all 
select * from t25
union all 
select * from t26
union all 
select * from t27
union all 
select * from t28
union all 
select * from t29
) a 
order by _id
-- 带年份的计算数值 