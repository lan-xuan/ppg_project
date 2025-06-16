/*
目标表：fine_dw.dw_svc_detail
来源表：
fine_ods.ods_svc_sh_df
fine_ods.ods_calendar_info_df
fine_ods.ods_svc_fb_df
fine_ods.ods_svc_js_df 
fine_ods.ods_svc_js_df 
fine_ods.ods_svc_bn_df 
fine_ods.ods_sales_pc_customer_df
fine_ods.ods_transaction_detail_report_df 
更新方式：全量更新

*/

with a as(
SELECT
    SUBSTR(svc_quarter, 1, 4) as svc_year, -- 所属财年
    actual_month as svc_month, -- 所属月份
    svc_quarter, -- 所属季度
    item_code, -- 产品编号
    svc,
    'sh' table_name
FROM
    fine_ods.ods_svc_sh_df
    LEFT JOIN fine_ods.ods_calendar_info_df ON svc_quarter = actual_quarter
-- 		where item_code = 'D800/5L-C3'
), b as(
SELECT
    SUBSTR(svc_quarter, 1, 4) as svc_year, -- 所属财年
    actual_month as svc_month, -- 所属月份
    svc_quarter, -- 所属季度
    item_code, -- 产品编号
    svc,
    'fb' table_name
FROM
    fine_ods.ods_svc_fb_df
    LEFT JOIN fine_ods.ods_calendar_info_df ON svc_quarter = actual_quarter
-- 		where item_code = 'D800/5L-C3'
), c as(
SELECT
    SUBSTR(svc_quarter, 1, 4) as svc_year, -- 所属财年
    actual_month as svc_month, -- 所属月份
    svc_quarter, -- 所属季度
    item_code, -- 产品编号
    svc,
    'js' table_name
FROM
    fine_ods.ods_svc_js_df 
    LEFT JOIN fine_ods.ods_calendar_info_df ON svc_quarter = actual_quarter
-- 		where item_code = 'D800/5L-C3'
), d as(
SELECT
    SUBSTR(svc_quarter, 1, 4) as svc_year, -- 所属财年
    actual_month as svc_month, -- 所属月份
    svc_quarter, -- 所属季度
    item_code, -- 产品编号
    svc,
    'bn' table_name
FROM
    fine_ods.ods_svc_bn_df 
    LEFT JOIN fine_ods.ods_calendar_info_df ON svc_quarter = actual_quarter
-- 		where item_code = 'D800/5L-C3'
), e as(
SELECT
    SUBSTR(svc_quarter, 1, 4) as svc_year, -- 所属财年
    actual_month as svc_month, -- 所属月份
    svc_quarter, -- 所属季度
    item_code, -- 产品编号
    svc,
    'sj' table_name
FROM
    fine_ods.ods_svc_sj_df 
    LEFT JOIN fine_ods.ods_calendar_info_df ON svc_quarter = actual_quarter
-- 		where item_code = 'D800/5L-C3'
), svc_1 as(
SELECT svc_year,svc_month, svc_quarter, item_code, svc, table_name
FROM (
    SELECT svc_year,svc_month, svc_quarter, item_code, svc, table_name,
           ROW_NUMBER() OVER (PARTITION BY svc_year,svc_month, svc_quarter, item_code ORDER BY order_no) AS rn
    FROM (
        SELECT svc_year,svc_month, svc_quarter, item_code, svc, 'sh' AS table_name,1 order_no FROM a
        UNION
        SELECT svc_year,svc_month, svc_quarter, item_code, svc, 'fj' AS table_name,2 order_no FROM b
        UNION
        SELECT svc_year,svc_month, svc_quarter, item_code, svc, 'js' AS table_name,3 order_no FROM c
        UNION
		    SELECT svc_year,svc_month, svc_quarter, item_code, svc, 'bn' AS table_name,4 order_no FROM d
        UNION
        SELECT svc_year,svc_month, svc_quarter, item_code, svc, 'sj' AS table_name,5 order_no FROM e
    ) AS all_tables
) AS ranked_tables
WHERE rn = 1
),
temp_001 as (
                  select distinct  item_code from  fine_ods.ods_sales_pc_customer_df t where item_code NOT LIKE '.%'  and item_code  <> ''
					union all 
					select distinct item_code from  fine_ods.ods_transaction_detail_report_df t where item_code = '.Price Adj' 
		


),

transaction_pc_svc as(

		SELECT
			DISTINCT
			actual_year as svc_year,
			actual_month as svc_month,
			actual_quarter as svc_quarter,
			t.item_code,
			(sales_value - pc)/sales_qty as svc,
			'ods_sales_pc_customer_df' as table_name
			
			FROM fine_ods.ods_sales_pc_customer_df t
			LEFT JOIN fine_ods.ods_calendar_info_df 
             ON STR_TO_DATE(concat(actual_month,'01'), '%Y%m%d') = STR_TO_DATE(sales_date, '%Y%m%d')
            inner join temp_001
            on t.item_code = temp_001.item_code 
            

			

			WHERE 1=1
				-- 第5步：gl_class not in ('2741 - REF, AUTOCOLOUR ASIA','2999 - REF, ASIA/PACIFIC, CHINA, IRD')
				and UPPER(t.gl_class) not in ('2741 - REF, AUTOCOLOUR ASIA','2999 - REF, ASIA/PACIFIC, CHINA, IRD')
				
				-- 第6步：剔除item_code like '.%'   and item_code <>'' 但是保留item_code  = '.Price Adj'
			--	and t.item_code in
			--	(
			--		select distinct  item_code from  fine_ods.ods_sales_pc_customer_df t where item_code NOT LIKE '.%'  and item_code  <> ''
			--		union all 
			--		select distinct item_code from  fine_ods.ods_transaction_detail_report_df t where item_code = '.Price Adj' 
			--	)

				-- 第7步：customer_code not in ods_customer_filter_df.customer_code
				and REPLACE(t.customer_code,'CN','') not in  (SELECT distinct customer_code from fine_dw.dw_customer_filter)
				and not EXISTS (SELECT 1 from svc_1 where svc_1.item_code = t.item_code and svc_1.svc_month = substr(t.sales_date,1,6))
				and sales_value <> 0 
				and sales_qty <> 0
)




SELECT DISTINCT  svc_year,svc_month,svc_quarter,item_code,svc,table_name,data_resource, SYSDATE() as etl_time from(
SELECT distinct svc_year,svc_month,svc_quarter,item_code,ROUND(svc, 2) as svc,table_name,'ods_svc_df' as data_resource, SYSDATE() as etl_time  from svc_1
UNION
SELECT distinct svc_year,svc_month,svc_quarter,item_code,ROUND(svc, 2) as svc,table_name,'ods_sales_pc_customer_df' as data_resource, SYSDATE() as etl_time  from transaction_pc_svc
)ssss
