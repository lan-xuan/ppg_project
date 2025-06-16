/*TRUNCATE TABLE fine_dm.dm_mm_service_report;
*/
-- 1770 1760 1754 1762 1724 1778
WITH  cal as 
(
	SELECT 
		DISTINCT
-- 				master1.customer_code vendor_code,
        cal.actual_month sales_month,
				cal.actual_quarter sales_quarter,
				cal.actual_year sales_year
    FROM fine_ods.ods_calendar_info_df cal
 where actual_year = ${mysql_yesterday_d_year}


), cal_q as 
(
	SELECT 
		DISTINCT
-- 				master1.customer_code vendor_code,
				cal.actual_quarter sales_quarter,
				cal.actual_year sales_year
    FROM fine_ods.ods_calendar_info_df cal
 where actual_year = ${mysql_yesterday_d_year}


),
temp_dw_customer_master_list as (
select DISTINCT u_customer_code,u_customer_name,district,sales_month from fine_dw.dw_customer_master_list
 where  SUBSTRING(sales_month,1,4) =${mysql_yesterday_d_year}


)

,a as (
	SELECT 
		p.vendor_code,
-- 		cal.proj_name,
		p.sales_value,
		p.district,
		cal.sales_month,
		cal.sales_quarter,
		cal.sales_year,
		p.proj_name as sec_1
	FROM cal
	LEFT JOIN
	(
		SELECT 
		sum(sales_value) sales_value,
		vendor_code,
		proj_name,
		district,
		sales_month,
        proj_name as sec_1
		FROM
		fine_dw.dw_transaction_detail_report
		WHERE channel = 'MM' -- 'distributor' -- 'SCHOOL'-- 'MSO'
-- 		and proj_name = '中升'
-- 		and vendor_code = '205534'
		and ship_to_code <> '333878' -- 不需要大众仓
        and ship_to_code <> '523444' -- 不需要大众仓
		GROUP BY
		vendor_code,
		proj_name,
		district,
		sales_month
		
		UNION ALL
		SELECT 
		sum(sales_value) sales_value,
		vendor_code,
		'total' proj_name,
		district,
		sales_month,
    'total' as sec_1
		FROM
		fine_dw.dw_transaction_detail_report
		WHERE channel = 'MM' -- 'distributor' -- 'SCHOOL'-- 'MSO'
-- 		and proj_name = '中升'
-- 		and vendor_code = '205534'
		and ship_to_code <> '333878' -- 不需要大众仓
        and ship_to_code <> '523444' -- 不需要大众仓
		GROUP BY
		vendor_code,
-- 		proj_name,
		district,
		sales_month
	)p
	on cal.sales_month = p.sales_month
)
-- SELECT * from actual;
, actual	as 
(   
	SELECT
            t.vendor_code,
            m.u_customer_name as vendor_name,
						m.district,
            'MM' channel,
            t.sales_value,
            t.sales_month,
            t.sales_quarter,
            t.sales_year,
						t.sec_1
    FROM a t
    LEFT JOIN temp_dw_customer_master_list m
    on t.vendor_code = m.u_customer_code
	and t.sales_month = m.sales_month
    -- and STR_TO_DATE(m.starting_date, '%Y%m%d') <= STR_TO_DATE(CONCAT(sales_month,'01'), '%Y%m%d')
    -- and STR_TO_DATE(m.ending_date, '%Y%m%d') >= STR_TO_DATE(CONCAT(sales_month,'01'), '%Y%m%d')

		
-- WHERE vendor_code = '101164'
    -- ORDER BY orderno.order_no
)		

SELECT
-- DISTINCT order_no,order_month,name_1
	   	t.vendor_code,
        t.vendor_name,
	    	t.district,
        t.channel,
	     cs.team_owner,
		    cs.team_owner_id,
        cs.sales_person,
	    	cs.sales_person_id,
        t.sales_value,
        t.sales_month,
        t.sales_quarter,
        t.sales_year,
        orderno.report_year,
        	 STR_TO_DATE(CONCAT( orderno.report_year,'0101') , '%Y%m%d') as report_date,
				order_no,
				orderno.sec_1,
				orderno.sec_2,
				orderno.sec_3,
			 '' as data_resource,
		   now() as etl_time
		   
						
from (
		SELECT 

						vendor_code,
						vendor_name,
						district,
						channel,
						sales_value,
						sales_month,
						sales_quarter,
						sales_year,
						sec_1
		FROM actual
		UNION ALL
		SELECT 

						vendor_code,
						vendor_name,
						district,
						channel,
						sum(sales_value) as sales_value,
						sales_quarter as sales_month,
						sales_quarter,
						sales_year,
						sec_1
		FROM actual
		GROUP BY 
						vendor_code,
						vendor_name,
						district,
						channel,
		--         sales_quarter as sales_month,
						sales_quarter,
						sales_year,
						sec_1
		UNION ALL
		SELECT 

						vendor_code,
						vendor_name,
						district,
						channel,
						sum(sales_value) as sales_value,
						sales_year,
						sales_year,
						sales_year,
						sec_1
		FROM actual
		GROUP BY 
						vendor_code,
						vendor_name,
						district,
						channel,
						sales_year,
						sec_1
) t
LEFT JOIN fine_dw.dw_order_report orderno
    on orderno.name_1 = t.sec_1
    and orderno.order_month = t.sales_month
    and orderno.sec_year = t.sales_year
    and orderno.order_no is not null
    and orderno.order_report = 'mm_service_report'
LEFT JOIN fine_dw.dw_cs_relationship_info cs
    on t.vendor_code = cs.customer_code
	and t.district = cs.district
    and t.sales_year = cs.s_year
where vendor_code is not null
 and orderno.report_year = ${mysql_yesterday_d_year} -- 具体报表的年


		ORDER BY orderno.order_no
