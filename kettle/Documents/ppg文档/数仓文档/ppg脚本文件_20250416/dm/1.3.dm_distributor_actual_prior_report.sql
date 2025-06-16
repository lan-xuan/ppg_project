-- dm_distributor_actual_target_report


/*目标表：fine_dm.dm_distributor_actual_target_report
  来源表：

fine_dw.dw_customer_master_list
fine_dw.dw_transaction_detail_report
fine_dw.dw_cb_detail
fine_dw.dw_ireport_sub_brand 
fine_dw.dw_distributor_sales_target 
fine_dw.dw_cs_relationship_info
fine_dw.dw_order_report
fine_ods.ods_calendar_info_df

更新方式：增量更新
更新粒度：年
更新字段：sales_year

*/

with cal as(
    	SELECT 
		DISTINCT
        cal.actual_month sales_month,
				cal.actual_quarter sales_quarter,
				cal.actual_year sales_year
    FROM fine_ods.ods_calendar_info_df cal
		WHERE actual_year = ${mysql_yesterday_d_year}
)
, target as(
SELECT
    sales_target/3 as sales_value,
    customer_code,
    district,
    team_owner_id,
    team_owner,
    sales_person_id,
    sales_person,
    customer_name,
    target_year,
    target_quarter,
    sec_3 sec_1,
    sales_month,
    'DISTRIBUTOR' channel,
    sales_year as report_year,
    report_date

FROM   cal
LEFT JOIN fine_dm.dm_distributor_target_report target
ON cal.sales_quarter = target.target_quarter
UNION ALL -- 季度数据
SELECT
    sales_target as sales_value,
    customer_code,
    district,
    team_owner_id,
    team_owner,
    sales_person_id,
    sales_person,
    customer_name,
    target_year,
    target_quarter as target_quarter,
    sec_3 sec_1,
    target_quarter as sales_month,
    'DISTRIBUTOR' channel,
    target_year as report_year,
    report_date
FROM fine_dm.dm_distributor_target_report target
WHERE substr(sec_2,1,1) = 'Q'
UNION ALL -- 年数据
SELECT
    sales_target as sales_value,
    customer_code,
    district,
    team_owner_id,
    team_owner,
    sales_person_id,
    sales_person,
    customer_name,
    target_year,
    target_quarter,
    sec_3 sec_1,
    target_year as sales_month,
    'DISTRIBUTOR' channel,
    target_year as report_year,
    report_date
FROM fine_dm.dm_distributor_target_report target
WHERE sec_2 = CONCAT('FY ' ,${mysql_yesterday_d_year})
  or substr(sec_2,1,1) = 'Q'
) -- SELECT DISTINCT sec_1,target_year,target_quarter,sales_month FROM target ;
,actual_1 as(
SELECT 
-- 0 sales_value,
    sum(sales_value) as sales_value,
		case when name_1 like '%Total' then 'Total' else name_1 end as name_1,
		sec_3,
    customer_code,
    district ,
    team_owner_id,
    team_owner,
    sales_person_id,
    sales_person,
    customer_name,
    sales_year,
    sales_quarter,
    case 
        when sec_3  like 'Fill Q1%' then concat(substr(sales_month,1,4),'04')
        when sec_3  like 'Fill Q2%' then concat(substr(sales_month,1,4),'07')
        when sec_3  like 'Fill Q3%' then concat(substr(sales_month,1,4),'10') else sales_month end AS sales_month,
    channel,
    report_year,
    report_date

FROM fine_dm.dm_distributor_report t
WHERE (sec_2 = 'BY CATEGORY AND BRAND' and report_year = ${mysql_yesterday_d_year}
and (sec_3  like  '%实际进货量%' or sec_3  like '%Target%'))
or (sec_33 = 'FULL YEAR'
and report_year = ${mysql_yesterday_d_year})
GROUP BY 
		case when name_1 like '%Total' then 'Total' else name_1 end ,
		sec_3,
    customer_code,
    district ,
    team_owner_id,
    team_owner,
    sales_person_id,
    sales_person,
    customer_name,
    sales_year,
    sales_quarter,
    case 
        when sec_3  like 'Fill Q1%' then concat(substr(sales_month,1,4),'04')
        when sec_3  like 'Fill Q2%' then concat(substr(sales_month,1,4),'07')
        when sec_3  like 'Fill Q3%' then concat(substr(sales_month,1,4),'10') else sales_month end ,
    channel,
    report_year,
    report_date
UNION ALL
SELECT 
    sum(sales_value) as sales_value,
		'Total' name_1,
		sec_3,
    customer_code,
    district ,
    team_owner_id,
    team_owner,
    sales_person_id,
    sales_person,
    customer_name,
    sales_year,
    sales_quarter,
    case when sec_3  like 'Fill Q1%' then concat(substr(sales_month,1,4),'04')
        when sec_3  like 'Fill Q2%' then concat(substr(sales_month,1,4),'07')
        when sec_3  like 'Fill Q3%' then concat(substr(sales_month,1,4),'10') else sales_month end  as sales_month,
    channel,
    report_year,
    report_date

FROM fine_dm.dm_distributor_report t
WHERE (1=1
and (sec_1  like  '%MTD %' or sec_1 like  'Total Sales Order(Fill%'))
GROUP BY 
		sec_3,
    customer_code,
    district ,
    team_owner_id,
    team_owner,
    sales_person_id,
    sales_person,
    customer_name,
    sales_year,
    sales_quarter,
    case when sec_3  like 'Fill Q1%' then concat(substr(sales_month,1,4),'04')
        when sec_3  like 'Fill Q2%' then concat(substr(sales_month,1,4),'07')
        when sec_3  like 'Fill Q3%' then concat(substr(sales_month,1,4),'10') else sales_month end ,
    channel,
    report_year,
    report_date
)
-- SELECT sum(sales_value),name_1,sales_month from actual_1 where name_1 = 'Total' GROUP BY name_1,sales_month ; -- GROUP BY 
, actual as(
SELECT 

    sum(sales_value) as sales_value,
    customer_code,
    district ,
    team_owner_id,
    team_owner,
    sales_person_id,
    sales_person,
    customer_name,
    sales_year,
    sales_quarter,
		report_brand_name,
    sales_target_name,
     sales_month,
    channel,
    report_year,
    report_date

FROM  actual_1 t
LEFT JOIN (
SELECT DISTINCT report_brand_name,sales_target_name FROM fine_dw.dw_ireport_sub_brand
UNION ALL
SELECT 'Total','Total'
) brand
on t.name_1 = brand.report_brand_name
WHERE 1=1
-- and sales_target_name is not null
-- and name_1 = 'AQ+'
GROUP BY 
    customer_code,
    district ,
    team_owner_id,
    team_owner,
    sales_person_id,
    sales_person,
    customer_name,
    sales_year,
    sales_quarter,
    sales_target_name,
		report_brand_name,
    sales_month,
    channel,
    report_year,
    report_date
) -- SELECT * from actual where sales_target_name is null;

,t as(
SELECT
        sum(sales_value) as sales_value,
        customer_code,
        district ,
        team_owner_id,
        team_owner,
        sales_person_id,
        sales_person,
        customer_name,
        sales_year,
        sales_quarter,
        sales_target_name,
        sales_month,
        channel,
        report_year,
        report_date
FROM
(
    SELECT 
        sales_value,
        customer_code,
        district ,
        team_owner_id,
        team_owner,
        sales_person_id,
        sales_person,
        customer_name,
        sales_year,
        sales_quarter,
        sales_target_name,
        sales_month,
        channel,
        report_year,
        report_date
    from actual
    union all
    SELECT
        -1*sales_value,
        customer_code,
        district,
        team_owner_id,
        team_owner,
        sales_person_id,
        sales_person,
        customer_name,
        target_year,
        target_quarter,
        sec_1,
        sales_month,
        channel,
        report_year,
        report_date
    from target
) s
GROUP BY
        customer_code,
        district ,
        team_owner_id,
        team_owner,
        sales_person_id,
        sales_person,
        customer_name,
        sales_year,
        sales_quarter,
        sales_target_name,
        sales_month,
        channel,
        report_year,
        report_date
) 
SELECT 
-- sum(sales_value),
-- orderno.order_no,
-- orderno.sec_1,orderno.sec_2,orderno.sec_3
        customer_code,
        customer_name,
        team_owner_id,
        team_owner,
        sales_person_id,
        sales_person,
        district,
        sales_value,
        sales_month,       
		sales_quarter,
		sales_year,
		orderno.order_no,
		orderno.sec_1,
		orderno.sec_2,
		orderno.sec_3,
		'dm_distributor_target_report/dm_distributor_report' as data_resource,
		now() as etl_time,
		report_date

				
from t
		LEFT JOIN fine_dw.dw_order_report orderno
		on upper(orderno.name_1) = upper(t.sales_target_name)
		and orderno.order_month = t.sales_month
		and orderno.order_report = 'distributor_target'
		where orderno.order_no is not null
-- 		GROUP BY orderno.order_no,orderno.sec_1,orderno.sec_2,orderno.sec_3
		ORDER BY orderno.order_no