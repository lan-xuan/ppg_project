
-- TRUNCATE TABLE fine_dm.dm_distributor_actual_prior_report;
-- insert into fine_dm.dm_distributor_actual_prior_report
-- delete from  fine_dm.dm_distributor_actual_prior_report
-- where report_year =${mysql_yesterday_d_year};
-- insert into fine_dm.dm_distributor_actual_prior_report
with t as(
    SELECT 
    -- DISTINCT sec_1,sec_2,sec_3,order_no
    -- order_no,sec_1,sales_month
    district,
    sum(sales_value) as sales_value,
    sales_month,
    -- name_1,
    -- sales_quarter,
    -- sales_year,
    sec_1,
    -- order_no,
    -- sec_2,
    customer_name,
    -- sec_3,
    customer_code,
    team_owner_id,
    team_owner,
    sales_person_id,
    sales_person,
    channel,
    report_year,
    'dm_distributor_report' as data_resource,
    now() as etl_time,
    STR_TO_DATE(CONCAT(report_year,'0101') , '%Y%m%d') as report_date
    FROM(
    SELECT 
    district,
    sales_value,
        case 
            when sec_3  like 'Fill Q1%' then concat(substr(sales_month,1,4),'04')
            when sec_3  like 'Fill Q2%' then concat(substr(sales_month,1,4),'07')
            when sec_3  like 'Fill Q3%' then concat(substr(sales_month,1,4),'10') else sales_month end AS sales_month,
    case when sec_1  like  'MTD%' or sec_1 like '%Total%' then 'Total'  else sec_1 end as sec_1,
    -- case when name_1  like  'MTD%' or name_1 like '%Total%' then 'Total'  else name_1 end as name_1,
    customer_name, 
    customer_code,
    team_owner,
    team_owner_id,
    sales_person,
    sales_person_id,
    channel,${mysql_yesterday_d_year} as report_year
    from fine_dm.dm_distributor_report 
    WHERE (sec_2 = 'BY CATEGORY AND BRAND' and report_year = ${mysql_yesterday_d_year}  and (sec_3  like '%实际进货量%' or sec_3 like 'Fill%' or sec_3 like '%ACTUAL%') )
    or   (sec_33  = 'FULL YEAR' and report_year = ${mysql_yesterday_d_year})
    or (sec_1  like  'MTD%' and report_year = ${mysql_yesterday_d_year})
    OR ( sec_1 like '%Total' and LENGTH(sec_1) = 7 and report_year = ${mysql_yesterday_d_year})

    UNION ALL 
    SELECT 
    district,
    -1*sales_value,
        case 
            when sec_3  like 'Fill Q1%' then concat(substr(sales_month,1,4)-1,'04')
            when sec_3  like 'Fill Q2%' then concat(substr(sales_month,1,4)-1,'07')
            when sec_3  like 'Fill Q3%' then concat(substr(sales_month,1,4)-1,'10') else concat(${mysql_yesterday_d_year},substr(sales_month,5,2)) end AS sales_month,
    case when sec_1  like  'MTD%' or sec_1 like '%Total%' then 'Total'  else sec_1 end as sec_1,
    customer_name, 
    customer_code,
    team_owner,
    team_owner_id,
    sales_person,
    sales_person_id,
    channel,
    ${mysql_yesterday_d_year} as report_year
    from fine_dm.dm_distributor_report 
    WHERE (sec_2 = 'BY CATEGORY AND BRAND' and report_year = ${mysql_yesterday_l_year}  and (sec_3  like '%实际进货量%' or sec_3 like 'Fill%' or sec_3 like '%ACTUAL%') )
    or   (sec_33  = 'FULL YEAR' and report_year = ${mysql_yesterday_l_year})
    or (sec_1  like  'MTD%' and report_year = ${mysql_yesterday_l_year})
    OR ( sec_1 like '%Total' and LENGTH(sec_1) = 7 and report_year = ${mysql_yesterday_l_year})

    )ssss

    GROUP by
    district,
    sales_month,
    sec_1,
    customer_name,
    customer_code,
    team_owner,
    team_owner_id,
    sales_person,
    sales_person_id,
    channel,
    report_year,
    STR_TO_DATE(CONCAT(report_year,'0101') , '%Y%m%d')
    -- ORDER BY order_no
)-- SELECT * from t;
    SELECT 
    -- DISTINCT sec_1,sec_2,sec_3,order_no
    -- order_no,sec_1,sales_month
    t.district,
    sales_value as sales_value,
    t.sales_month,
    orderno.name_1,
    null as sales_quarter,
    t.report_year as sales_year,
    t.sec_1,
    orderno.order_no,
    orderno.sec_2,
    t.customer_name,
    orderno.sec_3,
    t.customer_code,
    t.team_owner_id,
    t.team_owner,
    t.sales_person_id,
    t.sales_person,
    t.channel,
    t.report_year,
    'dm_distributor_report' as data_resource,
    now() as etl_time,
    STR_TO_DATE(CONCAT(t.report_year,'0101') , '%Y%m%d') as report_date

FROM t
		LEFT JOIN fine_dw.dw_order_report orderno
		on upper(orderno.sec_1) = upper(t.sec_1)
		and orderno.order_month = t.sales_month
		and orderno.order_report = 'distributor_sales_Prior'
		where orderno.order_no is not null
-- 		GROUP BY orderno.order_no,orderno.sec_1,orderno.sec_2,orderno.sec_3
		ORDER BY orderno.order_no
