/*目标表：fine_dw.dw_mm_sales_target
  来源表：
fine_ods.ods_mm_sales_target_df
fine_ods.ods_calendar_info_df

更新方式：增量更新 
更新粒度：year
*/
with target as (
SELECT 
    proj_name,
    target_year,
    CONCAT(target_year,'Q1') AS target_quarter,
    Q1 AS sales_target
FROM fine_ods.ods_mm_sales_target_df
UNION ALL
SELECT 
    proj_name,
    target_year,
    CONCAT(target_year,'Q2') AS target_quarter,
    Q2 AS sales_target
FROM fine_ods.ods_mm_sales_target_df
UNION ALL
SELECT 
    proj_name,
    target_year,
    CONCAT(target_year,'Q3') AS target_quarter,
    Q3 AS sales_target
FROM fine_ods.ods_mm_sales_target_df
UNION ALL
SELECT 
    proj_name,
    target_year,
    CONCAT(target_year,'Q4') AS target_quarter,
    Q4 AS sales_target
FROM fine_ods.ods_mm_sales_target_df
)
select 
    target.proj_name,
		target.sales_target/3 as sales_target,
    c.actual_quarter as target_quarter,
    c.adjusted_month as target_month,
    target.target_year  ,
		 'fine_ods.ods_mm_sales_target_df/fine_ods.ods_calendar_info_df' as data_resource,
		SYSDATE() as etl_time
from target
LEFT JOIN fine_ods.ods_calendar_info_df c
ON target.target_quarter = c.actual_quarter
where target.proj_name<>'TOTAL'