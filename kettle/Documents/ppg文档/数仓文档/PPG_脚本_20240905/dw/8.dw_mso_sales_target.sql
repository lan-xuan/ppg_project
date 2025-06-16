/*目标表：fine_dw.dw_mso_sales_target
  来源表：
fine_ods.ods_mso_sales_target_df 
fine_ods.ods_calendar_info_df

更新方式：增量更新 
更新粒度：year
*/

-- 主表
with temp_001 as (
select *  
FROM fine_ods.ods_mso_sales_target_df  
-- where  target_year ='2024'
),
target as(
SELECT 
    proj_name,
		'Paints' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q1') AS target_quarter,
    Q1Paints AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Putty' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q1') AS target_quarter,
    Q1Putty AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Sundries' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q1') AS target_quarter,
    Q1Sundries AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Total' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q1') AS target_quarter,
    Q1Total AS sales_target
FROM temp_001
UNION ALL

SELECT 
    proj_name,
		'Paints' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q2') AS target_quarter,
    Q2Paints AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Putty' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q2') AS target_quarter,
    Q2Putty AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Sundries' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q2') AS target_quarter,
    Q2Sundries AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Total' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q2') AS target_quarter,
    Q2Total AS sales_target
FROM temp_001

UNION ALL
SELECT 
    proj_name,
		'Paints' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q3') AS target_quarter,
    Q3Paints AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Putty' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q3') AS target_quarter,
    Q3Putty AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Sundries' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q3') AS target_quarter,
    Q3Sundries AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Total' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q3') AS target_quarter,
    Q3Total AS sales_target
FROM temp_001

-- UNION ALL
-- SELECT 
--     proj_name,
-- 		'Paints' as report_brand_group,
--     target_year,
--     CONCAT(target_year,'Q2') AS target_quarter,
--     Q2Paints AS sales_target
-- FROM temp_001
-- UNION ALL 
-- SELECT 
--     proj_name,
-- 		'Putty' as report_brand_group,
--     target_year,
--     CONCAT(target_year,'Q2') AS target_quarter,
--     Q2Putty AS sales_target
-- FROM temp_001
-- UNION ALL 
-- SELECT 
--     proj_name,
-- 		'Sundries' as report_brand_group,
--     target_year,
--     CONCAT(target_year,'Q2') AS target_quarter,
--     Q2Sundries AS sales_target
-- FROM temp_001
-- UNION ALL 
-- SELECT 
--     proj_name,
-- 		'Total' as report_brand_group,
--     target_year,
--     CONCAT(target_year,'Q2') AS target_quarter,
--     Q2Total AS sales_target
-- FROM temp_001

UNION ALL
SELECT 
    proj_name,
		'Paints' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q4') AS target_quarter,
    Q4Paints AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Putty' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q4') AS target_quarter,
    Q4Putty AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Sundries' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q4') AS target_quarter,
    Q4Sundries AS sales_target
FROM temp_001
UNION ALL 
SELECT 
    proj_name,
		'Total' as report_brand_group,
    target_year,
    CONCAT(target_year,'Q4') AS target_quarter,
    Q4Total AS sales_target
FROM temp_001
)
select 
    target.proj_name,
    target.report_brand_group,
   target.sales_target/3 as sales_target,
    c.actual_quarter as target_quarter,
    c.adjusted_month as target_month,
    target.target_year  ,
	'fine_ods.ods_mso_sales_target_df/fine_ods.ods_calendar_info_df' as data_resource,
	SYSDATE() as etl_time
from target
LEFT JOIN fine_ods.ods_calendar_info_df c
ON target.target_quarter = c.actual_quarter
where  target.proj_name<>'total'