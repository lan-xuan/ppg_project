/*目标表：dm_price_increase_mm_report
  来源表：
fine_dw.dw_sales_pc_customer
fine_dw.dw_cb_detail
fine_dw.dw_item_brand
fine_dw.dw_order_report
fine_dw.dw_customer_master_list

更新方式：增量更新 
更新粒度：年
*/
/*
-- ！！！！！！！！！！！！！！去年同期月份需要保持和今年月份一致，不可以直接取年！！！！
TRUNCATE TABLE fine_dm.dm_price_increase_mm_report;
insert into fine_dm.dm_price_increase_mm_report  (
					channel,
					customer_code,
					customer_name,
					proj_name,
					proj_name_en,
					item_code,
					item_code_ppg,
					category,
					category_brand,
					category_product_type,
					sales_value,
					sales_month,
					sales_year,
					report_year,
				  order_no,
					sec_1,
					sec_2,
					sec_3,
					is_flag,
				  data_resource, 
          etl_time,
         report_date
       
)
*/

with temp_dw_order_report as (
select * 
	 from fine_dw.dw_order_report orderno
	 where orderno.order_report = 'price_increase_2'
	 and  orderno.report_year = SUBSTRING(${mysql_yesterday_l_month},1,4)
 ),
-- 当年数据
temp_d_dw_price_increase as 
(select a.* ,
        DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') as sales_month_2 -- 关联字段
from fine_dw.dw_price_increase  a 
where 1=1 and 
		(
		 SUBSTRING(sales_month,1,4) = SUBSTRING( ${mysql_yesterday_l_month},1,4)
		 and sales_month <= ${mysql_yesterday_l_month}  
		 )
	and channel <> 'DISTRIBUTOR' 
-- 	 and  customer_code = '125640'
-- and item_code = 'P190-6060.JLR/5L-C3'
-- and proj_name_en = 'JLR'
),
-- 去年数据
temp_l_dw_price_increase as 
(select * 
from fine_dw.dw_price_increase 
where 1=1 and 
		 		(
		       SUBSTRING(sales_month,1,4) = SUBSTRING(${mysql_yesterday_l_month_l_year},1,4)
		        and sales_month <= ${mysql_yesterday_l_month_l_year}  
		        )
	
	and channel <> 'DISTRIBUTOR'
-- 	 and  customer_code = '125640'
-- and item_code = 'P190-6060.JLR/5L-C3'
-- and proj_name_en = 'JLR'
) -- SELECT sum(d_sales) FROM temp_d_dw_price_increase WHERE channel = 'MM';


 ,temp_dw_price_increase as (
 SELECT
      -- 维度字段
			customer_code
			,customer_name
			,case when proj_name = '比亚迪' then item_code_ppg else item_code end as item_code
			,item_code_ppg
			,category
			,category_brand
			,category_product_type
			,channel
			,proj_name
			,proj_name_en

      -- 日期字段
      		,sales_month
			,sales_year
			,DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b') as sales_month_en
			-- 销售数据
			, d_volume -- 1
			, d_qty -- 2
			, d_pc -- 3
			, d_sales -- 4
			, null  as ly_d_volume
		  	, null as ly_d_qty
			, null as ly_d_pc
			, null as ly_d_sales
			, s_volume -- 5
			, s_qty   -- 6
			, s_pc  -- 7
			, s_sales  -- 8
			, null as ly_s_volume
			, null as ly_s_qty
			, null as ly_s_pc 
			, null as ly_s_sales
			, net_volume -- 9
			, net_qty -- 10
			, net_pc -- 11
			, net_sales  -- 12
			, null  as ly_net_volume
			, null  as ly_net_qty
			, null  as ly_net_pc
			, null  as ly_net_sales
            , sales_month_2
			FROM temp_d_dw_price_increase  -- 当年数据
			WHERE 1=1
union all 
      SELECT
      -- 维度字段
			customer_code
			,customer_name
			,case when proj_name = '比亚迪' then item_code_ppg else item_code end as item_code 
			,item_code_ppg
			,category
			,category_brand
			,category_product_type
			,channel
			,proj_name
			,proj_name_en

      -- 日期字段 + 1年
      		,DATE_FORMAT(DATE_ADD(STR_TO_DATE(concat(sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') AS sales_month
			,sales_year + 1 as sales_year
			,DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b') as sales_month_en
			-- 销售数据
			, null as d_volume -- 1
			, null as d_qty -- 2
			, null as d_pc -- 3
			, null as d_sales -- 4
			, d_volume  as ly_d_volume
		  	, d_qty as ly_d_qty
			, d_pc as ly_d_pc
			, d_sales as ly_d_sales
			, null as s_volume -- 5
			, null as s_qty   -- 6
			, null as s_pc  -- 7
			, null as s_sales  -- 8
			, s_volume as ly_s_volume
			, s_qty as ly_s_qty
			, s_pc as ly_s_pc 
			, s_sales as ly_s_sales
			, null as net_volume -- 9
			, null as net_qty -- 10
			, null as net_pc -- 11
			, null as net_sales  -- 12
			, net_volume  as ly_net_volume
			, net_qty  as ly_net_qty
			, net_pc  as ly_net_pc
			, net_sales  as ly_net_sales
            , sales_month as sales_month_2
			FROM temp_l_dw_price_increase  -- 去年数据

 )
-- 423156 -- d数据242556 308380
 ,aa as(
		SELECT
			customer_code,
			customer_name,
			item_code,
			item_code_ppg,
			category,
			category_brand,
			category_product_type,
			channel,
			proj_name,
			proj_name_en,

			-- 销售数据
			sum(COALESCE(d_volume, 0)) as d_volume, -- 1
			sum(COALESCE(d_qty, 0)) as d_qty, -- 2
			sum(COALESCE(d_pc, 0)) as d_pc, -- 3
			sum(COALESCE(d_sales, 0)) as d_sales, -- 4
			sum(COALESCE(ly_d_volume, 0)) as ly_d_volume, 
			sum(COALESCE(ly_d_qty, 0)) as ly_d_qty, 
			sum(COALESCE(ly_d_pc, 0)) as ly_d_pc, 
			sum(COALESCE(ly_d_sales, 0)) as ly_d_sales, 
			sum(COALESCE(s_volume, 0)) as s_volume, -- 5
			sum(COALESCE(s_qty, 0)) as s_qty,  -- 6
			sum(COALESCE(s_pc, 0)) as s_pc,  -- 7
			sum(COALESCE(s_sales, 0)) as s_sales,  -- 8
			sum(COALESCE(s_volume, 0)) as ly_s_volume, 
			sum(COALESCE(ly_s_qty, 0)) as ly_s_qty,  
			sum(COALESCE(ly_s_pc, 0)) as ly_s_pc,  
			sum(COALESCE(ly_s_sales, 0)) as ly_s_sales,
			sum(COALESCE(net_volume, 0)) as net_volume, -- 9
			sum(COALESCE(net_qty, 0)) as net_qty, -- 10
			sum(COALESCE(net_pc, 0)) as net_pc, -- 11
			sum(COALESCE(net_sales, 0)) as net_sales,  -- 12
			sum(COALESCE(ly_net_volume, 0)) as ly_net_volume,
			sum(COALESCE(ly_net_qty, 0)) as ly_net_qty,
			sum(COALESCE(ly_net_pc, 0)) as ly_net_pc,
			sum(COALESCE(ly_net_sales, 0)) as ly_net_sales,
			
			sales_month,
			sales_month_2,
			sales_year,
			DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b') as sales_month_en
		FROM  temp_dw_price_increase
		GROUP BY
			customer_code,
			customer_name,
			item_code,
			item_code_ppg,
			category,
			category_brand,
			category_product_type,
			channel,
			proj_name,
			proj_name_en,
			sales_month,
			sales_month_2,
			sales_year,
			DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b')

)  -- SELECT sum(d_sales),sum(ly_d_sales) from aa WHERE channel = 'MM';

,bb as(

 -- 经销商数据
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,d_qty as sales_value,'Qty' as sec_1,'Distributor Sales' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,'是' as is_flag FROM aa where 1=2

UNION ALL
-- 经销商备货数据
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,s_qty,'Qty' as sec_1,'备货扣减' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,null as is_flag FROM aa
UNION ALL
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,s_volume,'Volume' as sec_1,'备货扣减' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,null as is_flag FROM aa
UNION ALL
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,s_sales,'Sales' as sec_1,'备货扣减' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,null as is_flag FROM aa
UNION ALL
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,s_pc,'PC' as sec_1,'备货扣减' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,null as is_flag FROM aa

UNION ALL
	-- 经销商数据 - 经销商备货数据
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,net_qty,'Net Qty' as sec_1,CONCAT('ACTUAL ',sales_month_en) as sec_2,CONCAT('ACTUAL ',sales_month_en) as sec_3,'是' as is_flag FROM aa
UNION ALL
SELECT sales_year-1,sales_month_2,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,ly_net_qty,'Net Qty' as sec_1,CONCAT('PRIOR ',sales_month_en)as sec_2,CONCAT('PRIOR ',sales_month_en) as sec_3,'是' as is_flag FROM aa

UNION ALL
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,net_volume,'Net Volume' as sec_1,CONCAT('ACTUAL ',sales_month_en) as sec_2,CONCAT('ACTUAL ',sales_month_en) as sec_3,null as is_flag FROM aa
UNION ALL
SELECT sales_year-1,sales_month_2,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,ly_net_volume,'Net Volume' as sec_1,CONCAT('PRIOR ',sales_month_en) as sec_2,CONCAT('PRIOR ',sales_month_en) as sec_3,null as is_flag FROM aa

UNION ALL
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,net_sales,'Net Sales' as sec_1,CONCAT('ACTUAL ',sales_month_en) as sec_2,CONCAT('ACTUAL ',sales_month_en) as sec_3,null as is_flag FROM aa
UNION ALL
SELECT sales_year-1,sales_month_2,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,ly_net_sales,'Net Sales' as sec_1,CONCAT('PRIOR ',sales_month_en) as sec_2,CONCAT('PRIOR ',sales_month_en) as sec_3,null as is_flag FROM aa

UNION ALL
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,net_pc,'Net PC' as sec_1,CONCAT('ACTUAL ',sales_month_en) as sec_2,CONCAT('ACTUAL ',sales_month_en) as sec_3,null as is_flag FROM aa 
UNION ALL
SELECT sales_year-1,sales_month_2,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,ly_net_pc,'Net PC' as sec_1,CONCAT('PRIOR ',sales_month_en) as sec_2,CONCAT('PRIOR ',sales_month_en) as sec_3,null as is_flag FROM aa 
-- January								
-- Net Sales  	Sales Volume		      Price Increase (Distributor)		 	Price Increase (备货扣减）		
-- Variance	    Impact	Variance	    Impact	Variance	Var.%	          Impact	Variance	Var.%
UNION ALL
-- Net Sales & Variance  -- 1-1
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,(net_sales - ly_net_sales),'Variance' as sec_1,'Net Sales' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,'是' as is_flag FROM aa
UNION ALL
-- Sales Volume & Impact -- 2-1=(1-1)- (3-1) 
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en, (net_sales - ly_net_sales) - COALESCE(if(net_volume<0,0,net_volume*(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0))),0),'Impact' as sec_1,'Sales Volume' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,'是' as is_flag FROM aa

UNION ALL
-- Sales Volume & Variance -- 2-2
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,net_volume - ly_net_volume,'Variance' as sec_1,'Sales Volume' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,'是' as is_flag FROM aa
UNION ALL
-- Price Increase  Impact -- 3-1 = d_volume*((4-1) - (4-2)) -- 其中如果d_volume<0 则为0
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,if(net_volume<0,0,net_volume*(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0))),'Impact' as sec_1,'Price Increase' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,'是' as is_flag FROM aa

UNION ALL
-- Price Increas Variance -- 3-2 =(4-1) - (4-2) -- 如果(4-1) 、 (4-2) 任一个为0就是0
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,if(net_sales/NULLIF(net_volume, 0)*ly_net_sales/NULLIF(ly_net_volume, 0)=0,0,(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0))),'Variance' as sec_1,'Price Increase' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,null as is_flag FROM aa

UNION ALL
-- Price Increase (Distributor) & Var.% -- 3-3 （(4-1) - (4-2)）/(4-2)
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,case when CONCAT(d_sales,d_volume,ly_d_sales,ly_d_volume) is null or d_sales=0 or d_volume=0 or ly_d_sales=0 or ly_d_volume=0 then 0 else (net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0))/NULLIF((ly_net_sales/NULLIF(ly_net_volume, 0)),0) end,'Var.%' as sec_1,'Price Increase' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,null as is_flag FROM aa

UNION ALL
-- -- 4-1 Unit Price Actual
SELECT sales_year,sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,net_sales/NULLIF(net_volume, 0),'Actual' as sec_1,'Unit Price' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,'是' as is_flag FROM aa

UNION ALL -- 4-2 Unit Price Prior
SELECT sales_year-1,sales_month_2,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,ly_net_sales/NULLIF(ly_net_volume, 0),'Prior' as sec_1,'Unit Price' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,'是' as is_flag FROM aa

)-- SELECT DISTINCT sec_1,sec_2,sales_month from bb;
, bbb as(

SELECT sales_year,sales_year as sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,sum(sales_value) as sales_value,sec_1,sec_2,sec_3,is_flag FROM bb WHERE is_flag = '是' GROUP BY sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,proj_name,proj_name_en,sec_1,sec_2,sec_3,is_flag
)
-- SELECT DISTINCT sec_1,sec_2,sales_month from bbb;
		SELECT  --   DISTINCT order_no
					t.channel,
					t.customer_code,
					t.customer_name,
					t.proj_name,
					t.proj_name_en,
					t.item_code,
					t.item_code_ppg,
					t.category,
					t.category_brand,
					t.category_product_type,
					case when orderno.sec_1 = 'Var.%' then t.sales_value*100 else t.sales_value end as sales_value,
					t.sales_month,
					t.sales_year,
					orderno.report_year,
				  	orderno.order_no,
					orderno.sec_1,
					orderno.sec_2,
					orderno.sec_3,
					orderno.is_flag,
				  '' as data_resource, 
          SYSDATE() as etl_time,
           STR_TO_DATE(CONCAT( orderno.report_year,'0101') , '%Y%m%d') as report_date
					
		FROM 
			bbb t
		
				LEFT JOIN temp_dw_order_report  orderno
				on UPPER(orderno.name_1) = UPPER(t.sec_1)
				and UPPER(orderno.sec_2) = UPPER(t.sec_2)
				-- and orderno.sec_3 = t.sec_3
				and orderno.order_month = t.sales_month				
				where 1=1
				and orderno.order_no is not null
				ORDER BY orderno.order_no
