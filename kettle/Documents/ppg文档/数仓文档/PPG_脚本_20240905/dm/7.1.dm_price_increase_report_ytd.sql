-- 20240903重写ytd，在7.1基础上
--20240903 修改2-1公式与7.1一致

/*目标表：dm_price_increase_report
  来源表：
fine_dw.dw_sales_pc_customer
fine_dw.dw_cb_detail
fine_dw.dw_item_brand
fine_dw.dw_order_report
fine_dw.dw_customer_master_list

更新方式：增量更新 
更新粒度：月
*/
/*
-- ！！！！！！！！！！！！！！去年同期月份需要保持和今年月份一致，不可以直接取年！！！！
*/


with temp_dw_order_report as (
select * 
	 from fine_dw.dw_order_report orderno
	 where orderno.order_report = 'price_increase'
				--  and  orderno.report_year = SUBSTRING(${mysql_yesterday_l_month},1,4)

 ),
-- 当年数据
temp_d_dw_price_increase as 
(select a.* ,
        DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') as sales_month_2 -- 关联字段
from fine_dw.dw_price_increase  a 
where 1=1
and  SUBSTRING(sales_month,1,4) =SUBSTRING(${mysql_yesterday_l_month},1,4)
and sales_month <= ${mysql_yesterday_l_month}
and channel = 'DISTRIBUTOR'

),
-- 去年数据
temp_l_dw_price_increase as 
(select *
from fine_dw.dw_price_increase 
where 1=1
and  SUBSTRING(sales_month,1,4) =SUBSTRING(${mysql_yesterday_l_month_l_year},1,4)
and sales_month <=  ${mysql_yesterday_l_month_l_year} 
and channel = 'DISTRIBUTOR'

)

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

      -- 日期字段
    --   ,sales_month
			,sales_year
			-- ,DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b') as sales_month_en
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
            -- , sales_month_2
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

      -- 日期字段 + 1年
    --   ,DATE_FORMAT(DATE_ADD(STR_TO_DATE(concat(sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') AS sales_month
			,sales_year + 1 as sales_year
			-- ,DATE_FORMAT(STR_TO_DATE(CONCAT(sales_month, '01'), '%Y%m%d'), '%b') as sales_month_en
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
            -- , sales_month as sales_month_2
			FROM temp_l_dw_price_increase  -- 去年数据
 ) -- SELECT * FROM temp_dw_price_increase;

,aa as(
SELECT -- 维度字段
			customer_code
			,customer_name
			,item_code
			,item_code_ppg
			,category
			,category_brand
			,category_product_type
			,channel

      -- 日期字段 
      		-- ,sales_month
			,sales_year
			-- ,sales_month_en
            -- ,sales_month_2
			
						-- 销售数据
			, sum(COALESCE(d_volume, 0) )  as d_volume -- 1
			, sum(COALESCE(d_qty, 0) )  as d_qty -- 2
			, sum(COALESCE(d_pc, 0) )  as d_pc -- 3
			, sum(COALESCE(d_sales, 0) )  as d_sales -- 4
			, sum(COALESCE(ly_d_volume, 0) )   as ly_d_volume
		  	, sum(COALESCE(ly_d_qty, 0) )  as ly_d_qty
			, sum(COALESCE(ly_d_pc, 0) )  as ly_d_pc
			, sum(COALESCE(ly_d_sales, 0) )  as ly_d_sales
			
			, sum(COALESCE(s_volume, 0) )  as s_volume -- 5
			, sum(COALESCE(s_qty, 0) )  as s_qty   -- 6
			, sum(COALESCE(s_pc, 0) )  as s_pc  -- 7
			, sum(COALESCE(s_sales, 0) )  as s_sales  -- 8
			, sum(COALESCE(ly_s_volume, 0) )  as ly_s_volume
			, sum(COALESCE(ly_s_qty, 0) )  as ly_s_qty
			, sum(COALESCE(ly_s_pc, 0) )  as ly_s_pc 
			, sum(COALESCE(ly_s_sales, 0) )  as ly_s_sales
			
			, sum(COALESCE(net_volume, 0) )  as net_volume -- 9
			, sum(COALESCE(net_qty, 0) )  as net_qty -- 10
			, sum(COALESCE(net_pc, 0) )  as net_pc -- 11
			, sum(COALESCE(net_sales, 0) )  as net_sales  -- 12
			, sum(COALESCE(ly_net_volume, 0) )   as ly_net_volume
			, sum(COALESCE(ly_net_qty, 0) )   as ly_net_qty
			, sum(COALESCE(ly_net_pc, 0) )   as ly_net_pc
			, sum(COALESCE(ly_net_sales, 0) )   as ly_net_sales
			
 FROM temp_dw_price_increase 
group by 
       -- 维度字段
			customer_code
			,customer_name
			,item_code
			,item_code_ppg
			,category
			,category_brand
			,category_product_type
			,channel

      -- 日期字段 
    --   ,sales_month
			,sales_year
			-- ,sales_month_en
            -- ,sales_month_2
 
 ) -- SELECT * FROM aa;

,bb as(

 -- 经销商数据
SELECT sales_year,sales_year as sales_month,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,d_qty as sales_value,'Qty' as sec_1,'Distributor Sales' as sec_2,'YTD' as sec_3,'是' as is_flag FROM aa WHERE 1=2
UNION ALL
SELECT sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,net_volume,'ACTUAL' as sec_1,'Volume' as sec_2,'YTD' sec_3,'Net Volume' as is_flag FROM aa
UNION ALL
SELECT sales_year,sales_year-1,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,ly_net_volume,'PRIOR' as sec_1,'Volume' as sec_2,'YTD' sec_3,'ly Net Volume' as is_flag FROM aa

-- January								
-- Net Sales  	Sales Volume		      Price Increase (Distributor)		 	Price Increase (备货扣减）		
-- Variance	    Impact	Variance	    Impact	Variance	Var.%	          Impact	Variance	Var.%
UNION ALL
-- Net Sales & Variance  -- 1-1
SELECT sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,(net_sales - ly_net_sales),'Variance' as sec_1,'Net Sales' as sec_2,'YTD' as sec_3,'是' as is_flag FROM aa
UNION ALL
-- Sales Volume & Impact -- 2-1=(1-1)- (3-1) - (4-1)
SELECT sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel, 
-- (net_sales - ly_net_sales) - if(net_sales/NULLIF(net_volume, 0)*ly_net_sales/NULLIF(ly_net_volume, 0)=0 ,0,COALESCE(((d_sales/ NULLIF(d_volume, 0)) - (ly_d_sales/NULLIF(ly_d_volume, 0)))*d_volume,0)) - if(s_sales*s_volume*ly_s_sales*ly_s_volume=0,0, (-1 * ((s_sales/NULLIF(s_volume, 0) - ly_s_sales/NULLIF(ly_s_volume, 0))) * s_volume)) 
(net_sales - ly_net_sales) 
- if( COALESCE(d_volume,0)*COALESCE(ly_d_volume,0)=0 ,0,COALESCE(((d_sales/ NULLIF(d_volume, 0)) - (ly_d_sales/NULLIF(ly_d_volume, 0)))*d_volume,0)) 
- if(s_sales*s_volume*ly_s_sales*ly_s_volume=0,0, (-1 * ((s_sales/NULLIF(s_volume, 0) - ly_s_sales/NULLIF(ly_s_volume, 0))) * s_volume))
,'Impact' as sec_1,'Sales Volume' as sec_2,'YTD' as sec_3,'是' as is_flag FROM aa
UNION ALL
-- Sales Volume & Variance -- 2-2
SELECT sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,net_volume - ly_net_volume,'Variance' as sec_1,'Sales Volume' as sec_2,'YTD' as sec_3,'是' as is_flag FROM aa
UNION ALL
-- Price Increase (Distributor) & Impact -- 3-1
 SELECT sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,if( COALESCE(d_volume,0)*COALESCE(ly_d_volume,0)=0 ,0,COALESCE(((d_sales/ NULLIF(d_volume, 0)) - (ly_d_sales/NULLIF(ly_d_volume, 0)))*d_volume,0)),'Impact' as sec_1,'Price Increase (Distributor)' as sec_2,'YTD' as sec_3,'是' as is_flag FROM aa

UNION ALL
-- Price Increase (Distributor) & Variance -- 3-2
SELECT sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,if(net_sales/NULLIF(net_volume, 0)*ly_net_sales/NULLIF(ly_net_volume, 0)=0,0, COALESCE((d_sales/ NULLIF(d_volume, 0) - ly_d_sales/NULLIF(ly_d_volume, 0)),0)) ,'Variance' as sec_1,'Price Increase (Distributor)' as sec_2,'YTD' as sec_3,null as is_flag FROM aa

UNION ALL
-- Price Increase (Distributor) & Var.% -- 3-3
SELECT sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,case when CONCAT(d_sales,d_volume,ly_d_sales,ly_d_volume) is null or d_sales=0 or d_volume=0 or ly_d_sales=0 or ly_d_volume=0 then 0 else (d_sales/NULLIF(d_volume, 0) - ly_d_sales/NULLIF(ly_d_volume, 0))/(ly_d_sales/NULLIF(ly_d_volume, 0)) end,'Var.%' as sec_1,'Price Increase (Distributor)' as sec_2,'YTD' as sec_3,null as is_flag FROM aa

UNION ALL
-- Price Increase (备货扣减） Impact  -- 4-1
SELECT sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,if(s_sales*s_volume*ly_s_sales*ly_s_volume=0,0, (-1 * ((s_sales/NULLIF(s_volume, 0) - ly_s_sales/NULLIF(ly_s_volume, 0))) * s_volume)),'Impact' as sec_1,'Price Increase (备货扣减）' as sec_2,'YTD' as sec_3,'是' as is_flag FROM aa

UNION ALL
-- Price Increase (备货扣减） Variance  -- 4-2
SELECT sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,case when CONCAT(s_sales,s_volume,ly_s_sales,ly_s_volume) is null or s_sales=0 or s_volume=0 or ly_s_sales=0 or ly_s_volume=0 then 0 else (s_sales/NULLIF(s_volume, 0) - ly_s_sales/NULLIF(ly_s_volume, 0)) end,'Variance' as sec_1,'Price Increase (备货扣减）' as sec_2,'YTD' as sec_3,null as is_flag FROM aa

UNION ALL
-- Price Increase (备货扣减）Var.%  -- 4-3
SELECT sales_year,sales_year,customer_code,customer_name,item_code,item_code_ppg,category,category_brand,category_product_type,channel,case when CONCAT(s_sales,s_volume,ly_s_sales,ly_s_volume) is null or s_sales=0 or s_volume=0 or ly_s_sales=0 or ly_s_volume=0 then 0 else ((s_sales/NULLIF(s_volume, 0) - ly_s_sales/NULLIF(ly_s_volume, 0))/(ly_s_sales/NULLIF(ly_s_volume, 0))) end,'Var.%' as sec_1,'Price Increase (备货扣减）' as sec_2,'YTD' as sec_3,null as is_flag FROM aa

)


		SELECT    -- DISTINCT order_no 
-- 		order_no,sum(sales_value),orderno.sec_1,orderno.sec_2,orderno.sec_3
					t.channel,
					t.customer_code,
					t.customer_name,
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
			bb t
		
				LEFT JOIN temp_dw_order_report  orderno
				on UPPER(orderno.name_1) = UPPER(t.sec_1)
				and UPPER(orderno.sec_2) = UPPER(t.sec_2)
				-- and orderno.sec_3 = t.sec_3
				and orderno.order_month = t.sales_month				
				where 1=1
				and orderno.order_no is not null
-- 				GROUP BY orderno.sec_1,orderno.sec_2,orderno.sec_3
				ORDER BY orderno.order_no;