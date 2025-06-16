-- 20240902:修改ytd公式

/*目标表：dm_price_increase_report_lead
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
delete from  fine_dm.dm_price_increase_report_lead
where  report_year = '2024';
         
insert into fine_dm.dm_price_increase_report_lead  (
			    channel,
					customer_code,
					customer_name,
					proj_name,
					proj_name_en,
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
	 where orderno.order_report = 'price_increase_proj'
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
		  and sales_month <= ${mysql_yesterday_l_month}  )


),
-- 去年数据
temp_l_dw_price_increase as 
(select *
from fine_dw.dw_price_increase 
where 1=1 and 
 (
		     SUBSTRING(sales_month,1,4) = SUBSTRING(${mysql_yesterday_l_month_l_year},1,4)
		      and sales_month <= ${mysql_yesterday_l_month_l_year}  )

)


,temp_dw_price_increase as (
 SELECT
      -- 维度字段
			customer_code
			,customer_name
			,proj_name
			,proj_name_en
			,case when proj_name = '比亚迪' then item_code_ppg else item_code end as item_code
			,item_code_ppg
			,category
			,category_brand
			,category_product_type
			,channel

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
			,proj_name
			,proj_name_en
			,case when proj_name = '比亚迪' then item_code_ppg else item_code end as item_code
			,item_code_ppg
			,category
			,category_brand
			,category_product_type
			,channel

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
 ) -- SELECT * FROM temp_dw_price_increase;

,aa as(
SELECT -- 维度字段
			customer_code
			,customer_name
			,proj_name
			,proj_name_en
			,item_code
			,item_code_ppg
			,category
			,category_brand
			,category_product_type
			,channel

      -- 日期字段 
      		,sales_month
			,sales_year
			,sales_month_en
            ,sales_month_2
			
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
			,proj_name
			,proj_name_en
			,item_code
			,item_code_ppg
			,category
			,category_brand
			,category_product_type
			,channel

      -- 日期字段 
      ,sales_month
			,sales_year
			,sales_month_en
            ,sales_month_2
 
 ) -- SELECT * FROM aa;
 
,bb as(
SELECT sales_year,sales_month,channel,d_qty as sales_value,'Qty' as sec_1,'Distributor Sales' as sec_2,CONCAT('FY',substr(sales_year,3,2),' ',sales_month_en) as sec_3,'是' as is_flag FROM aa WHERE 1=2


UNION ALL  -- 2-1 Net Sales
SELECT sales_year,sales_month,channel,sum(net_sales),'GROSS SALES' as sec_1,'ACTUAL' as sec_2,upper(sales_month_en) as sec_3,'GROSSSALES' as is_flag FROM aa
GROUP BY sales_year,sales_month,channel,sales_month_en
UNION ALL  -- 3-1 prior Net Sales
SELECT sales_year-1,sales_month_2,channel,sum(ly_net_sales),'GROSS SALES' as sec_1,'PRIOR' as sec_2,upper(sales_month_en) as sec_3,'GROSSSALES' as is_flag FROM aa
GROUP BY sales_year-1,sales_month_2,channel,sales_month_en

UNION ALL
-- Sales Volume & Impact -- 1-1 明细表的2-1=(1-1)- (3-1) - (4-1)
SELECT sales_year,sales_month,channel, 
sum((net_sales - ly_net_sales) - 
if( COALESCE(d_volume,0)*COALESCE(ly_d_volume,0)=0 ,0,COALESCE(((d_sales/ NULLIF(d_volume, 0)) - (ly_d_sales/NULLIF(ly_d_volume, 0)))*d_volume,0))
- if(s_sales*s_volume*ly_s_sales*ly_s_volume=0,0,(-1 * ((s_sales/NULLIF(s_volume, 0) - ly_s_sales/NULLIF(ly_s_volume, 0))) * s_volume)))
,'VOLUME' as sec_1,'IMPACT' as sec_2,sales_month_en as sec_3,'是' as is_flag 
FROM aa
WHERE channel = 'DISTRIBUTOR'
GROUP BY sales_year,sales_month,channel,sales_month_en

UNION ALL
-- Price Increase (Distributor) & Impact -- 1-2 明细表的(3-1) + (4-1)
-- 3-1
-- if( COALESCE(d_volume,0)*COALESCE(ly_d_volume,0)=0 ,0,COALESCE(((d_sales/ NULLIF(d_volume, 0)) - (ly_d_sales/NULLIF(ly_d_volume, 0)))*d_volume,0))
SELECT sales_year,sales_month,channel,
sum(
if( COALESCE(d_volume,0)*COALESCE(ly_d_volume,0)=0 ,0,COALESCE(((d_sales/ NULLIF(d_volume, 0)) - (ly_d_sales/NULLIF(ly_d_volume, 0)))*d_volume,0))
+ if(s_sales*s_volume*ly_s_sales*ly_s_volume=0,0,(-1 * ((s_sales/NULLIF(s_volume, 0) - ly_s_sales/NULLIF(ly_s_volume, 0))) * s_volume)) )
,'PRICE' as sec_1,'IMPACT' as sec_2,sales_month_en as sec_3,'是' as is_flag
FROM aa
WHERE channel = 'DISTRIBUTOR'
GROUP BY sales_year,sales_month,channel,sales_month_en

-- ---------------------------- 其他渠道 ------------------------------------

UNION ALL
-- Sales Volume & Impact -- 1-1 明细表的2-1=(1-1)- (3-1) - (4-1)
SELECT sales_year,sales_month,channel, 
sum((net_sales - ly_net_sales) - COALESCE(if(net_volume<0,0,net_volume*(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0))),0))
,'VOLUME' as sec_1,'IMPACT' as sec_2,sales_month_en as sec_3,'是' as is_flag 
FROM aa
WHERE channel <> 'DISTRIBUTOR'
GROUP BY sales_year,sales_month,channel,sales_month_en

UNION ALL
-- Price Increase (Distributor) & Impact -- 1-2 明细表的(3-1)
SELECT sales_year,sales_month,channel,
sum(if(net_volume<0,0,net_volume*(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0))))
,'PRICE' as sec_1,'IMPACT' as sec_2,sales_month_en as sec_3,'是' as is_flag
FROM aa
WHERE channel <> 'DISTRIBUTOR'
GROUP BY sales_year,sales_month,channel,sales_month_en
-- (net_sales - ly_net_sales) - COALESCE(if(net_volume<0,0,net_volume*(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0))),0)
-- if(net_volume<0,0,net_volume*(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0)))

)
, bbb as( -- YTD
-- 1-1 & 1-2 & 2-1 & 3-1
SELECT sales_year,sales_year as sales_month,channel,sum(sales_value) as sales_value,sec_1,sec_2,'YTD',is_flag FROM bb where 1=1
GROUP BY sales_year,sales_year,channel,sec_1,sec_2,is_flag

UNION ALL
-- 4-1 = （2-1）/（3-1）-1
SELECT sales_year,sales_year as sales_month,channel,
-- sum(net_sales/NULLIF(ly_net_sales,0))-1 as sales_value
sum(net_sales)/sum(ly_net_sales)-1 as sales_value
,'Sales Growth%' as sec_1,'Sales Growth%' as sec_2,'YTD' as sec_3,null is_flag 
FROM aa WHERE 1=1 and channel = 'DISTRIBUTOR'
GROUP BY sales_year,sales_year,channel
UNION ALL
-- 5-1 = （1-2）/（3-1）
SELECT sales_year,sales_year as sales_month,channel,
-- 1=2
-- sum(
-- if( COALESCE(d_volume,0)*COALESCE(ly_d_volume,0)=0 ,0,COALESCE(((d_sales/ NULLIF(d_volume, 0)) - (ly_d_sales/NULLIF(ly_d_volume, 0)))*d_volume,0))
-- + if(s_sales*s_volume*ly_s_sales*ly_s_volume=0,0,(-1 * ((s_sales/NULLIF(s_volume, 0) - ly_s_sales/NULLIF(ly_s_volume, 0))) * s_volume)) )
-- sum(ly_net_sales) 3-1
-- sum((if(d_sales*d_volume*ly_d_sales*ly_d_volume=0,0,((d_sales/ NULLIF(d_volume, 0)) - (ly_d_sales/NULLIF(ly_d_volume, 0))*d_volume)) + if(s_sales*s_volume*ly_s_sales*ly_s_volume=0,0,(-1 * ((s_sales/NULLIF(s_volume, 0) - ly_s_sales/NULLIF(ly_s_volume, 0))) * s_volume)))/NULLIF(ly_net_sales,0) )
sum(
if( COALESCE(d_volume,0)*COALESCE(ly_d_volume,0)=0 ,0,COALESCE(((d_sales/ NULLIF(d_volume, 0)) - (ly_d_sales/NULLIF(ly_d_volume, 0)))*d_volume,0))
+ if(s_sales*s_volume*ly_s_sales*ly_s_volume=0,0,(-1 * ((s_sales/NULLIF(s_volume, 0) - ly_s_sales/NULLIF(ly_s_volume, 0))) * s_volume)) )/nullif(sum(ly_net_sales),0)
,'Price Increase%' as sec_1,'Price Increase‱' as sec_2,'YTD' as sec_3,null is_flag 
FROM aa WHERE 1=1 and channel = 'DISTRIBUTOR'
GROUP BY sales_year,sales_year,channel


-- ---------------------------- 其他渠道 ------------------------------------
UNION ALL
-- 4-1 = （2-1）/（3-1）-1
SELECT sales_year,sales_year as sales_month,channel,
sum(net_sales)/NULLIF(sum(ly_net_sales),0)-1 as sales_value
-- sum(ly_net_sales) as sales_value
,'Sales Growth%' as sec_1,'Sales Growth%' as sec_2,'YTD' as sec_3,null is_flag 
FROM aa WHERE 1=1 and channel <> 'DISTRIBUTOR'
GROUP BY sales_year,sales_year,channel

UNION ALL
-- 5-1 = （1-2）/（3-1）
SELECT sales_year,sales_year as sales_month,channel,
-- sum(if(net_volume<0,0,net_volume*(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0)))) 1-2
-- sum(ly_net_sales) 3-1
-- sum(if(net_volume<0,0,net_volume*(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0)))/NULLIF(ly_net_sales,0))
-- sum(if(net_volume<0,0,net_volume*(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0))))
sum(if(net_volume<0,0,net_volume*(net_sales/NULLIF(net_volume, 0)-ly_net_sales/NULLIF(ly_net_volume, 0))))/sum(ly_net_sales)
,'Price Increase%' as sec_1,'Price Increase‱' as sec_2,'YTD' as sec_3,null is_flag 
FROM aa WHERE 1=1 and channel <> 'DISTRIBUTOR'
GROUP BY sales_year,sales_year,channel
)

		SELECT    -- DISTINCT order_no 
-- 		order_no,sum(sales_value),channel
					t.channel,
					null as customer_code,
					null as customer_name,
					null as proj_name,
					null as proj_name_en,
					case when orderno.name_1 = 'Sales Growth%' then t.sales_value*100
							 when orderno.name_1 = 'Price Increase%'	then t.sales_value*10000 else t.sales_value end as sales_value,
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
				and orderno.order_no  is not null
-- 				and channel = 'SCHOOL'
-- 				GROUP BY orderno.order_no,channel
				ORDER BY orderno.order_no