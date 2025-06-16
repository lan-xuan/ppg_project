/*
-- 368340
TRUNCATE TABLE fine_dm.dm_model_vpm;
INSERT INTO fine_dm.dm_model_vpm
(
	order_type,
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
	report_year,
	order_no,
	sec_1,
	sec_2,
	sec_3,
	report_date,
	etl_time
)
*/

with temp_dw_order_report as (
select * 
	 from fine_dw.dw_order_report orderno
	 where orderno.order_report = 'model_vpm'
				 and  orderno.report_year = SUBSTRING(${mysql_yesterday_l_month},1,4)
 ),temp_d_dw_price_increase as(
		SELECT -- count(1) -- 334050
			'进货' order_type,
			t.customer_code,
			t.customer_name,
			case when proj_name = '比亚迪' then item_code_ppg else item_code end as item_code,
			t.item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			t.proj_name,
			t.proj_name_en,
			t.sales_month,
			t.sales_year,
			sum(t.d_volume) as d_volume,
			sum(t.d_sales) as d_sales,
			sum(t.d_qty) as d_qty,
			sum(t.d_pc) as d_pc,
			DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') as sales_month_2
		FROM 
			fine_dw.dw_price_increase t
		where 1=1
	  and sales_month = ${mysql_yesterday_l_month}
		and t.channel = 'DISTRIBUTOR'
-- 		and t.channel <> 'SCHOOL'
		-- and proj_name = '一汽丰田'
		-- and item_code = 'P991-8952.TOY/1L-C3'
        Group BY  
			t.customer_code,
			t.customer_name,
			case when proj_name = '比亚迪' then item_code_ppg else item_code end ,
			t.item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			t.proj_name,
			t.proj_name_en,
			t.sales_month,
			t.sales_year,
			DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m')
UNION ALL
		SELECT -- count(1) -- 334050
			'备货' order_type,
			t.customer_code,
			t.customer_name,
			case when proj_name = '比亚迪' then item_code_ppg else item_code end as item_code,
			t.item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			t.proj_name,
			t.proj_name_en,
			t.sales_month,
			t.sales_year,
			sum(t.s_volume) as s_volume,
			sum(t.s_sales) as s_sales,
			sum(t.s_qty) as s_qty,
			sum(t.s_pc) as s_pc,
			DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') as sales_month_2
		FROM 
			fine_dw.dw_price_increase t
		where 1=1
	  and sales_month = ${mysql_yesterday_l_month}
		and t.channel = 'DISTRIBUTOR'
-- 		and t.channel <> 'SCHOOL'
		-- and proj_name = '一汽丰田'
		-- and item_code = 'P991-8952.TOY/1L-C3'
        Group BY  
			t.customer_code,
			t.customer_name,
			case when proj_name = '比亚迪' then item_code_ppg else item_code end,
			t.item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			t.proj_name,
			t.proj_name_en,
			t.sales_month,
			t.sales_year,
			DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m')

)  -- SELECT * from temp_d_dw_price_increase ;
,temp_l_dw_price_increase as(
		SELECT -- count(1) -- 334050
			'进货' order_type,
			t.customer_code,
			t.customer_name,
			case when proj_name = '比亚迪' then item_code_ppg else item_code end as item_code,
			t.item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			t.proj_name,
			t.proj_name_en,
			t.sales_month,
			t.sales_year,
			sum(t.d_volume) as d_volume,
			sum(t.d_sales) as d_sales,
			sum(t.d_qty) as d_qty,
			sum(t.d_pc) as d_pc
		FROM 
			fine_dw.dw_price_increase t
		where 1=1
		and sales_month = ${mysql_yesterday_l_month_l_year}
		and t.channel = 'DISTRIBUTOR' 
-- 		and t.channel <> 'SCHOOL'
		-- and proj_name = '一汽丰田'
		-- and item_code = 'P991-8952.TOY/1L-C3'
        Group BY
			t.customer_code,
			t.customer_name,
			case when proj_name = '比亚迪' then item_code_ppg else item_code end ,
			t.item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			t.proj_name,
			t.proj_name_en,
			t.sales_month,
			t.sales_year
UNION ALL
		SELECT -- count(1) -- 334050
			'备货' order_type,
			t.customer_code,
			t.customer_name,
			case when proj_name = '比亚迪' then item_code_ppg else item_code end as item_code,
			t.item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			t.proj_name,
			t.proj_name_en,
			t.sales_month,
			t.sales_year,
			sum(t.s_volume) as s_volume,
			sum(t.s_sales) as s_sales,
			sum(t.s_qty) as s_qty,
			sum(t.s_pc) as s_pc
		FROM 
			fine_dw.dw_price_increase t
		where 1=1
		and sales_month = ${mysql_yesterday_l_month_l_year}
		and t.channel = 'DISTRIBUTOR' 
-- 		and t.channel <> 'SCHOOL'
		-- and proj_name = '一汽丰田'
		-- and item_code = 'P991-8952.TOY/1L-C3'
        Group BY
			t.customer_code,
			t.customer_name,
			case when proj_name = '比亚迪' then item_code_ppg else item_code end ,
			t.item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			t.proj_name,
			t.proj_name_en,
			t.sales_month,
			t.sales_year
) -- SELECT * from temp_l_dw_price_increase ;
,temp_dw_price_increase as(
SELECT
			 order_type
			,channel
			,customer_code
			,customer_name
			,proj_name
			,proj_name_en
			,item_code 
			,item_code_ppg 
			,category
			,category_brand
			,category_product_type

    -- 日期字段
      		,sales_month
			,sales_year
			,sales_month_2
	-- 销售数据
			,sum(d_volume) as d_volume
			,sum(d_qty) as d_qty
			,sum(d_pc) as d_pc
			,sum(d_sales) as d_sales
			,sum(ly_d_volume) as ly_d_volume
			,sum(ly_d_qty) as ly_d_qty
			,sum(ly_d_pc) as ly_d_pc
			,sum(ly_d_sales) as ly_d_sales

FROM(
     SELECT     
     -- 维度字段
			 order_type
			,channel
			,customer_code
			,customer_name
			,proj_name
			,proj_name_en
			,item_code 
			,item_code_ppg 
			,category
			,category_brand
			,category_product_type

      -- 日期字段
      		,sales_month
			,sales_year
			,DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(sales_month,'01'), '%Y%m%d'), INTERVAL 1 year), '%Y%m') as sales_month_2

			-- 销售数据
			, d_volume -- 1
			, d_qty -- 2
			, d_pc -- 3
			, d_sales -- 4
			, null  as ly_d_volume
		  	, null as ly_d_qty
			, null as ly_d_pc
			, null as ly_d_sales
	
			FROM temp_d_dw_price_increase  -- 当年数据
union all 
   SELECT     
     -- 维度字段
			 order_type
			,channel
			,customer_code
			,customer_name
			,proj_name
			,proj_name_en
			,item_code 
			,item_code_ppg 
			,category
			,category_brand
			,category_product_type

      -- 日期字段
			,DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(sales_month,'01'), '%Y%m%d'), INTERVAL -1 year), '%Y%m') as sales_month
			,sales_year+1
			,sales_month as sales_month_2

			-- 销售数据
			, null d_volume -- 1
			, null d_qty -- 2
			, null d_pc -- 3
			, null d_sales -- 4
			
			, d_volume  as ly_d_volume
		  	, d_qty as ly_d_qty
			, d_pc as ly_d_pc
			, d_sales as ly_d_sales
	
			FROM temp_l_dw_price_increase  -- 去年数据
)t
GROUP BY 
			 order_type
			,channel
			,customer_code
			,customer_name
			,proj_name
			,proj_name_en
			,item_code 
			,item_code_ppg 
			,category
			,category_brand
			,category_product_type

      -- 日期字段
      		,sales_month
			,sales_year
			,sales_month_2
) -- SELECT * FROM temp_dw_price_increase;
,bb as(
SELECT 
			t.order_type,
			COALESCE(d_volume,0) as d_volume,  -- 1
			COALESCE(d_sales,0) as d_sales, -- 2
			COALESCE(d_pc,0) as d_pc, -- 3
			COALESCE(ly_d_volume,0) as ly_d_volume,  -- 1
			COALESCE(ly_d_sales,0) as ly_d_sales, -- 2
			COALESCE(ly_d_pc,0) as ly_d_pc, -- 3
			-- 公式序号2/序号1
			COALESCE(d_sales/NULLIF(d_volume,0),0) as price_per_unite, -- 4
			-- 公式序号1/sum(序号1) -- 5
			--  as qty_rate,

			-- 公式如果序号1or序号1去年为Y，则空 -- 6
			case when  d_volume + ly_d_volume is null then 1 else null end as sales_y,
			-- 公式如果序号1or序号1去年为0，否则计算（序号2/序号1-序号2去年/序号1去年）*序号1 -- 9
			if(COALESCE(d_volume,0)*COALESCE(ly_d_volume,0)=0,0,COALESCE(d_volume,0)*(COALESCE(d_sales/NULLIF(d_volume,0),0) - COALESCE(ly_d_sales/NULLIF(ly_d_volume,0),0)) ) sales_price,
			-- 8
			-- 9
			-- 公式等于序号7 -- 10
			-- 公式-(序号2-序号3)/序号1-(序号2去年-序号3去年)/序号1去年)*序号1
			if(COALESCE(d_volume,0)*COALESCE(ly_d_volume,0)=0,0,( COALESCE(((COALESCE(d_sales,0)-COALESCE(d_pc,0))/NULLIF(COALESCE(d_volume,0),0)),0)- COALESCE(((COALESCE(ly_d_sales,0)-COALESCE(ly_d_pc,0))/NULLIF(COALESCE(ly_d_volume,0),0)),0)  )) as pc_svc, -- 11
			-- 公式if 序号6=Y 则 if 序号2去年=0 then 序号8*(序号3/序号2) else if 序号2=0 then  序号8*(序号3去年/序号2去年) else 序号8*(序号3/序号2) -- 12
			-- 公式序号9*序号3去年/序号2去年 -- 13
			-- 公式序号3-序号3去年 
			COALESCE(d_pc,0) - COALESCE(ly_d_pc,0) as final_variance, -- 14
			-- 公式序号3/序号2
			COALESCE(d_pc,0)/NULLIF(d_sales,0) as final_pc_rate, -- 15
			-- 16
			COALESCE(ly_d_volume,0) as prior_d_volume, -- 1去年
			COALESCE(ly_d_sales,0) as prior_d_sales, -- 2 去年
			COALESCE(ly_d_pc,0) as prior_d_pc, -- 3去年
			t.customer_code,
			t.customer_name,
			t.proj_name,
			t.proj_name_en,
			t.item_code,
			t.item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			-- t.proj_name,
			-- t.proj_name_en,
			t.sales_month,
			t.sales_year,
            t.sales_month_2
FROM temp_dw_price_increase t

)  -- SELECT * from bb;
,prior as (
		SELECT
		SUM(d_volume) AS d_volume_sum, -- 1的全部
		SUM(ly_d_volume) AS ly_d_volume_sum, -- 1的全部的去年
		channel,
		sales_month,
		sales_month_2,
		sales_year,
		order_type
		FROM bb t
		GROUP BY
		channel,
		sales_month,
		sales_month_2,
		sales_year,
		order_type
)  -- SELECT * FROM prior;
,bbb as(


    SELECT 
		t.order_type,
    	t.d_volume, -- 1
		t.d_sales, -- 2
		t.d_pc, -- 3
    	t.ly_d_volume, -- 1去年
		t.ly_d_sales, -- 2去年
		t.ly_d_pc, -- 3去年
		t.price_per_unite, -- 
		-- 公式序号1/sum(序号1) -- 4
		t.d_volume/NULLIF(SUM(t.d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year),0) as qty_rate, -- 4
		t.ly_d_volume/NULLIF(SUM(t.ly_d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year),0) as prior_qty_rate, -- 4去年
		-- t.d_volume/NULLIF(prior.d_volume_sum,0) as qty_rate, -- 5
		-- t.ly_d_volume/NULLIF(prior.ly_d_volume_sum,0) as ly_qty_rate, -- 5去年
		t.sales_y, -- 6
		t.sales_price, -- 7
		-- 公式如果序号6=Y 则序号2-序号2去年，else （序号5去年*sum（序号1）-序号1去年）*序号2去年/序号1去年
		-- if(d_volume*ly_d_volume=0, d_sales - ly_d_sales, (1*d_volume - ly_d_volume)*(ly_d_sales/ly_d_volume))

		-- 1  t.ly_d_volume/NULLIF(SUM(t.ly_d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year),0)
		-- 2  SUM(t.d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year)
		-- 3  t.ly_d_volume
		-- 4  t.ly_d_sales/NULLIF(ly_d_volume,0)
		if(d_volume*ly_d_volume=0, d_sales - ly_d_sales, 
		COALESCE(((t.ly_d_volume/NULLIF(SUM(t.ly_d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year),0))
		*(SUM(t.d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year))
		-t.ly_d_volume)
		*(t.ly_d_sales/NULLIF(ly_d_volume,0)))
		) as sales_volume,
		


		-- 公式if 序号6=Y 则 序号2-序号2去年 else (1-序号5去年*sum(序号1)）*序号2去年/序号1去年  -- 9

		-- 2  t.ly_d_volume/NULLIF(SUM(t.ly_d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year),0)
		-- 3  SUM(t.d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year)
		-- 1  t.d_volume
		-- 4  t.ly_d_sales/NULLIF(ly_d_volume,0)
		if(d_volume*ly_d_volume=0,0,
		COALESCE((t.d_volume-(t.ly_d_volume/NULLIF(SUM(t.ly_d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year),0)*SUM(t.d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year)))*t.ly_d_sales/NULLIF(ly_d_volume,0),0)
		) sales_mix, -- 9

		-- t.d_volume sales_mix,
		t.sales_price as sales_price_7, -- 10 公式等于序号7
		t.pc_svc, -- 11
		-- 公式if 序号6=Y 则 if 序号2去年=0 then 序号8*(序号3/序号2) else if 序号2=0 then  序号8*(序号3去年/序号2去年) else 序号8*(序号3/序号2) 
		-- 12
		--  公式序号9*序号3去年/序号2去年 as pc_mix -- 13
		t.final_variance, -- 公式序号3-序号3去年 -- 14
		t.final_pc_rate, -- 公式序号3/序号2 -- 15
    	SUM(t.d_volume) OVER (PARTITION BY t.order_type,t.channel,t.sales_month,t.sales_year) AS sum_qty_rate, -- 1的合计
		t.prior_d_pc, -- 3去年
		t.prior_d_volume, -- 1去年
		t.prior_d_sales, -- 2 去年
		t.customer_code,
		t.customer_name,
		t.proj_name,
		t.proj_name_en,
		t.item_code,
		t.item_code_ppg,
		t.category,
		t.category_brand,
		t.category_product_type,
		t.channel,
		-- t.proj_name,
		-- t.proj_name_en,
		t.sales_month,
		t.sales_month_2,
		t.sales_year
	
    FROM bb t
	LEFT JOIN  prior
			ON 	t.channel = prior.channel
			and t.order_type = prior.order_type
			and t.sales_month = prior.sales_month
)  -- SELECT * from bbb;
, t as(
-- SELECT
-- 		t.order_type,
--         sum(t.d_volume) as d_volume, -- 1
-- 		sum(t.d_sales) as d_sales, -- 2
-- 		sum(t.d_pc) as d_pc, -- 3
-- 		sum(t.prior_d_pc) as prior_d_pc, -- 3去年
-- 		sum(t.prior_d_volume) as prior_d_volume, -- 1去年
-- 		sum(t.prior_d_sales) as prior_d_sales, -- 2 去年
-- 		sum(t.price_per_unite) as price_per_unite, -- 
-- 		sum(t.qty_rate) as qty_rate, -- 4
-- 		sum(t.prior_qty_rate) as prior_qty_rate, -- 4去年
-- 		sum(t.sales_y) as sales_y, -- 6
-- 		sum(t.sales_price) as sales_price, -- 7
-- 		sum(t.sales_volume) as sales_volume, -- 8
-- 		sum(t.sales_mix) as sales_mix, -- 9
-- 		sum(t.sales_price_7) as sales_price_7, -- 10
-- 		sum(t.pc_svc) as pc_svc, -- 11
-- 		sum(t.pc_volume) as pc_volume,-- 12
-- 		sum(t.pc_mix) as pc_mix, -- 13
-- 		sum(t.final_variance) as final_variance, -- 公式序号3-序号3去年 -- 14
-- 		sum(t.final_pc_rate) as final_pc_rate, -- 公式序号3/序号2 -- 15
-- 		t.item_code,
-- 		t.item_code_ppg,
-- 		-- t.customer_code,
-- 		-- t.customer_name,
-- 	    t.proj_name,
-- 		t.proj_name_en,
-- 		t.category,
-- 		t.category_brand,
-- 		t.category_product_type,
-- 		t.channel,
-- 		-- t.proj_name,
-- 		-- t.proj_name_en,
-- 		t.sales_month,
-- 		t.sales_month_2,
-- 		t.sales_year,
-- 		t.sales_month_en
-- FROM
-- 	(
SELECT 
		t.order_type,
        t.d_volume, -- 1
		t.d_sales, -- 2
		t.d_pc, -- 3
		t.prior_d_pc, -- 3去年
		t.prior_d_volume, -- 1去年
		t.prior_d_sales, -- 2 去年
		t.price_per_unite, -- 
		t.qty_rate, -- 4
		t.prior_qty_rate, -- 4去年
		t.sales_y, -- 6
		t.sales_price, -- 7
		t.sales_volume, -- 8
		t.sales_mix, -- 9
		t.sales_price_7, -- 10
		t.pc_svc, -- 11
		-- 公式if 序号6=Y 则 if 序号2去年=0 then 序号8*(序号3/序号2) else if 序号2=0 then  序号8*(序号3去年/序号2去年) else 序号8*(序号3/序号2) 
		COALESCE(d_pc - prior_d_pc - sales_price - pc_svc - if(COALESCE(prior_d_sales,0)=0,0,prior_d_pc/NULLIF(prior_d_sales,0)*sales_mix),0) as pc_volume,-- 14
		--  公式序号9*序号3去年/序号2去年 as pc_mix -- 13
		if(COALESCE(prior_d_sales,0)=0,0,COALESCE(prior_d_pc/NULLIF(prior_d_sales,0)*sales_mix,0)) as pc_mix, -- 13
		t.final_variance, -- 公式序号3-序号3去年 -- 14
		t.final_pc_rate, -- 公式序号3/序号2 -- 15
		t.item_code,
		t.item_code_ppg,
		t.customer_code,
		t.customer_name,
	    t.proj_name,
		t.proj_name_en,
		t.category,
		t.category_brand,
		t.category_product_type,
		t.channel,
		-- t.proj_name,
		-- t.proj_name_en,
		t.sales_month,
		t.sales_month_2,
		t.sales_year,
		DATE_FORMAT(STR_TO_DATE(CONCAT(t.sales_month, '01'), '%Y%m%d'), '%b') as sales_month_en
FROM bbb t
-- )t
-- GROUP BY
-- 		t.order_type,
-- 		t.item_code,
-- 		t.item_code_ppg,
-- 		-- t.customer_code,
-- 		-- t.customer_name,
-- 	    t.proj_name,
-- 		t.proj_name_en,
-- 		t.category,
-- 		t.category_brand,
-- 		t.category_product_type,
-- 		t.channel,
-- 		-- t.proj_name,
-- 		-- t.proj_name_en,
-- 		t.sales_month,
-- 		t.sales_month_2,
-- 		t.sales_year,
-- 		t.sales_month_en
)  -- SELECT * from t;


, month_actual as(
	-- 1
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,d_volume as sales_value,'VOLUME' as sec_1,'ACTUAL' as sec_2,t.sales_month FROM t
-- 2
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,d_sales,'Sales' as sec_1,'ACTUAL' as sec_2,t.sales_month FROM t
-- -- 3
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,d_pc,'PC' as sec_1,'ACTUAL' as sec_2,t.sales_month FROM t
-- -- 4
-- UNION ALL
-- SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,price_per_unite,'Price per Unit' as sec_1,'ACTUAL' as sec_2,t.sales_month FROM t
-- -- 5
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,qty_rate,'VOLUME%' as sec_1,'ACTUAL' as sec_2,t.sales_month FROM t

-- ------------------- 去年数据
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,prior_d_volume as sales_value,'VOLUME' as sec_1,'PRIOR' as sec_2,t.sales_month_2 FROM t
-- 2
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,prior_d_sales,'Sales' as sec_1,'PRIOR' as sec_2,t.sales_month_2 FROM t
-- -- 3
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,prior_d_pc,'PC' as sec_1,'PRIOR' as sec_2,t.sales_month_2 FROM t
-- -- 4
-- UNION ALL
-- SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,price_per_unite,'Price per Unit' as sec_1,CONCAT('FY',SUBSTR(sales_year,3,2),' ',sales_month_en)  as sec_2,t.sales_month FROM t
-- -- 5
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,prior_qty_rate,'VOLUME%' as sec_1,'PRIOR' as sec_2,t.sales_month_2 FROM t

-- -----------------------------
-- -- 6
-- UNION ALL
-- SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,sales_y,'Falg' as sec_1,CONCAT('FY',SUBSTR(sales_year,3,2),' ',sales_month_en,' SALES') as sec_2,t.sales_month FROM t
-- -- 9
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,sales_price,'Price' as sec_1,CONCAT(sales_month_en,' SALES') as sec_2,t.sales_month FROM t
-- 10
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,sales_volume,'Volume' as sec_1,CONCAT(sales_month_en,' SALES') as sec_2,t.sales_month FROM t
-- 11
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,sales_mix,'Mix' as sec_1,CONCAT(sales_month_en,' SALES') as sec_2,t.sales_month FROM t 					
-- 12
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,sales_price_7,'Price' as sec_1,CONCAT(sales_month_en,' PC') as sec_2,t.sales_month FROM t
-- 13
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,pc_svc,'SVC' as sec_1,CONCAT(sales_month_en,' PC') as sec_2,t.sales_month FROM t
-- 14
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,pc_volume,'Volume' as sec_1,CONCAT(sales_month_en,' PC') as sec_2,t.sales_month FROM t
-- 15
UNION ALL
SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,pc_mix,'Mix' as sec_1,CONCAT(sales_month_en,' PC') as sec_2,t.sales_month FROM t
-- -- 14
-- UNION ALL
-- SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,final_variance,'Variance' as sec_1,'PC' as sec_2,t.sales_month FROM t
-- -- 15
-- UNION ALL
-- SELECT t.order_type,t.channel,t.customer_code,t.customer_name,t.proj_name,t.proj_name_en,t.item_code,t.item_code_ppg,t.category,t.category_brand,t.category_product_type,final_pc_rate,'PC%' as sec_1,CONCAT('FY',SUBSTR(sales_year,3,2),' ',sales_month_en,' PC%') as sec_2,t.sales_month FROM t
) 
-- SELECT DISTINCT sec_1 FROM month_actual;
SELECT  -- DISTINCT orderno.order_no
-- 	count(1)
	t.order_type,
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
	case when orderno.name_1 = 'VOLUME%' then t.sales_value*10000 else t.sales_value end as sales_value,
	t.sales_month,
	orderno.report_year,
	orderno.order_no,
	orderno.sec_1,
	orderno.sec_2,
	orderno.sec_3,
	STR_TO_DATE(CONCAT( orderno.report_year,'0101') , '%Y%m%d') as report_date,
	SYSDATE() as etl_time
FROM month_actual t
	LEFT  JOIN  temp_dw_order_report orderno
	on upper(orderno.name_1) = upper(t.sec_1)
	and upper(orderno.sec_2) = upper(t.sec_2)
	and orderno.order_month = t.sales_month
where orderno.order_no is not null
-- and sales_value is not null
-- and  proj_name = '一汽丰田' 
-- and item_code  = 'P980-5000.TOY/5L-C3'
-- and orderno.order_no = 11
-- and district ='NATIONAL' and orderno.sec_1 ='Y24 JAN'