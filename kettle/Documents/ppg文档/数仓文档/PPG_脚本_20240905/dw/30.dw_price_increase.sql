
/*
目标表：fine_dw.dw_price_increase
来源表：

fine_dw.dw_sales_pc_customer
fine_dw.dw_cb_detail
fine_dw.dw_item_brand

更新方式：增量更新 month 
参数 ：${mysql_yesterday_l_month}

*/




with temp_dw_sales_pc_customer as (
select * 
 from  fine_dw.dw_sales_pc_customer
 where  substr(sales_month,1,6)  = ${mysql_yesterday_l_month}



),

temp_dw_cb_detail as (
select * 
 from  fine_dw.dw_cb_detail
 where substr(sales_month,1,6)   = ${mysql_yesterday_l_month}
),

temp_dw_item_brand as 
(select DISTINCT item_code,category,category_brand,category_product_type 
  from  fine_dw.dw_item_brand
  )

 
--  688083 -- 809985 -- 2年1月数据809787 -- 423156 -- d数据242556 308380
 ,code as (
		SELECT DISTINCT item_code,customer_code,customer_name,CONCAT(sales_year,sales_month)sales_month,channel as channel,sales_year FROM(

		SELECT DISTINCT item_code,customer_code,customer_name,SUBSTR(sales_month,5,2) as sales_month, channel FROM temp_dw_sales_pc_customer s		
		where channel ='DISTRIBUTOR' or channel ='SCHOOL' 
		UNION
		SELECT DISTINCT item_code_ppg,vendor_code,vendor_name,SUBSTR(sales_month,5,2) as sales_month,'DISTRIBUTOR' channel FROM temp_dw_cb_detail s 

		)s  
		,( 
     select substr(${mysql_yesterday_l_month},1,4)   as sales_year
		     ) y	
		where 1=1
		and item_code is not null
		and customer_code is not null

),


s as(
	SELECT
		sum(sales_volume) as sales_volume,
		sum(sales_qty) as sales_qty,
		sum(sales_qty*distributor_price) as shipment_sales,
		sum(pc) as pc,
		sales_month,
		vendor_code,
		vendor_name,
		item_code_ppg
			FROM temp_dw_cb_detail s
			where 1=1
			and business_type = '回购'
			and is_flag = '否'
			and vendor_code <>'195726'
			and vendor_code <>'191204'
			GROUP BY
			sales_month,
			vendor_code,
			vendor_name,
			item_code_ppg
			
), t as(
		SELECT
			sum(sales_volume) as sales_volume,
			sum(sales_qty) as sales_qty,
			sum(sales_value) as sales_value,
			sum(pc) as pc,
			sales_month,
			customer_code,
			customer_name,
			item_code,
			category,
			category_brand,
			channel,
			proj_name,
			proj_name_en,
			category_product_type
				
			FROM temp_dw_sales_pc_customer s
			where 
			-- channel in( 'DISTRIBUTOR','SCHOOL')
			
			(channel ='DISTRIBUTOR' or channel ='SCHOOL')
			and (brand_name <> 'CENTRAL SUPPLY' or brand_name is null)
			and customer_code <>'195726'
			and customer_code <>'191204'
				GROUP BY
				sales_month,
				customer_code,
				customer_name,
				item_code,
				category,
				category_brand,
				channel,
				proj_name,
				proj_name_en,
				category_product_type
),t_full as (
SELECT 
			sales_volume,
			sales_qty,
			sales_value,
			pc,
			code.sales_month,
			code.customer_code,
			code.customer_name,
			code.item_code,
			brand.category,
			brand.category_brand,
			code.channel,
			proj_name,
			proj_name_en,
			brand.category_product_type
from code
LEFT JOIN t
on t.item_code = code.item_code
and t.sales_month = code.sales_month
and t.customer_code = code.customer_code
and t.channel = code.channel
left join temp_dw_item_brand brand
on code.item_code = brand.item_code
) 


  
, code_proj as (
		SELECT DISTINCT item_code,proj_name,proj_name_en,CONCAT(sales_year,sales_month)sales_month,channel,sales_year FROM(
		SELECT DISTINCT item_code,proj_name,proj_name_en,SUBSTR(sales_month,5,2) as sales_month,channel FROM  temp_dw_sales_pc_customer s		
		where 
		-- channel in( 'MM','MSO')
		
		channel ='MM' or channel ='MSO'
 	 

		)s  
		,( 

 select substr(${mysql_yesterday_l_month},1,4)   as sales_year
			  
			  ) y
		where 1=1
		and item_code is not null
		and proj_name is not null
), t_proj as(
		SELECT
			sum(sales_volume) as sales_volume,
			sum(sales_qty) as sales_qty,
			sum(sales_value) as sales_value,
			sum(pc) as pc,
			sales_month,
			-- customer_code,
			item_code,
			category,
			category_brand,
			channel,
			proj_name,
			proj_name_en,
			category_product_type
				
		  FROM temp_dw_sales_pc_customer s
			where 1=1 
 
				GROUP BY
				sales_month,
				-- customer_code,
				item_code,
				category,
				category_brand,
				channel,
				proj_name,
				proj_name_en,
				category_product_type
)
,t_full_proj as (
SELECT 
			sales_volume,
			sales_qty,
			sales_value,
			pc,
			code.sales_month,
			code.item_code,
			brand.category,
			brand.category_brand,
			code.channel,
			code.proj_name,
			code.proj_name_en,
			brand.category_product_type
from code_proj code
LEFT JOIN t_proj t
on t.item_code = code.item_code
and t.sales_month = code.sales_month
and t.proj_name = code.proj_name
left join temp_dw_item_brand brand
on code.item_code = brand.item_code
)

		SELECT
			t.customer_code,
			t.customer_name,
			t.item_code,
			t.item_code as item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			t.proj_name,
			t.proj_name_en,

			-- 销售数据
			sum(COALESCE(t.sales_volume, 0)) as d_volume, -- 1
			sum(COALESCE(t.sales_qty, 0)) as d_qty, -- 2
			sum(COALESCE(t.pc, 0)) as d_pc, -- 3
			sum(COALESCE(t.sales_value, 0)) as d_sales, -- 4
			
			-- 备货数据
			sum(COALESCE(s.sales_volume, 0)) as s_volume, -- 5
			sum(COALESCE(s.sales_qty, 0)) as s_qty,   -- 6
			sum(COALESCE(s.pc, 0)) as s_pc, -- 7 
			sum(COALESCE(s.shipment_sales, 0)) as s_sales, -- 8

			
			-- 净销售数据 = 销售数据 - 备货数据
			sum(COALESCE(t.sales_volume, 0) - COALESCE(s.sales_volume, 0)) as net_volume, -- 9
			sum(COALESCE(t.sales_qty, 0) - COALESCE(s.sales_qty, 0)) as net_qty, -- 10
			sum(COALESCE(t.pc, 0) - COALESCE(s.pc, 0)) as net_pc, -- 11
			sum(COALESCE(t.sales_value, 0) - COALESCE(s.shipment_sales, 0)) as net_sales, -- 12

			t.sales_month,
			substr(t.sales_month,1,4) as sales_year,
            'fine_dw.dw_sales_pc_customer/fine_dw.dw_cb_detail/fine_dw.dw_item_brand' as data_resource, 
            SYSDATE() as etl_time,
            STR_TO_DATE(CONCAT( t.sales_month,'01') , '%Y%m%d') as report_date

		FROM t_full t -- fine_dw.dw_sales_pc_customer
		LEFT JOIN s -- fine_dm.dm_stock_deduction 
		on t.sales_month = s.sales_month
		and t.item_code = s.item_code_ppg
		and t.customer_code = s.vendor_code
		and case when t.channel =  'DISTRIBUTOR' then 1=1
		else  1=2 end

		where 1=1
		GROUP BY 
				t.customer_code,
				t.customer_name,
				t.item_code,
				t.category,
				t.category_brand,
				t.category_product_type,
				t.channel,
				t.proj_name,
				t.proj_name_en,
				t.sales_month
	UNION ALL
	
		SELECT
			null as customer_code,
			null as customer_name,
			t.item_code,
			b.item_code_ppg,
			t.category,
			t.category_brand,
			t.category_product_type,
			t.channel,
			t.proj_name,
			t.proj_name_en,

			-- 销售数据
			sum(COALESCE(t.sales_volume, 0)) as d_volume, -- 1
			sum(COALESCE(t.sales_qty, 0)) as d_qty, -- 2
			sum(COALESCE(t.pc, 0)) as d_pc, -- 3
			sum(COALESCE(t.sales_value, 0)) as d_sales, -- 4

			
			-- 备货数据
			0 as  s_volume, -- 5
			0 as s_qty,   -- 6
			0 as s_pc, -- 7 
			0 as s_sales, -- 8

			
			-- 净销售数据 = 销售数据 - 备货数据
			sum(COALESCE(t.sales_volume, 0)) as net_volume, -- 9
			sum(COALESCE(t.sales_qty, 0)) as net_qty, -- 10
			sum(COALESCE(t.pc, 0)) as net_pc, -- 11
			sum(COALESCE(t.sales_value, 0)) as net_sales, -- 12

			t.sales_month,
			substr(t.sales_month,1,4) as sales_year,
		   'fine_dw.dw_sales_pc_customer/fine_dw.dw_cb_detail/fine_dw.dw_item_brand' as data_resource, 
       SYSDATE() as etl_time,
       STR_TO_DATE(CONCAT( t.sales_month,'01') , '%Y%m%d') as report_date

		FROM t_full_proj t -- fine_dw.dw_sales_pc_customer
		-- 其中MM/MSO根据dw_sales_pc_customer.item_code=dw_item_mapping_cb.item_code
		
		LEFT JOIN (
				SELECT DISTINCT
					UPPER(item_code) as item_code,
					UPPER(item_code_ppg) as item_code_ppg
				FROM
					fine_dw.dw_item_mapping_cb
			) b
		 on t.item_code = b.item_code

		
		where 1=1
		GROUP BY 
				-- t.customer_code,
				-- t.customer_name,
				t.item_code,
				b.item_code_ppg,
				t.category,
				t.category_brand,
				t.category_product_type,
				t.channel,
				t.proj_name,
				t.proj_name_en,
				t.sales_month