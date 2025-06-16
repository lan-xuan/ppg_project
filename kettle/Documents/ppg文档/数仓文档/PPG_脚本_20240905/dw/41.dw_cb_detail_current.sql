
-- 20240901 增加了项目级别取sales month最大日期的逻辑，修改了service fee不为空，则service fee2为空逻辑
/*
目标表：fine_dw.dw_cb_detail
来源表：
fine_dw.dw_transaction_detail_report
fine_dw.dw_item_mapping_cb
fine_dw.dw_item_flag

fine_dw.dw_svc_detail
fine_dw.dw_item_mapping_distributor
fine_dw.dw_order_filter

更新方式：全量更新

参数：${mysql_yesterday_d_month} = '202401'
*/

-- 2年1月份数据 84125 44000 42834 -- 83815 43690
-- 	delete from  fine_dw.dw_cb_detail
-- where sales_month =${mysql_yesterday_d_month};
-- INSERT INTO fine_dw.dw_cb_detail (
-- business_type,
-- customer_code,
--  customer_name,
--  sales_month,
--  sales_quarter,
--  proj_name,
--  proj_name_en,
--  vendor_code,
--  vendor_name,
--  channel,
--  district,
--  item_code,
--  item_code_ppg,
--  category,
--  category_brand,
--  category_product_type,
--  report_brand_group,
--  report_brand_name,
--  sales_qty,
--  service_fee,
--  service_fee2,
--  service_rate,
--  rebate_rate,
--  commision_fee_rate,
--  reward_rate,
--  bs_price,
--  is_flag,
--  pc,
--  svc,
--  sales_value,
--  distributor_price,
--  sales_volume,
--  ship_to_code,
--  warehouse_code,
--  data_resource,
--  etl_time,
--  sales_year,
--  report_date,
--  uom_ltr_flag) 

with temp_dw_transaction_detail_report as (
select * from fine_dw.dw_transaction_detail_report
  where SUBSTRING(sales_month,1,6) = ${mysql_yesterday_d_month}
--  where SUBSTRING(sales_month,1,6) = ${mysql_yesterday_d_month}
--  and item_code in ('P190-588/5L-C3','P190-588.HX/5L-C3')
--  and vendor_code ='12222'
-- and item_code = 'P426-PP05.HX/1L-C3'
-- and proj_name_en = 'INFINITI'


),
temp_dw_svc_detail as (
select * from fine_dw.dw_svc_detail
 where SUBSTRING(svc_month,1,6) = ${mysql_yesterday_d_month}
--  where SUBSTRING(svc_month,1,6) = ${mysql_yesterday_d_month}

)


, t as(
			select 
			cb.item_code_ppg,
			isflag.is_flag as is_flag1,
			t.*,
			DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(t.sales_month,'01'), '%Y%m%d'), INTERVAL 1 month), '%Y%m') as sales_month_2 -- 关联字段 
			from temp_dw_transaction_detail_report t
			LEFT JOIN (SELECT DISTINCT item_code_ppg,item_code FROM fine_dw.dw_item_mapping_cb) cb
			on cb.item_code = t.item_code



			-- 第1步：匹配ods_item_flag_df.item_code，如果is_flag='是'则该字段为‘是’
			-- 其他根据项目走，剩余的为否
			left join fine_dw.dw_item_flag isflag
			on isflag.item_code = t.item_code
			WHERE 1=1
			and  t.channel in ('MM','MSO')
			and  t.business_type in ( '回购','补货')

		),

temp_dw_item_mapping_cb1  as (

				-- SELECT DISTINCT item_code,max(bs_price) as bs_price,bs_starting_date,bs_ending_date,proj_name,channel,
				--                 DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(bs_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				--                  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(bs_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				--  FROM fine_dw.dw_item_mapping_cb c
				-- WHERE (service_type <> '总服务商' or service_type is null)
				-- GROUP BY item_code,bs_starting_date,bs_ending_date,proj_name,channel


				select item_code,bs_price,bs_starting_date,bs_ending_date,proj_name,channel,
					DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(bs_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(bs_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				from 
				
		            (SELECT item_code,bs_price,bs_starting_date,bs_ending_date,proj_name,channel
		                  ,ROW_NUMBER() OVER (PARTITION BY item_code,proj_name,channel ORDER BY bs_starting_date desc,bs_ending_date DESC) AS seq  
		                FROM fine_dw.dw_item_mapping_cb
										WHERE (service_type <> '总服务商' or service_type is null)
										and bs_starting_date is not null
										and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(bs_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
										<= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(${mysql_yesterday_d_month},'01'), '%Y%m%d'), INTERVAL 1 month), '%Y%m')
		                ) A
		                WHERE A.seq =1

)
,
temp_dw_item_mapping_cb2  as (

				-- SELECT DISTINCT item_code,max(service_fee) as service_fee,service_fee_starting_date,service_fee_ending_date,proj_name,channel,warehouse_code,
				-- 				         DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				--                  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_fee_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				-- FROM fine_dw.dw_item_mapping_cb
				-- WHERE service_type = '总服务商'
				-- GROUP BY item_code,service_fee_starting_date,service_fee_ending_date,proj_name,channel,warehouse_code

				select item_code,service_fee,service_fee_starting_date,service_fee_ending_date,proj_name,channel,warehouse_code,
					DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_fee_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				from 
				
		            (SELECT item_code,service_fee,service_fee_starting_date,service_fee_ending_date,proj_name,channel,warehouse_code
		                  ,ROW_NUMBER() OVER (PARTITION BY item_code,proj_name,channel,warehouse_code ORDER BY service_fee_starting_date desc,service_fee_ending_date DESC) AS seq  
		                FROM fine_dw.dw_item_mapping_cb
										WHERE service_type = '总服务商' 
										and service_fee_starting_date is not null
										-- and item_code in ('P190-588/5L-C3','P190-588.HX/5L-C3')
										and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
										<= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(${mysql_yesterday_d_month},'01'), '%Y%m%d'), INTERVAL 1 month), '%Y%m')
		                ) A
		                WHERE A.seq =1

)
,temp_dw_item_mapping_cb22  as (

				-- SELECT DISTINCT item_code,max(service_fee) as service_fee,service_fee_starting_date,service_fee_ending_date,proj_name,channel,
				-- 				         DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				--                  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_fee_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				-- FROM fine_dw.dw_item_mapping_cb
				-- WHERE (service_type <> '总服务商' or service_type is null)
				-- GROUP BY item_code,service_fee_starting_date,service_fee_ending_date,proj_name,channel
select item_code,service_fee,service_fee_starting_date,service_fee_ending_date,proj_name,channel,
					DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_fee_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				from 
				
		            (SELECT item_code,service_fee,service_fee_starting_date,service_fee_ending_date,proj_name,channel
		                  ,ROW_NUMBER() OVER (PARTITION BY item_code,proj_name,channel ORDER BY service_fee_starting_date desc,service_fee_ending_date DESC) AS seq  
		                FROM fine_dw.dw_item_mapping_cb
										WHERE (service_type <> '总服务商' or service_type is null)
										and service_fee_starting_date is not null
-- 										and item_code in ('P190-588/5L-C3','P190-588.HX/5L-C3')
										and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
										<= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(${mysql_yesterday_d_month},'01'), '%Y%m%d'), INTERVAL 1 month), '%Y%m')
		                ) A
		                WHERE A.seq =1

)
,
temp_dw_item_mapping_cb3  as (

				-- SELECT DISTINCT item_code,max(service_rate) as service_rate,service_rate_starting_date,service_rate_ending_date,proj_name,channel,
				         
				--                	 DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				--                  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_rate_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				-- FROM fine_dw.dw_item_mapping_cb
				-- -- WHERE service_type = '总服务商' 
				-- GROUP BY item_code,service_rate_starting_date,service_rate_ending_date,proj_name,channel
select item_code,service_rate,service_rate_starting_date,service_rate_ending_date,proj_name,channel,
					DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_rate_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				from 
				
		            (SELECT item_code,service_rate,service_rate_starting_date,service_rate_ending_date,proj_name,channel
		                  ,ROW_NUMBER() OVER (PARTITION BY item_code,proj_name,channel ORDER BY service_rate_starting_date desc,service_rate_ending_date DESC) AS seq  
		                FROM fine_dw.dw_item_mapping_cb
										WHERE 1=1 -- (service_type <> '总服务商' or service_type is null)
										and service_rate_starting_date is not null
-- 										and item_code = 'P426-PP05.HX/1L-C3'
										and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(service_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
										<= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(${mysql_yesterday_d_month},'01'), '%Y%m%d'), INTERVAL 1 month), '%Y%m')
		                ) A
		                WHERE A.seq =1

)
,
temp_dw_item_mapping_cb4  as (

				-- SELECT DISTINCT item_code,max(rebate_rate) as rebate_rate,rebate_rate_starting_date,rebate_rate_ending_date,proj_name,channel,
				-- 				         DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(rebate_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				--                  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(rebate_rate_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				-- FROM fine_dw.dw_item_mapping_cb
				-- WHERE (service_type <> '总服务商' or service_type is null)
				-- GROUP BY item_code,rebate_rate_starting_date,rebate_rate_ending_date,proj_name,channel

				select item_code,rebate_rate,rebate_rate_starting_date,rebate_rate_ending_date,proj_name,channel,
					DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(rebate_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(rebate_rate_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				from 
				
		            (SELECT item_code,rebate_rate,rebate_rate_starting_date,rebate_rate_ending_date,proj_name,channel
		                  ,ROW_NUMBER() OVER (PARTITION BY item_code,proj_name,channel ORDER BY rebate_rate_starting_date desc,rebate_rate_ending_date DESC) AS seq  
		                FROM fine_dw.dw_item_mapping_cb
										WHERE (service_type <> '总服务商' or service_type is null)
										and rebate_rate_starting_date is not null
										and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(rebate_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
										<= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(${mysql_yesterday_d_month},'01'), '%Y%m%d'), INTERVAL 1 month), '%Y%m')
		                ) A
		                WHERE A.seq =1

)
,
temp_dw_item_mapping_cb5  as (

				-- SELECT DISTINCT item_code,max(commision_fee_rate) as commision_fee_rate,commision_fee_starting_date,commision_fee_ending_date,proj_name,channel,
				-- 								DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(commision_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				--                  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(commision_fee_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				-- FROM fine_dw.dw_item_mapping_cb
				-- -- WHERE (service_type <> '总服务商' or service_type is null)
				-- GROUP BY item_code,commision_fee_starting_date,commision_fee_ending_date,proj_name,channel
				select item_code,commision_fee_rate,commision_fee_starting_date,commision_fee_ending_date,proj_name,channel,
					DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(commision_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(commision_fee_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				from 
				
		            (SELECT item_code,commision_fee_rate,commision_fee_starting_date,commision_fee_ending_date,proj_name,channel
		                  ,ROW_NUMBER() OVER (PARTITION BY item_code,proj_name,channel ORDER BY commision_fee_starting_date desc,commision_fee_ending_date DESC) AS seq  
		                FROM fine_dw.dw_item_mapping_cb
										WHERE 1=1 -- (service_type <> '总服务商' or service_type is null)
										and commision_fee_starting_date is not null
-- 										and item_code in ('P190-588/5L-C3','P190-588.HX/5L-C3')
										and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(commision_fee_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
										<= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(${mysql_yesterday_d_month},'01'), '%Y%m%d'), INTERVAL 1 month), '%Y%m')
		                ) A
		                WHERE A.seq =1

)
,
temp_dw_item_mapping_cb6 as (

				-- SELECT DISTINCT item_code,max(reward_rate) as reward_rate,reward_rate_starting_date,reward_rate_ending_date,proj_name,channel,
				-- 											DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(reward_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				--                  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(reward_rate_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				-- FROM fine_dw.dw_item_mapping_cb
				-- WHERE (service_type <> '总服务商' or service_type is null)
				-- GROUP BY item_code,reward_rate_starting_date,reward_rate_ending_date,proj_name,channel
				select item_code,reward_rate,reward_rate_starting_date,reward_rate_ending_date,proj_name,channel,
					DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(reward_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') AS bs_starting_date_2,
				  DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(reward_rate_ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')   as bs_ending_date_2
				from 
				
		            (SELECT item_code,reward_rate,reward_rate_starting_date,reward_rate_ending_date,proj_name,channel
		                  ,ROW_NUMBER() OVER (PARTITION BY item_code,proj_name,channel ORDER BY reward_rate_starting_date desc,reward_rate_ending_date DESC) AS seq  
		                FROM fine_dw.dw_item_mapping_cb
										WHERE (service_type <> '总服务商' or service_type is null)
										and reward_rate_starting_date is not null
										and DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(reward_rate_starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m') 
										<= DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(${mysql_yesterday_d_month},'01'), '%Y%m%d'), INTERVAL 1 month), '%Y%m')
		                ) A
		                WHERE A.seq =1

),
temp_dw_item_mapping_distributor as (

select d.*, 
       DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(d.starting_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')  as starting_date_2,
       DATE_FORMAT(DATE_SUB(STR_TO_DATE(concat(substr(d.ending_date,1,6),'01'), '%Y%m%D'), INTERVAL 0 MONTH), '%Y%m')  as ending_date_2
from fine_dw.dw_item_mapping_distributor d

),
cb as(
		-- 84125
		select
		t.business_type,
		t.customer_code,
		t.customer_name,
		t.sales_month,
		t.sales_quarter,
		t.proj_name,
		t.proj_name_en,
		t.vendor_code,
		t.vendor_name,
		t.channel,
		t.district,
		t.item_code,
		t.item_code_ppg,
		t.category,
		t.category_brand,
		t.category_product_type,
		t.report_brand_group,
		t.report_brand_name,
		case when t.sales_value<0 and t.sales_qty is null then t.credit_qty else t.sales_qty end as sales_qty,
-- 		1 as service_fee,
		cb2.service_fee,-- 总服务商费2
		case when cb2.service_fee is not null then null else cb22.service_fee end as service_fee2,-- 服务商服务费
		cb3.service_rate, -- 服务商服务费率
		cb4.rebate_rate, -- 主机厂/集团返利率
		cb5.commision_fee_rate,  -- 代理商服务费率 
		cb6.reward_rate,  -- 奖励金率
		cb1.bs_price, -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应门店价格
		case when t.is_flag1 is not null then t.is_flag1 else t.is_flag end as is_flag ,
		-- case when svc.svc is null then svc2.svc else svc.svc end as svc,
		case when sales_qty is null and svc1.svc is null then ((d.distributor_price-svc2.svc)*t.credit_qty) 
		when sales_qty is not null and svc1.svc is null then ((d.distributor_price-svc2.svc)*t.sales_qty)  
		when sales_qty is null and svc1.svc is not null then ((d.distributor_price-svc1.svc)*t.credit_qty) 
		when sales_qty is not null and svc1.svc is not null then ((d.distributor_price-svc1.svc)*t.sales_qty) end as pc, -- pc=(sales_value-svc*invoice qty)
		case when svc1.svc is null then svc2.svc else svc1.svc end as svc,
		t.sales_value,
		d.distributor_price,
		t.sales_volume,
		t.ship_to_code,
		t.warehouse_code,
		'fine_dw.dw_transaction_detail_report/fine_dw.dw_item_mapping_cb/fine_dw.dw_item_flag/fine_dw.dw_svc_detail/fine_dw.dw_item_mapping_distributor/fine_dw.dw_order_filter' as data_resource, 
		SYSDATE() as etl_time,
		SUBSTRING(t.sales_month,1,4) as sales_year,
		STR_TO_DATE(CONCAT( t.sales_month,'01') , '%Y%m%d') as report_date,
		t.uom_ltr_flag
		from t

		-- 第2步：
		-- svc匹配规则根据dw_svc_detail.item_code_ppg优先匹配table_type='sh'按照日期区间获得svc
		-- pc=(sales_value-svc*invoice qty)；如果还匹配不到，到当月的dw_sales_pc_customer中根据item计算svc：svc=(sales_value-pc)/sales_qty
		left join temp_dw_svc_detail svc1
		on svc1.svc_month   = t.sales_month
		and svc1.item_code = t.item_code_ppg

		left join temp_dw_svc_detail svc2
		on svc2.svc_month   = t.sales_month
		and svc2.item_code = t.item_code
		-- left join (
		--     SELECT
		-- 		item_code,
		-- 		DATE_FORMAT(STR_TO_DATE(sales_date, '%Y%m%d'), '%Y%m') sales_month,
		-- 		AVG((sales_value-pc)/case when sales_qty = 0 then null else sales_qty end) as svc
		-- 		FROM fine_ods.ods_sales_pc_customer_df
		-- 		WHERE update_date in ('202401','202201')
		-- 		group by item_code,
		-- 		DATE_FORMAT(STR_TO_DATE(sales_date, '%Y%m%d'), '%Y%m')
		-- 		
		-- ) svc2
		-- on svc2.item_code = t.item_code
		-- and svc2.sales_month = t.sales_month

		-- -- 第4步：dw_item_maping_distributor.item_code_ppg匹配获得经销商价格（其中starting_date<=sales_date-1<=ending_date)
		left join temp_dw_item_mapping_distributor d
		on d.item_code = t.item_code_ppg


	     AND t.sales_month_2 >= starting_date_2
         AND (
             t.sales_month_2 <=  ending_date_2
             OR d.ending_date IS NULL
         )
     
		-- LEFT JOIN (select DISTINCT item_code, item_code_ppg from fine_ods.ods_item_mapping_cb_df) ppgcode
		-- on t.item_code = ppgcode.item_code

	-- 根据item_code、proj_name匹配所属时间区间的对应服务费
    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 门店价格
        LEFT JOIN temp_dw_item_mapping_cb1 cb1
        ON cb1.item_code = t.item_code
		    AND cb1.proj_name = t.proj_name_en
		    AND cb1.channel = t.channel
			 AND t.sales_month_2 >= cb1.bs_starting_date_2
			 AND (
				t.sales_month_2 <= cb1.bs_ending_date_2
				OR cb1.bs_ending_date IS NULL
			)

    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 服务费,先匹配warehouse_code
        LEFT JOIN temp_dw_item_mapping_cb2 cb2
        ON cb2.item_code = t.item_code
		  AND cb2.proj_name = t.proj_name_en
		  AND cb2.channel = t.channel
			and cb2.warehouse_code  = t.warehouse_code
			AND t.sales_month_2 >=  cb2.bs_starting_date_2
			AND (
				t.sales_month_2  <=  cb2.bs_ending_date_2
				OR cb2.service_fee_ending_date IS NULL
			)
    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 服务费, 再匹配其他
        LEFT JOIN temp_dw_item_mapping_cb22 cb22
        ON cb22.item_code = t.item_code
		  AND cb22.proj_name = t.proj_name_en
		  AND cb22.channel = t.channel
			AND t.sales_month_2 >=  cb22.bs_starting_date_2
			AND (
				t.sales_month_2  <=  cb22.bs_ending_date_2
				OR cb22.service_fee_ending_date IS NULL
			)
    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 总服务商服务费率
        LEFT JOIN temp_dw_item_mapping_cb3 cb3
        ON cb3.item_code = t.item_code
		   AND cb3.proj_name = t.proj_name_en
		   AND cb3.channel = t.channel
		   AND t.sales_month_2 >=  cb3.bs_starting_date_2
			AND (
				t.sales_month_2  <=   cb3.bs_ending_date_2
				OR cb3.service_rate_ending_date IS NULL
			)		
    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 主机厂/集团返利率
        LEFT JOIN temp_dw_item_mapping_cb4 cb4
        ON cb4.item_code = t.item_code
		   AND cb4.proj_name = t.proj_name_en
		   AND cb4.channel = t.channel
		   AND t.sales_month_2 >= cb4.bs_starting_date_2
			 AND (
				t.sales_month_2  <=  cb4.bs_ending_date_2
				OR cb4.rebate_rate_ending_date IS NULL
			)	
    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 代理商服务费率 -- commision_fee_rate
        LEFT JOIN temp_dw_item_mapping_cb5  cb5
        ON cb5.item_code = t.item_code
		   AND cb5.proj_name = t.proj_name_en
		   AND cb5.channel = t.channel
		   AND t.sales_month_2>= cb5.bs_starting_date_2
			AND (
				t.sales_month_2  <= cb5.bs_ending_date_2
				OR cb5.commision_fee_ending_date IS NULL
			)
    -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应 奖励金率
        LEFT JOIN temp_dw_item_mapping_cb6  cb6 
        ON cb6.item_code = t.item_code
		   AND cb6.proj_name = t.proj_name_en
		   AND cb6.channel = t.channel
		   AND t.sales_month_2 >=   cb6.bs_starting_date_2
			 AND (
				t.sales_month_2  <=   cb6.bs_ending_date_2
				OR cb6.reward_rate_ending_date IS NULL
			)

		where 1=1 
		-- 仅保留order type not in  "STANDARD ORDER - SH
		-- BILL ONLY ORDER - WU
		-- STANDARD ORDER - WU" and channel in (‘MM’,’MSO’)计算
		-- and t.item_code ='P999-XR12/0.5L-C3'
		-- and t.customer_code = '205374'
)
select

		business_type,
		customer_code,
		customer_name,
		sales_month,
		sales_quarter,
		proj_name,
		proj_name_en,
		vendor_code,
		vendor_name,
		channel,
		district,
		item_code,
		item_code_ppg,
		category,
		category_brand,
		category_product_type,
		report_brand_group,
		report_brand_name,
		sales_qty,
		service_fee,
		case when ship_to_code = '333878' then 0 else service_fee2 end as service_fee2,  -- 服务费
		service_rate, -- 服务商服务费率
		rebate_rate, -- 主机厂/集团返利率
		commision_fee_rate,  -- 代理商服务费率 
		reward_rate,  -- 奖励金率
		bs_price, -- 根据dw_item_mapping_cb.item_code、proj_name匹配所属时间区间的对应门店价格
		is_flag ,
		-- case when svc.svc is null then svc2.svc else svc.svc end as svc,
		pc, -- pc=(sales_value-svc*invoice qty)
		svc,
		sales_value,
		distributor_price,
		sales_volume,
		ship_to_code,
		warehouse_code,
		'fine_dw.dw_transaction_detail_report/fine_dw.dw_item_mapping_cb/fine_dw.dw_item_flag/fine_dw.dw_svc_detail/fine_dw.dw_item_mapping_distributor/fine_dw.dw_order_filter' as data_resource, 
		etl_time,
		sales_year,
		report_date,
		uom_ltr_flag
FROM
cb
				