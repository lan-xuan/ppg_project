-- TRUNCATE TABLE fine_dm.dm_mso_actual_pc_report;
-- insert into fine_dm.dm_mso_actual_pc_report 
-- (
-- team_owner,
-- team_owner_id,
-- sales_person,
-- sales_person_id,
-- proj_name,
-- proj_name_en,
-- pc,
-- pc_rate,
-- service_fee,
-- service_rate,
-- rebate_rate,
-- c_rebate,
-- adjusted_fee,
-- vendor_rebate,
-- actual_pc,
-- actual_pc_rate,
-- commision_fee,
-- ebit,
-- ebit_rate,
-- sales_value,
-- sales_month,
-- data_resource,
-- etl_time,
-- report_date
-- )

with price_pc as(
SELECT sum(d_pc) as pc,channel,t.proj_name,t.proj_name_en,sales_month 
			FROM fine_dw.dw_price_increase t
			WHERE 1=1
			and t.channel = 'MSO'
			-- and t.sales_month  = '202401'
			GROUP BY 				t.channel,t.proj_name,t.proj_name_en,sales_month
)-- SELECT count(1) from price_pc;
,dw_cb_detail as (
SELECT 
			sum(sales_value) as sales_value,
			sum(cb.sales_qty) as sales_qty,
			sum(COALESCE(cb.service_fee * sales_qty,0)) as service_fee, -- —————费率*qty -- 总服务商费2
			sum(COALESCE(cb.service_fee2 * sales_qty,0)) as service_fee2, -- —————费率*qty -- 服务商服务费
			-- sum(COALESCE(cb.rebate_rate * sales_value,0)) as rebate_rate, -- —————费率*sales_value
			sum(COALESCE(cb.commision_fee_rate * sales_value,0)) as commision_fee, 
			sum(COALESCE(cb.service_rate * sales_value,0)) as service_rate, -- —————费率*sales_value  -- 总服务商费1
			sum(COALESCE(cb.rebate_rate * sales_value,0)) as rebate_rate,-- —————费率*sales_value -- MSO集团返利
			sum(COALESCE(cb.bs_price,0)) as bs_price,
			sum(COALESCE(cb.distributor_price,0)) as distributor_price,
			cb.channel,
			cb.proj_name,
			cb.proj_name_en,
			cb.sales_month 
			FROM fine_dw.dw_cb_detail cb
			WHERE 1=1
			and cb.channel = 'MSO'
			-- and cb.sales_month  = '202401'
			GROUP BY 
			cb.channel,
			cb.proj_name,
			cb.proj_name_en,
			cb.sales_month 
) --  SELECT count(1) from dw_cb_detail; -- 5119
,aa as(
    SELECT
		t.channel,
		t.proj_name,
        t.proj_name_en,
--         price_pc.pc,
--         price_pc.pc/NULLIF(t.sales_value, 0)  as pc_rate,
        t.service_fee, -- 费率*qty
		t.service_fee2,
        t.service_rate,
        t.rebate_rate, -- 服务商备货价格*qty*费率（其中上汽大众按门店价*qty*费率） 主机厂项目优质服务季度奖励金
        -- t.rebate_rate,
		COALESCE(adj.c_rebate,0) c_rebate, -- MM集采渠道返利
        COALESCE(adj.adjusted_fee,0) adjusted_fee, -- proj_name，所属月份匹配获得 -- —————调整金额
		COALESCE(adj.rebate,0) as vendor_rebate,  -- —————总服务商返利
        -- t.actual_pc, -- 公式=pc-1-2-3-4-5
        -- t.actual_pc_rate, -- 公式=actual_pc/(sales-3-4-5）
        t.commision_fee,
        -- t.ebit, -- 公式=actual_pc-6
        -- t.ebit_rate, -- 公式=ebit/(sales-3-4-5）
        t.sales_value,
		t.sales_month

    FROM dw_cb_detail t

            LEFT JOIN 
							(SELECT sum(COALESCE(c_rebate,0)) c_rebate,
						SUM(COALESCE(rebate,0)) rebate,
						sum(COALESCE(adjusted_fee,0)) adjusted_fee,  
						proj_name,adjusted_month
					FROM fine_dw.dw_adjusted_fee adj 
				GROUP BY proj_name,adjusted_month) adj
            on adj.proj_name = t.proj_name
			and adjusted_month = sales_month
    where 1=1
		
) -- SELECT * from aa;
,aaa as(
SELECT 
		t.channel,
		t.proj_name,
    	t.proj_name_en,
		t.sales_month,
		sum(service_fee) as service_fee,
		sum(service_fee2) as service_fee2,
		sum(service_rate) as service_rate,
		sum(rebate_rate) as rebate_rate,
		sum(c_rebate) as c_rebate,
		sum(adjusted_fee) as adjusted_fee,
		sum(vendor_rebate) as vendor_rebate,
		sum(commision_fee) as commision_fee,
		sum(sales_value) as sales_value
FROM aa t
GROUP BY 
		t.channel,
		t.proj_name,
    	t.proj_name_en,
		t.sales_month

) --  SELECT count(1) from aaa; - 24
, t as (
SELECT 
		t.channel,
		t.proj_name,
    	t.proj_name_en,
		t.sales_month,	
		price_pc.pc,
    	price_pc.pc/NULLIF(t.sales_value, 0)  as pc_rate,
		t.service_fee,
		t.service_fee2,
		t.service_rate,
		t.rebate_rate,
		t.c_rebate,
		t.adjusted_fee,
		t.vendor_rebate,
		t.commision_fee,
		t.sales_value
 from aaa t			
		LEFT JOIN price_pc
		
		ON t.proj_name = price_pc.proj_name
		and t.sales_month = price_pc.sales_month
		and t.channel = price_pc.channel
)
SELECT
		
    	cs.team_owner,
		cs.team_owner_id,
		cs.sales_person,
		cs.sales_person_id,
		-- t.channel,
		t.proj_name,
    	t.proj_name_en,
    sum(COALESCE(t.pc , 0)) as pc,
    sum(COALESCE(t.pc_rate , 0)) as pc_rate, 
    sum(COALESCE(t.service_fee , 0)) as service_fee, -- 1  ----  总服务商费2
	sum(COALESCE(t.service_fee2 , 0)) as service_fee2, -- 1  ----  服务商服务费service_fee2
    sum(COALESCE(t.service_rate , 0)) as service_rate, -- 2 ---- 总服务商费1
	sum(COALESCE(t.service_fee , 0)) + sum(COALESCE(t.service_rate , 0)) as service_fee_total,-- 总服务商费2 +总服务商费1=总服务商费
    sum(COALESCE(t.rebate_rate , 0)) as rebate_rate,  -- 3 ----  MSO集团返利
    sum(COALESCE(t.c_rebate , 0)) as c_rebate,  -- 4 ----  集采渠道返利 adjusted fee表c_rebate
    sum(COALESCE(t.adjusted_fee , 0)) as adjusted_fee,  -- 6  ----  调整金额 adjusted fee表adjusted fee
	sum(COALESCE(t.vendor_rebate , 0)) as vendor_rebate, -- 5 ---- 总服务商返利   adjusted fee表rebate
    sum(COALESCE(t.pc , 0) - COALESCE( t.service_fee , 0) - COALESCE( t.service_rate , 0) - COALESCE( t.service_fee2 , 0) - COALESCE( t.rebate_rate , 0) - COALESCE( t.c_rebate , 0) - COALESCE( t.vendor_rebate , 0) - COALESCE( t.adjusted_fee , 0)) as actual_pc, -- 公式=pc-1-2-3-4-5
	-- MSO: actual pc% = actual_pc/(项目结算销售额-MSO集团返利-MM主机厂渠道返利-总服务商返利-调整金额）
    sum(COALESCE(t.pc , 0) - COALESCE( t.service_fee , 0) - COALESCE( t.service_rate , 0) - COALESCE( t.service_fee2 , 0) - COALESCE( t.rebate_rate , 0) - COALESCE( t.c_rebate , 0) - COALESCE( t.vendor_rebate , 0) - COALESCE( t.adjusted_fee , 0))/NULLIF(sum(COALESCE( sales_value  , 0) - COALESCE( t.rebate_rate , 0) - COALESCE( t.c_rebate , 0) - COALESCE( t.adjusted_fee , 0) - COALESCE( t.vendor_rebate , 0)),0) as actual_pc_rate, 
    sum(COALESCE(t.commision_fee , 0))  as commision_fee, -- 7
    sum(COALESCE(t.pc , 0) - COALESCE( t.service_fee , 0) - COALESCE( t.service_rate , 0) - COALESCE( t.service_fee2 , 0) - COALESCE( t.rebate_rate , 0) - COALESCE( t.c_rebate , 0) - COALESCE( t.vendor_rebate , 0) - COALESCE( t.adjusted_fee , 0)- COALESCE( t.commision_fee ,0))  as ebit,  -- 公式=actual_pc-7
	-- eop% = eop/(项目结算销售额-MSO集团返利-MM主机厂渠道返利-总服务商返利-调整金额）
    sum(COALESCE(t.pc , 0) - COALESCE( t.service_fee , 0) - COALESCE( t.service_rate , 0) - COALESCE( t.service_fee2 , 0) - COALESCE( t.rebate_rate , 0) - COALESCE( t.c_rebate , 0) - COALESCE( t.vendor_rebate , 0) - COALESCE( t.adjusted_fee , 0)- COALESCE( t.commision_fee ,0))/NULLIF(sum(COALESCE( sales_value  , 0) - COALESCE( t.rebate_rate , 0) - COALESCE( t.c_rebate , 0) - COALESCE( t.adjusted_fee , 0) - COALESCE( t.vendor_rebate , 0)),0) as ebit_rate, -- 公式=ebit/(sales-3-4-5）
    sum(COALESCE(t.sales_value, 0)) as sales_value,
	sales_month,
    'dw_transaction_detail_report' as data_resource, 
  	SYSDATE() as etl_time,
	 STR_TO_DATE(CONCAT(sales_month ,'01') , '%Y%m%d') as report_date
	
FROM  t
            LEFT JOIN fine_dw.dw_cs_relationship_info cs
            on t.proj_name = cs.proj_name
						and SUBSTR(sales_month,1,4) = cs.s_year
-- 						where sales_month = '202406'
-- 						and t.proj_name = '中升'
		GROUP BY     
    	cs.team_owner,
		cs.team_owner_id,
		cs.sales_person,
		cs.sales_person_id,
	-- 	t.channel,
		t.proj_name,
    	t.proj_name_en,
		sales_month