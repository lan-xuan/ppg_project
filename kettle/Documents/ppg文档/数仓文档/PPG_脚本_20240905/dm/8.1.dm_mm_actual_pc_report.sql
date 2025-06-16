-- -- dm_mm_actual_pc_report
-- TRUNCATE TABLE fine_dm.dm_mm_actual_pc_report;
-- insert into fine_dm.dm_mm_actual_pc_report 
-- (
-- team_owner,
--  team_owner_id,
--  sales_person,
--  sales_person_id,
--  proj_name,
--  proj_name_en,
--  stock,
--  pc,
--  pc_rate,
--  service_fee2,
--  service_rate,
--  reward_rate,
--  rebate_rate,
--  adjusted_fee,
--  actual_pc,
--  actual_pc_rate,
--  commision_fee,
--  ebit,
--  ebit_rate,
--  sales_value,
--  sales_month,
--  data_resource,
--  etl_time,
--  report_date
--  )
with price_pc as(
SELECT sum(d_pc) as pc,channel,t.proj_name,t.proj_name_en,sales_month 
			FROM fine_dw.dw_price_increase t
			WHERE 1=1
			and t.channel = 'MM'
			-- and t.sales_month  = '202401'
			GROUP BY t.channel,t.proj_name,t.proj_name_en,sales_month
)-- SELECT count(1) from price_pc;
,dw_cb_detail as (
SELECT 
			sum(sales_value) as sales_value,
			sum(cb.sales_qty) as sales_qty,
			sum(COALESCE(cb.service_fee2 * sales_qty,0)) as service_fee2,
			sum(COALESCE(cb.rebate_rate * sales_value,0)) as rebate_rate,
			sum(COALESCE(cb.commision_fee_rate * sales_value,0)) as commision_fee,
			sum(COALESCE(cb.service_rate * sales_value,0)) as service_rate,
			case when proj_name = '上汽大众' then sum(COALESCE((bs_price * sales_qty) * reward_rate,0))
            else sum(COALESCE((distributor_price * sales_qty) * reward_rate,0)) end as  reward_rate, -- 服务商备货价格*qty*费率（其中上汽大众按门店价*qty*费率） 主机厂项目优质服务季度奖励金
			case when proj_name = '上汽大众' then sum(COALESCE((bs_price * sales_qty) ,0))
            else sum(COALESCE((distributor_price * sales_qty),0)) end as  stock,
			cb.channel,
			cb.proj_name,
			cb.proj_name_en,
			cb.sales_month 
			FROM fine_dw.dw_cb_detail cb
			WHERE 1=1
			and cb.channel = 'MM'
			-- and cb.sales_month  = '202401'
			GROUP BY 
			-- COALESCE(cb.service_fee2,0),
			-- COALESCE(cb.rebate_rate,0),
			-- COALESCE(cb.commision_fee_rate,0),
			-- COALESCE(cb.service_rate,0),
			-- COALESCE(cb.reward_rate,0),
			-- COALESCE(cb.bs_price,0),
			-- COALESCE(cb.distributor_price,0),
			cb.channel,
			cb.proj_name,
			cb.proj_name_en,
			cb.sales_month 
)  
,cb_detail as(
    SELECT
		t.channel,
		t.proj_name,
        t.proj_name_en,
--         price_pc.pc,
--         price_pc.pc/NULLIF(t.sales_value, 0)  as pc_rate,
        t.service_fee2, -- 费率*qty
        t.service_rate,
        t.reward_rate, -- 服务商备货价格*qty*费率（其中上汽大众按门店价*qty*费率） 主机厂项目优质服务季度奖励金
		t.stock,
        t.rebate_rate,
        COALESCE(adj.adjusted_fee,0) as adjusted_fee, -- proj_name，所属月份匹配获得
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
			and adj.adjusted_month = t.sales_month
    where 1=1
		
) --  SELECT * from cb_detail;
,aaa as(
SELECT 
		t.channel,
		t.proj_name,
   		t.proj_name_en,
		t.sales_month,
		sum(service_fee2) as service_fee2,
		sum(service_rate) as service_rate,
		sum(stock) stock , -- 备货
		sum(reward_rate) as reward_rate, -- 季度奖金
		sum(rebate_rate) as rebate_rate, -- 项目返利
		sum(adjusted_fee) as adjusted_fee, -- 调整金额
		sum(commision_fee) as commision_fee,
		sum(sales_value) as sales_value
FROM cb_detail t
GROUP BY 
		t.channel,
		t.proj_name,
    t.proj_name_en,
		t.sales_month

)   
, t as (
SELECT 
		t.channel,
		t.proj_name,
    	t.proj_name_en,
		t.sales_month,	
		price_pc.pc,
    	price_pc.pc/NULLIF(t.sales_value - reward_rate - rebate_rate - adjusted_fee, 0)  as pc_rate,
		t.service_fee2,
		t.service_rate,
		t.stock,
		t.reward_rate,
		t.rebate_rate,
		t.adjusted_fee,
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
	sum(COALESCE(t.stock , 0)) stock,
    sum(COALESCE(t.pc , 0)) pc,
    sum(COALESCE(pc_rate , 0)) pc_rate, 
    sum(COALESCE(t.service_fee2 , 0)) service_fee2, -- 1
    sum(COALESCE(t.service_rate , 0)) service_rate, -- 2
    sum(COALESCE(t.reward_rate , 0)) reward_rate,  -- 3
    sum(COALESCE(t.rebate_rate , 0)) rebate_rate,  -- 4
    sum(COALESCE(t.adjusted_fee , 0)) adjusted_fee,  -- 5

    sum((COALESCE(t.pc,0) -  COALESCE(t.service_fee2,0) -  COALESCE(t.service_rate,0) -  COALESCE(t.reward_rate,0) -  COALESCE(t.rebate_rate,0) -  COALESCE(t.adjusted_fee,0))) as actual_pc, -- 公式=pc-1-2-3-4-5
	-- MM: actual pc%= actual pc/(项目结算销售额-季度奖励金-项目返利-调整金额） 
    sum((COALESCE(t.pc,0) -  COALESCE(t.service_fee2,0) -  COALESCE(t.service_rate,0) -  COALESCE(t.reward_rate,0) -  COALESCE(t.rebate_rate,0) -  COALESCE(t.adjusted_fee,0)) /NULLIF( COALESCE(sales_value,0)  -  COALESCE(t.reward_rate,0) -  COALESCE(t.rebate_rate,0) -  COALESCE(t.adjusted_fee,0) ,0)) as actual_pc_rate, -- 公式=actual_pc/(sales-3-4-5）
    sum(COALESCE(t.commision_fee , 0))   as commision_fee, -- 6
    -- sum(COALESCE(pc_rate,0) - COALESCE(t.commision_fee,0)) ebit,  -- 公式=actual_pc-6
	sum((COALESCE(t.pc,0) -  COALESCE(t.service_fee2,0) -  COALESCE(t.service_rate,0) -  COALESCE(t.reward_rate,0) -  COALESCE(t.rebate_rate,0) -  COALESCE(t.adjusted_fee,0)) - COALESCE(t.commision_fee,0)) ebit,
	-- eop%=eop/(项目结算销售额-季度奖励金-项目返利-调整金额）
    sum((COALESCE(t.pc,0) -  COALESCE(t.service_fee2,0) -  COALESCE(t.service_rate,0) -  COALESCE(t.reward_rate,0) -  COALESCE(t.rebate_rate,0) -  COALESCE(t.adjusted_fee,0)) - COALESCE(t.commision_fee,0))/NULLIF( sum(COALESCE(sales_value,0)  -  COALESCE(t.reward_rate,0) -  COALESCE(t.rebate_rate,0) -  COALESCE(t.adjusted_fee,0)) ,0) as ebit_rate, -- 公式=ebit/(sales-3-4-5）
    sum(COALESCE(t.sales_value , 0)) as sales_value,
	sales_month,
    'dw_transaction_detail_report' as data_resource, 
	SYSDATE() as etl_time,
	STR_TO_DATE(CONCAT(sales_month ,'01') , '%Y%m%d') as report_date
	
FROM  t
            LEFT JOIN fine_dw.dw_cs_relationship_info cs
            on t.proj_name = cs.proj_name
						and SUBSTR(sales_month,1,4) = cs.s_year

		GROUP BY     
    	cs.team_owner,
		cs.team_owner_id,
		cs.sales_person,
		cs.sales_person_id,
	-- 	t.channel,
		t.proj_name,
    	t.proj_name_en,
		sales_month