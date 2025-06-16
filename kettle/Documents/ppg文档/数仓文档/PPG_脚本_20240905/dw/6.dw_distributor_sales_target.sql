
/*
目标表：fine_dw.dw_distributor_sales_target
来源表：
fine_ods.ods_distributor_sales_target_df

fine_dw.dw_customer_master_list
更新方式：增量更新
更新粒度：年
更新字段：target_year

*/

-- 相关SQL
with A AS(
SELECT 
     customer_name,   -- 经销商名
	   customer_code,
	   district,
       gdsxq_1,         -- 高档水性漆第一季度指标
       gdyxq_1,         -- 高档油性漆第一季度指标
			 quickline_1,     -- 中档水性 QUICKLINE
       emaxx_1,         -- 中档油性 EMAXX
			 belco_1,         -- 中档油性 BELCO PLUS
       yzh_1,           -- 原子灰
       yhp_1,           -- 易耗品
			 qsx_1,           -- AUTOMOTIVE WB全水性漆
			 lqhg_1,           -- 凌趣惠管站软件 
			 total_1,          -- 第一季度总指标		 
       gdsxq_2,         
       gdyxq_2,         
			 quickline_2,     
       emaxx_2,        
			 belco_2,         
       yzh_2,           
       yhp_2,          
			 qsx_2,           
			 lqhg_2,   
       total_2,							 		 
       gdsxq_3,        
       gdyxq_3,         
			 quickline_3,     
       emaxx_3,         
			 belco_3,        
       yzh_3,           
       yhp_3,           
			 qsx_3,           
			 lqhg_3,
			 total_3,														
       gdsxq_4,         
       gdyxq_4,         
			 quickline_4,     
       emaxx_4,         
			 belco_4,         
       yzh_4,           
       yhp_4,           
			 qsx_4,          
			 lqhg_4,
			 total_4,
			 target_year
 FROM fine_ods.ods_distributor_sales_target_df

 ),
 B as(
 SELECT  
				A.district,
				A.customer_code,
				A.customer_name,
				cs.team_owner,
				cs.team_owner_id,
				cs.sales_person,
				cs.sales_person_id,
         		A.total_1 AS Q1,
				 A.total_2 AS Q2,
				 A.total_3 AS Q3,
				 A.total_4 AS Q4,
				 A.total_1+A.total_2+A.total_3+A.total_4  AS total_ze,
         A.gdsxq_1,         
         A.gdyxq_1,         
			   A.quickline_1,   
         A.emaxx_1,        
			   A.belco_1,      
         A.yzh_1,           
         A.yhp_1,           
			   A.qsx_1,         
			   A.lqhg_1,          
			   A.total_1,         
         A.gdsxq_2,         
         A.gdyxq_2,         
			   A.quickline_2,     
         A.emaxx_2,        
			   A.belco_2,         
         A.yzh_2,           
         A.yhp_2,          
			   A.qsx_2,           
			   A.lqhg_2,   
         A.total_2,							 
         A.gdsxq_3,        
         A.gdyxq_3,         
		  	 A.quickline_3,     
         A.emaxx_3,         
		  	 A.belco_3,        
         A.yzh_3,           
         A.yhp_3,           
		  	 A.qsx_3,           
			   A.lqhg_3,
		  	 A.total_3,											
         A.gdsxq_4,         
         A.gdyxq_4,         
		   	 A.quickline_4,     
         A.emaxx_4,         
		  	 A.belco_4,         
         A.yzh_4,           
         A.yhp_4,           
		  	 A.qsx_4,          
	  		 A.lqhg_4,
	  		 A.total_4,
         A.gdsxq_1+	A.gdsxq_2+A.gdsxq_3+A.gdsxq_4 AS full_year_gdsxq,
				 A.gdyxq_1+ A.gdyxq_2+A.gdyxq_3+A.gdyxq_4 as full_year_gdyxq,
				 A.quickline_1+A.quickline_2+A.quickline_3+A.quickline_4  AS full_year_quickline,
				 A.emaxx_1+A.emaxx_2+A.emaxx_3+A.emaxx_4  AS full_year_emaxx,
				 A.belco_1+A.belco_2+A.belco_3+A.belco_4  as full_year_belco,
				 A.yzh_1+ A.yzh_2+ A.yzh_3+ A.yzh_4       as full_year_yzh,
				 A.yhp_1+A.yhp_2+A.yhp_3+ A.yhp_4         AS full_year_yhp,
				 A.qsx_1+A.qsx_2+A.qsx_3+A.qsx_4          as full_year_qsx,
				 A.lqhg_1+A.lqhg_2+A.lqhg_3+A.lqhg_4      as full_year_lqhg,
				 A.total_1+A.total_2+A.total_3+A.total_4  AS full_year_total,
				 A.target_year
 FROM A
--  right join fine_dw.dw_customer_master_list s 
--  on A.customer_name=s.customer_name
--  and channel = 'DISTRIBUTOR'
--   where s.u_customer_name is not null
LEFT JOIN fine_dw.dw_cs_relationship_info cs
on A.customer_code = cs.customer_code
and A.district = cs.district
and A.target_year = cs.s_year
 )
	select DISTINCT
 B.* ,
	'fine_ods.ods_distributor_sales_target_df' as data_resource, 
	SYSDATE() as etl_time
 from B
where target_year is not null