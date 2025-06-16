select 
       j.MainCode	                  as customer_code	  -- 主机厂编码(customer code)
      ,j.MainName	                  as customer_name	  -- 主机厂名称(customer name)
      ,j.BillNo	                    as bill_no	        -- 集采单号
      ,j.SendNo	                    as send_no	        -- 发货通知单号
      ,j1.CreateDate	              as create_date	    -- 导单日期
      ,j1.BillState	                as bill_state	    -- 订单状态
      ,j.ShopCode	                  as shop_code	      -- 门店编码
      ,j.ShopName	                  as shop_name	      -- 门店名称
      ,j.ShopOrcalCode	            as shoporcal_code	  -- Shipto
      ,j.ServiceCode	              as vendor_code	    -- 服务商编码(vendor code)
      ,j.ServiceName	              as vendor_name	    -- 服务商名称(vendor name)
      ,j.WarehouseCode	            as warehouse_code	-- 仓位(warehouse)
      ,j.PoNo	                      as order_no	      -- PO单号(order no)
      ,j.MainPartCode	              as mainpart_code	  -- 主机厂产品编码
      ,j.MainPartName	              as mainpart_name	  -- 主机厂产品名称
      ,j.MainPackUnit	              as mainpack_unit	  -- 主机厂包装单位
      ,j.MainPackSpec	              as main_packspec	  -- 主机厂包装规格
      ,j.MainPartPrice	            as mainpart_price	-- 主机厂价格
      ,j.PPGMainPartPrice	          as ppgmainpart_price	-- 门店价格
      ,j.MainPartNum	              as mainpart_num	-- 主机厂数量
      ,j.ConvertNum	                as convert_num	-- 转换值
      ,j.BomFlag	                  as bom_flag	       -- 是否BOM
      ,j.PPGPartCode	              as item_code	      -- PPG产品编码（item code)
      ,j.PPGPartName	              as ppgpart_name	  -- PPG产品名称
      ,j.PPGPackUnit	              as ppgpack_unit	  -- PPG包装单位
      ,j.PPGPackSpec	              as ppgpack_spec	  -- PPG包装规格
      ,j.ServiceMainFactoryShipto	  as ship_to_code	  -- 服务商主机厂Shipto(ship_to_code)
      ,j.PPGOrgCode	                as ppgorg_code	    -- PPG原始编号
      ,j.ProductProperties	        as product_properties	-- 产品属性
      ,j.OriginalPosition	          as original_position	-- 原始仓位
      ,j.ChangeFlag	                as change_flag	      -- 换季产品
      ,j.OldPPGPartCode	            as oldppgpar_tcode	-- 换标签产品
      ,j.DuiZhangMode	              as duizhang_mode	  -- 订单性质
      ,j.ServiceFeePrice	          as servicefee_price	-- 服务费单价
      ,j.PPGPartPrice	              as ppgpart_price	-- 服务商备货价
      ,j.PPGPartNum	                as ppgpart_num	-- PPG数量
      ,j.MainCanBackNum	            as maincanback_num	-- 主机厂可退换数量
      ,j.MainBackNum	              as mainback_num	  -- 主机厂退货数量
      ,j.MainChangeNum	            as mainchange_num	-- 主机厂换货数量
      ,j.PPGCanBackNum	            as ppgcanback_num	-- PPG可退换数量
      ,j.PPGBackNum	                as ppgback_num	-- PPG退货数量
      ,j.IsBacked	                  as is_backed	-- 是否退货
      ,j.Remark	                    as remark	-- 导单备注
      ,j.MainAccountNum	            as mainaccount_num	-- 主机厂对账数量
      ,j.MainAccountState	          as mainaccount_state	-- 主机厂对账状态
      ,j.MainAccountRemark	        as mainaccount_remark	-- 主机厂对账备注
      ,j.PPGAccountNum	            as sales_qty	-- PPG对账数量(qty)
      ,j.ServiceAccountNum	        as serviceaccount_num	-- 服务商对账数量
      ,j.MainBackCanAccountNum	    as mainbackcanaccount_num	-- 主机厂退货可对账数
      ,j.ServiceBackCanAccountNum	  as servicebackcanaccount_num	-- 服务商退货可对账数
      ,j.ServiceAccountState	      as serviceaccount_state	-- 服务商对账状态
      ,j.NoticeState	              as notices_tate	-- 可通知状态
      ,j.DisDate	                  as dis_date	-- 分单日期
      ,j.EmailDate	                as email_date	--邮件日期
      ,j.SendDate	                  as send_date	-- 发货日期
      ,j.SendState	                as send_state	-- 发货状态
      ,j.UploadState	              as upload_state	-- 上传状态
      ,j.UploadDate	                as upload_date	-- 上传发货单日期
      ,j.LastUploadDate	            as lastupload_date	-- 最后上传日期
      ,j.CloseDate	                as close_date	-- 关闭日期
      ,j.CloseEmpName	              as closeemp_name	-- 关闭人
      ,j.OrcalPoNo	                as orcalpo_no	-- OrcalPoNo
      ,j.ReleaseNo	                as release_no	-- ReleaseNo
      ,j.MainAccountDate	          as mainaccount_date	-- 主机厂首次对账日期
      ,j.MainAccountLastDate	      as mainaccountlast_date	-- 主机厂最后对账日期
      ,j.ServiceAccountDate	        as serviceaccount_date	-- 服务商首次对账日期
      ,j.ServiceAccountLastDate	    as serviceaccountlast_date	-- 服务商最后对账日期
      ,'集采系统'                    as data_resource
      ,GETDATE()                     as etl_time	--ETL更新时间 
FROM jcorderbilld j 
JOIN jcorderbillm j1 
ON j.BillID=j1.ID
 -- where FORMAT(j1.MainAccountLastDate  ,'yyyyMM')  >='202401'
where FORMAT(j.MainAccountLastDate  ,'yyyyMM') = ${sqlserver_yesterday_d_month}
or j.MainAccountLastDate is null 