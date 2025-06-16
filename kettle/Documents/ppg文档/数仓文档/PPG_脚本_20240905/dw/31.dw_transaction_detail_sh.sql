-- 31.dw_transaction_detail_sh

SELECT 
customer_code
,customer_name
,bill_no
,send_no
,create_date
,bill_state
,shop_code
,shop_name
,shoporcal_code
,vendor_code
,vendor_name
,warehouse_code
,order_no
,mainpart_code
,mainpart_name
,mainpack_unit
,main_packspec
,mainpart_price
,ppgmainpart_price
,mainpart_num
,convert_num
,bom_flag
,item_code
,ppgpart_name
,ppgpack_unit
,ppgpack_spec
,ship_to_code
,ppgorg_code
,product_properties
,original_position
,change_flag
,oldppgpar_tcode
,duizhang_mode
,servicefee_price
,ppgpart_price
,ppgpart_num
,maincanback_num
,mainback_num
,mainchange_num
,ppgcanback_num
,ppgback_num
,is_backed
,remark
,mainaccount_num
,mainaccount_state
,mainaccount_remark
,sales_qty
,mainbackcanaccount_num
,servicebackcanaccount_num
,serviceaccount_state
,notices_tate
,dis_date
,email_date
,send_date
,send_state
,upload_state
,upload_date
,lastupload_date
,close_date
,closeemp_name
,orcalpo_no
,release_no
,mainaccount_date
,mainaccountlast_date
,serviceaccount_date
,serviceaccountlast_date
,etl_time
,data_resource
,sales_month
,sales_month_remark
from 
(select a.* 
      ,  DATE_FORMAT(mainaccountlast_date, '%Y%m')  AS sales_month  -- 月份_来自主机厂最后对账日期
       ,REGEXP_SUBSTR(mainaccount_remark, '[12][0-9]{3}(0[1-9]|1[0-2])') AS sales_month_remark  -- 月份_来自备注字段
FROM fine_ods.ods_transaction_detail_sh a
 where DATE_FORMAT(mainaccountlast_date, '%Y%m')   = ${mysql_yesterday_d_month}
 or mainaccountlast_date is null 
) b

