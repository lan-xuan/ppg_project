select 
 invoice_no
,order_no
,purchase_order
,customer_po
,delivery_number
,batch_source
,invoice_type
,complete
,status_code
,sales_date
,due_date
,terms
,salesrep
,invoice_currency_code
,gl_class
,ship_to_customer_code
,ship_to_customer_name
,ship_to_code
,customer_code
,customer_name
,line_no
,item_code
,aero_platform
,brand_name
,customer_item_description
,invoice_line_description
-- 字符转化成数字类型，处理‘-’字符
,case when sales_qty like '% -%' or  sales_qty like '%- %' or sales_qty like '% - %' or sales_qty = '-'
       then null 
      else sales_qty * 1 end  as sales_qty
,case when shipped_qty_kg like '% -%' or  shipped_qty_kg like '%- %' or shipped_qty_kg like '% - %' or shipped_qty_kg = '-'
       then null 
      else shipped_qty_kg * 1 end  as shipped_qty_kg
,case when sales_volume like '% -%' or  sales_volume like '%- %' or sales_volume like '% - %' or sales_volume = '-'
       then null 
      else sales_volume * 1 end  as sales_volume
,case when credit_qty like '% -%' or  credit_qty like '%- %' or credit_qty like '% - %' or credit_qty = '-'
       then null 
      else credit_qty * 1 end  as credit_qty
,case when unit_price like '% -%' or  unit_price like '%- %' or unit_price like '% - %' or unit_price = '-'
       then null 
      else unit_price * 1 end  as unit_price
,case when extended_amount like '% -%' or  extended_amount like '%- %' or extended_amount like '% - %' or extended_amount = '-'
       then null 
      else extended_amount * 1 end  as extended_amount
,case when tax_amount like '% -%' or  tax_amount like '%- %' or tax_amount like '% - %' or tax_amount = '-'
       then null 
      else tax_amount * 1 end  as tax_amount       
,uom
,waybill
,invoice_remark
,local_currency_code
,warehouse_code
,order_date
,actual_ship_date
-- 字符转化成数字类型，处理‘-’字符
,case when sales_value like '% -%' or  sales_value like '%- %' or sales_value like '% - %' or sales_value = '-'
       then null 
      else sales_value * 1 end  as sales_value
,case when local_sales_value like '% -%' or  local_sales_value like '%- %' or local_sales_value like '% - %' or local_sales_value = '-'
       then null 
      else local_sales_value * 1 end  as local_sales_value
,case when local_item_cost like '% -%' or  local_item_cost like '%- %' or local_item_cost like '% - %' or local_item_cost = '-'
       then null 
      else local_item_cost * 1 end  as local_item_cost
,case when blanket_number like '% -%' or  blanket_number like '%- %' or blanket_number like '% - %' or blanket_number = '-'
       then null 
      else blanket_number * 1 end  as blanket_number
,bsa
-- 字符转化成数字类型，处理‘-’字符
,case when local_pc like '% -%' or  local_pc like '%- %' or local_pc like '% - %' or local_pc = '-'
       then null 
      else local_pc * 1 end  as local_pc
,case when pc_pct like '% -%' or  pc_pct like '%- %' or pc_pct like '% - %' or pc_pct = '-'
       then null 
      else pc_pct * 1 end  as pc_pct   
,ppg_sales_class
,order_type
,comments
,update_date
,data_resource
,etl_time
from fine_ods.ods_transaction_detail_report_df_mid
Where update_date  = '${UPDATE_DATE}'

