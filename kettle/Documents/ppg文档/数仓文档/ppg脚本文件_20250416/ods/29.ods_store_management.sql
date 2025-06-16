/*
目标表：fine_ods.ods_store_management
来源表：
binfoshop 
tbinfomainfactory 
sysoperator

更新方式：全量更新
*/
-- 6.门店管理
  SELECT  t.ShopCode AS  shop_code --  '门店编码'
         , t.ShopName AS  shop_name -- '门店名称'
         , t.OrcalCode AS ship_to_code -- 'Shipto'
         , t.LinkMan  as linkman        -- '联系人'
         , t.LinkPhone  as linkphone    -- '联系电话'
         , t.Address  as address         -- '详细地址'
         , t.DisableFlag  as disableflag -- '状态'
         , t.CreateEmpName  as createempname -- '添加人'
         , t.CreateDate  as createdate -- '添加时间'
         , t.EditEmpName  as  editempname -- '编辑人'
         ,t.EditDate  as editdate -- '最后编辑时间'
         , t1.MainCode  as maincode --  '所属主机厂编码'
         ,t1.MainName  as mainname -- '所属主机厂名称'
         , s1.EmpName  as regional_manager -- '大区经理'
         , s2.EmpName  as area_manager  -- '区域经理'
         ,s3.EmpName  as  sales_person -- '销售'
         ,t.SID       as sid   -- '帆软ID'  
        ,'集采系统'       as data_resource
        ,GETDATE()       as etl_time	--ETL更新时间 
FROM tbinfoshop t
 INNER JOIN tbinfomainfactory t1 ON t.MainID=t1.ID
 LEFT JOIN sysoperator s1 ON t.RegionManagerID=s1.ID
 LEFT JOIN sysoperator s2 ON t.AreaManagerID=s2.ID
 LEFT JOIN sysoperator s3 ON t.SalesManID=s3.ID