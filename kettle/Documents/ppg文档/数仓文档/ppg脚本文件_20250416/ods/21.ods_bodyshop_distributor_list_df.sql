/*
目标表：fine_ods.ods_bodyshop_distributor_list_df
来源表：
tbinfoshop 
tbinfoclient
tbinfoshoprelservice
tbinfomainfactory

更新方式：全量更新 
*/

SELECT   --  a.ID        as bodyshop_code  --- 门店id
         a.ShopCode      as bodyshop_code  -- 门店编码
        ,a.ShopName      as bodyshop_name -- 门店名称
        ,d.MainCode      as customer_code -- 所属主机厂编码
        ,d.MainName      as customer_name -- 所属主机厂名称
        ,a.OrcalCode     as ship_to_code 
        -- ,b.ID         as 服务商id 
        ,b.ClientCode    as vendor_code -- 服务商编码
        ,b.ClientName    as vendor_name -- 服务商名称
        ,b.WarehouseCode as warehouse_code -- 仓位
        ,c.DefaultFlag   as is_default -- 是否默认
        ,'集采系统'       as data_resource
        ,GETDATE()       as etl_time	--ETL更新时间 
FROM  tbinfoshop a
     ,tbinfoclient b
     ,tbinfoshoprelservice c
     ,tbinfomainfactory d
 WHERE a.ID=c.ShopID 
       AND b.ID=c.ServiceID 
       AND a.MainID=d.ID
-- ORDER BY a.ShopCode