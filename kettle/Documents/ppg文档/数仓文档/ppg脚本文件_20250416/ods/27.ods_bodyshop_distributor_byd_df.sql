/*
目标表：fine_ods.ods_bodyshop_distributor_byd_df
来源表：
tbinfoclient 
tbinfoclientrelmainfactory
tbinfomainfactory
更新方式：全量更新 
*/


-- 2 ods_bodyshop_distributor_byd_df
SELECT a.ClientCode  as vendor_code-- 服务商编码
      ,a.ClientName  as vendor_name -- 服务商名称
      ,c.MainCode    as customer_code -- 主机厂编码
      ,c.MainName    as customer_name -- 主机厂名称
      ,b.OracleCode  as ship_to_code -- Shipto
    ,'集采系统'       as data_resource
        ,GETDATE()       as etl_time	--ETL更新时间 
 FROM tbinfoclient a
     ,tbinfoclientrelmainfactory b
     ,tbinfomainfactory c
WHERE a.ID=b.ClientID 
     AND b.MainfactoryID=c.ID 
-- ORDER BY a.ClientCode