-- 7. 销售跟踪表-主机厂+门店+产品+服务商（包含门店金额）
--   ods_sales_tracking_table_sh
select t1.MainName      AS  mainname   -- '主机厂名称'
      ,t1.MainCode      AS  maincode   -- '主机厂编码'
      ,t1.ShopCode      AS  shopcode   -- '门店编码'
      ,t1.ShopName      AS  shopname   -- '门店名称'
      ,t1.ShopOrcalCode AS  shoporcalcode -- 'ShipTo'
      ,t1.ServiceCode   AS  servicecode   -- '服务商编码'
      ,t1.ServiceName   AS  servicename   --  '服务商名称'
      ,t1.WarehouseCode AS  warehousecode -- '仓位'
      ,t1.PPGPartCode   AS  ppgpartcode   -- '产品编码'
      ,t1.SaleYear     as   saleyear -- '销售年份'  -- 增量字段
      ,t1.TotalNumShop  AS  totalnumshop -- '年度合计门店金额'
      ,t1.TotalNumMain  AS  totalnummain -- '年度合计主机厂金额'
      ,t1.Month1AmountShop AS month1amountshop -- '一月门店金额'
      ,Month1AmountMain AS  month1amountmain -- '一月主机厂金额'
      ,Month2AmountShop AS  month2amountshop  -- '二月门店金额'
      ,Month2AmountMain AS  month2amountmain  -- '二月主机厂金额'
      ,Month3AmountShop AS  month3amountshop  --'三月门店金额'
      ,Month3AmountMain AS  month3amountmain  -- '三月主机厂金额'
      ,Month4AmountShop AS  month4amountshop  -- '四月门店金额'
      ,Month4AmountMain AS  month4amountmain  -- '四月主机厂金额'
      ,Month5AmountShop AS  month5amountshop  -- '五月门店金额'
      ,Month5AmountMain AS  month5amountmain  -- '五月主机厂金额'
      ,Month6AmountShop AS  month6amountshop  -- '六月门店金额'
      ,Month6AmountMain AS  Month6AmountMain  -- '六月主机厂金额'
      ,Month7AmountShop AS  Month7AmountShop  -- '七月门店金额'
      ,Month7AmountMain AS  Month7AmountMain  -- '七月主机厂金额'
      ,Month8AmountShop AS  Month8AmountShop  -- '八月门店金额'
      ,Month8AmountMain AS  Month8AmountMain  -- '八月主机厂金额'
      ,Month9AmountShop AS  Month9AmountShop  -- '九月门店金额'
      ,Month9AmountMain AS  Month9AmountMain  -- '九月主机厂金额'
      ,Month10AmountShop AS Month10AmountShop -- '十月门店金额'
      ,Month10AmountMain AS Month10AmountMain -- '十月主机厂金额'
      ,Month11AmountShop AS Month11AmountShop -- '十一月门店金额'
      ,Month11AmountMain AS Month11AmountMain -- '十一月主机厂金额'
      ,Month12AmountShop AS Month12AmountShop -- '十二月门店金额'
      ,Month12AmountMain AS Month12AmountMain -- '十二月主机厂金额'
      ,t3.MainType AS       maintype -- '主机厂类型'
      ,t2.SaleArea AS       salearea  -- '所属区域'
      ,t2.ClientProvince AS clientprovince -- '省份'
      ,t2.ClientCity AS clientcity -- '城市'
      ,t2.RegionManager AS regionmanager -- '大区经理'
      ,t2.AreaManager AS areamanager -- '区域经理'
      ,t2.SalesMan    AS salesman -- '销售' 
      ,'集采系统'        as data_resource  -- 数据来源
      ,GETDATE()         as etl_time	--ETL更新时间 
      from (
select MainID
      ,ShopID
      ,MainName
      ,MainCode
      ,ShopCode
      ,ShopName
      ,ShopOrcalCode
      ,ServiceID
      ,ServiceCode
      ,ServiceName
      ,WarehouseCode
      ,PPGPartCode
      ,SaleYear 
      ,CAST(sum(PartShopAmountTax) AS DECIMAL(24, 2)) TotalNumShop
      ,CAST(sum(PartMainAmountTax) AS DECIMAL(24, 2)) TotalNumMain
      ,CAST(sum(case SaleMonth when 1 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month1AmountShop
      ,CAST(sum(case SaleMonth when 1 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month1AmountMain
      ,CAST(sum(case SaleMonth when 2 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month2AmountShop
      ,CAST(sum(case SaleMonth when 2 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month2AmountMain
      ,CAST(sum(case SaleMonth when 3 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month3AmountShop
      ,CAST(sum(case SaleMonth when 3 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month3AmountMain
      ,CAST(sum(case SaleMonth when 4 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month4AmountShop
      ,CAST(sum(case SaleMonth when 4 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month4AmountMain
      ,CAST(sum(case SaleMonth when 5 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month5AmountShop
      ,CAST(sum(case SaleMonth when 5 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month5AmountMain
      ,CAST(sum(case SaleMonth when 6 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month6AmountShop
      ,CAST(sum(case SaleMonth when 6 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month6AmountMain
      ,CAST(sum(case SaleMonth when 7 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month7AmountShop
      ,CAST(sum(case SaleMonth when 7 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month7AmountMain
      ,CAST(sum(case SaleMonth when 8 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month8AmountShop
      ,CAST(sum(case SaleMonth when 8 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month8AmountMain
      ,CAST(sum(case SaleMonth when 9 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month9AmountShop
      ,CAST(sum(case SaleMonth when 9 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month9AmountMain
      ,CAST(sum(case SaleMonth when 10 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month10AmountShop
      ,CAST(sum(case SaleMonth when 10 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month10AmountMain
      ,CAST(sum(case SaleMonth when 11 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month11AmountShop
      ,CAST(sum(case SaleMonth when 11 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month11AmountMain
      ,CAST(sum(case SaleMonth when 12 then (PartShopAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month12AmountShop
      ,CAST(sum(case SaleMonth when 12 then (PartMainAmountTax) else 0 end ) AS DECIMAL(24, 2)) Month12AmountMain
from jcsaletrackimportd j 
where j.DataState='正式' 
and SaleYear >='2022'   -- 抽取22年之后的数据
    -- and SaleYear = ${sqlserver_yesterday_d_year} 
group by 
      MainID
     ,ShopID
     ,MainName
     ,MainCode
     ,ShopCode
     ,ShopName
     ,ShopOrcalCode
     ,ServiceID
     ,ServiceCode
     ,ServiceName
     ,WarehouseCode
     ,PPGPartCode
     ,SaleYear
     ) t1 
inner join tbinfoclient t2 
on t1.ServiceID=t2.ID
inner join tbinfomainfactory t3
on t1.MainID=t3.ID


