
/*
目标表：ods_rebate_import_query
*/

SELECT  j.RebateNo AS rebateno           --  '返利编号'
       ,j.ServiceCode    AS vendor_code  -- '服务商编码',
       ,j.ServiceName   AS vendor_name   -- '服务商名称'
       ,j.RebateType   AS  rebatetype   -- '返利类型'
       ,j.RebateState   AS rebatestate -- '返利状态'
       ,j.FrozenState  AS  frozenstate -- '冻结状态'
       ,LockState AS  lockstate  -- '锁定状态'
       ,j.Entry  AS entry -- '条目'
       ,j.RebateMoney AS rebatemoney -- '返利金额'
       ,j.DecMoneySum AS decmoneysum -- '抵扣金额'
       ,j.RemainMoney AS remainomney -- '剩余金额'
       ,j.FrozenMoney AS frozenmoney -- '冻结金额'
       ,j.RebateDate AS rebatedate -- '返利日期'
       ,OpeEmpName AS opeempname -- '导入人姓名'
       ,OpeDate AS opedate -- '导入日期'
       ,(j.RemainMoney-j.FrozenMoney) as   available_amount  -- '可用金额' 
      ,FORMAT(j.RebateDate ,'yyyyMM')  as sales_month
      ,RebateChannel AS rebatechannel -- '渠道'
      ,RebateItem AS rebateitem -- '项目'
      ,RebatePeriod AS rebateperiod -- '返利所属期'
      ,RebateClassification AS rebateclassification -- '分类'
       ,'集采系统'       as data_resource
       ,GETDATE()       as etl_time	--ETL更新时间 
  FROM jcrebatem j 
  WHERE j.RebateOpeType='导入返利'