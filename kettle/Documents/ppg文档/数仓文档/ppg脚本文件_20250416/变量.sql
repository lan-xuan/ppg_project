参数


文件路径
1.file_path  /usr/local/kettle/   excel文件路径
2.昨天
${mysql_yesterday}
DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY),'%Y%m%d') mysql_昨天
3.昨天对应的当月 
${mysql_yesterday_d_month}
 DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY),'%Y%m') mysql_昨天对应的当月
4.昨天对应的当年 
${mysql_yesterday_d_year}
DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY),'%Y')  mysql_昨天对应的当年
5.sqlserver_yesterday_d_month
FORMAT(DATEADD(DAY, -1, GETDATE() )   ,'yyyyMM')  sqlserver_昨天对应的当月
6.昨天对应的上月 
${mysql_yesterday_l_month}
DATE_FORMAT( DATE_ADD(DATE_ADD(CURDATE(), INTERVAL -1 DAY) , INTERVAL -1 MONTH ), '%Y%m' ) mysql_昨天对应的上月
7.mysql_昨天对应的当年的年末 mysql_yesterday_d_year_end
date_format(concat(year(DATE_ADD(CURDATE(), INTERVAL -1 DAY)),'1231'),'%Y%m%d')
8.昨天对应的去年
${mysql_yesterday_l_year}
DATE_FORMAT(DATE_ADD(DATE_ADD(CURDATE(), INTERVAL -1 DAY), INTERVAL -1 YEAR),'%Y') 
9.mysql_昨天对应上月的同期月
${mysql_yesterday_l_month_l_year}
DATE_FORMAT(DATE_ADD(DATE_ADD(DATE_ADD(CURDATE(), INTERVAL -1 DAY),INTERVAL -1 Month), INTERVAL -1 YEAR),'%Y%m' ) 
10.sqlserver_yesterday_d_month
FORMAT(DATEADD(DAY, -1, GETDATE() )   ,'yyyyMM')  sqlserver_昨天对应的当年
