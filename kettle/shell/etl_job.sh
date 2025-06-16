#!/bin/bash  

# 获取当前日期的日部分  
current_day=$(date +%d)  

# 检查日期  
if [ "$current_day" -eq 1 ]; then  
    /usr/local/kettle/shell/main_job_4.sh
else  
    /usr/local/kettle/shell/main_job_2.sh
fi