#!/bin/bash  

# ��ȡ��ǰ���ڵ��ղ���  
current_day=$(date +%d)  

# �������  
if [ "$current_day" -eq 1 ]; then  
    /usr/local/kettle/shell/main_job_4.sh
else  
    /usr/local/kettle/shell/main_job_2.sh
fi