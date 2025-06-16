#!/bin/bash
export JAVA_HOME=/usr/local/java/jdk1.8.0_401
export PATH=$JAVA_HOME/bin:$PATH 
export CLASSPATH=.:$JAVA_HOME/lib:$JAVA_HOME/jre/lib

DATE_N=`date "+%Y-%m-%d"`

/usr/local/kettle/data-integration/kitchen.sh   -file /usr/local/kettle/ETL/main_job_2.kjb -level Detailed -logfile  /usr/local/kettle/data-integration/logs/main_job/main_job_$DATE_N.log >/dev/null 2>&1
