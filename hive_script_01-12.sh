#!/bin/bash

exec > `basename ${0} .sh`.log 2>&1

DUP_FILE_PATH="/home/hadoop"
CURR_DATE_TIME=`date '+%Y-%m-%y_%H%M%S%N'`
DUP_FILE_NAME=duplicate_records_${CURR_DATE_TIME}.csv

## Creating file for duplicate records 
hive -v -e "select CONCAT_WS(',', ccn, state, provider_name) from (select tab.*, row_number() over (partition by ccn order by ccn) rnk from home_health_care.HHC_SOCRATA_PRVDR tab ) tab2 where tab2.rnk > 1;" > ${DUP_FILE_PATH}/${DUP_FILE_NAME}

## Removing headings from file 
sed '1,2d' ${DUP_FILE_PATH}/${DUP_FILE_NAME} ${DUP_FILE_PATH}/${DUP_FILE_NAME}.tmp
mv ${DUP_FILE_PATH}/${DUP_FILE_NAME}.tmp ${DUP_FILE_PATH}/${DUP_FILE_NAME}

## Inserting clean data into base table from staging after removing duplicates 
hive -v -e "insert into home_health_care.new_table select ccn, state, provider_name from (select tab.*, row_number() over (partition by ccn order by ccn) rnk from home_health_care.HHC_SOCRATA_PRVDR tab ) tab2 where tab2.rnk = 1"

## Move duplicate data file to Hadoop path
#hadoop fs -put ${DUP_FILE_PATH}/${DUP_FILE_NAME} /user/hadoop

echo "script completed"
