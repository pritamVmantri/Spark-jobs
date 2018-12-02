#!/bin/bash 

############################################################################
########### Script Name:             #######################################
########### Author :                 #######################################
########### Version :                #######################################
########### Usage  :                 #######################################
############################################################################

echo "Script $0 start execution for duplicate deletion"

spark-submit --class project_30_11 \
	--conf "javax.jdo.option.ConnectionDriverName"="com.mysql.jdbc.Driver" \
	--conf "javax.jdo.option.ConnectionURL"="jdbc:mysql://localhost:3306/metastore_db?createDatabaseIfNotExist=true" \
	--conf "javax.jdo.option.ConnectionUserName"="hadoop" \
	--conf "javax.jdo.option.ConnectionPassword"="hadoop" \
	--conf "hive.exec.scratchdir"="/tmp/hive/${USER}" \
	--conf "spark.sql.warehouse.dir"="/home/hadoop/spark_warehouse" \
	--master local \
	--jars /home/hadoop/mysql-connector-java-5.1.34.jar \
	/home/hadoop/project_0112_2.11-0.1.jar

echo "Script executed successfully.."

#	--deploy-mode cluster \
#	--executor-memory 1G \
#	--total-executor-cores 5 \
