#!/bin/bash
################################################################################
# Program      : struct_streaming_job.sh.sh
# Date Created : 10/11/2018
# Description  :This Script is Trigger Streaming Job 24/7
#
# Modification history:
#
# Date         Author               Description
# ===========  ===================  ============================================
# 10/11/2018   Anand Ayyasamy 	    Creation
################################################################################

# Log
APP_NAME="Struct_Streaming"
UNDERSCORE="_"
TODAY_DATE=`date +"%Y-%m-%d"`
LOG_FILE="/data/00/device/app_logs/$APP_NAME$UNDERSCORE$TODAY_DATE.log"
touch $LOG_FILE
chmod 777 $LOG_FILE

# lib

LIB_PATH=/data/00/common_lib

DEP_LIB=$LIB_PATH/kafka-clients-0.10.0.1.jar,$LIB_PATH/spark-streaming-kafka-0-10_2.11-2.2.0.jar,$LIB_PATH/spark-sql-kafka-0-10_2.11-2.2.0.jar,$LIB_PATH/json-20180130.jar

# App

APP_PATH=/data/00/device/app_code

# Auth
principal=xxx@HADOOP.COM
keyTab=xxx.keytab
kinit $principal -k -t $keyTab

cd $APP_PATH

# Env Set

export KAFKA_OPTS="-Djava.security.auth.login.config=jaas.conf"
export SPARK_MAJOR_VERSION=2
export SPARK_KAFKA_VERSION=0.10

# Submit the job

spark-submit --name $APP_NAME --jars structured-streaming.properties,/security/tls/truststore/truststore.jks,$DEP_LIB \
--master yarn  --deploy-mode cluster \
--driver-memory 2G --executor-memory 4G \
--conf spark.driver.extraJavaOptions=" -XX:MaxPermSize=4G "  \
--conf spark.executor.extraJavaOptions=" -XX:MaxPermSize=4G "  \
--conf spark.hadoop.fs.hdfs.impl.disable.cache="true" \
--conf spark.yarn.max.executor.failures="8" \
--conf spark.yarn.executor.memoryOverhead="2048" \
--files "$keyTab,$APP_PATH/jaas.conf" \
--conf "spark.executor.extraJavaOptions=-Dsun.security.krb5.debug=true" \
--conf "spark.driver.extraJavaOptions=-Dsun.security.krb5.debug=true" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=/tmp/log4j-spark.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=/tmp/log4j-spark.properties" \
--driver-java-options "-Dlog4j.configuration=/tmp/log4j-spark.properties" \
--conf spark.driver.extraJavaOptions="-Djava.security.auth.login.config=jaas.conf"  \
--conf spark.executor.extraJavaOptions="-Djava.security.auth.login.config=jaas.conf" \
--class com.org.data.struct.stream.Structured_Streaming \
$APP_PATH/struct-Streaming-2.0-SNAPSHOT.jar 2>&1 | tee -a $LOG_FILE


# You are Reached !
