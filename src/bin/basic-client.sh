#! /bin/bash
PWD_PATH=`dirname $0`/..;
source $PWD_PATH/conf/env.sh

main_class="io.masterkun.nfsonhdfs.util.cli.NfsClientMain"
export SERVICE_OPTS="$SERVICE_OPTS -Xms128M -Xmx128M -Xss256K -client"
export SERVICE_OPTS="$SERVICE_OPTS -Dlog4j.configurationFile=$PWD_PATH/conf/log4j2-client.xml"

export LOG_STDOUT=ACCEPT
export HOSTNAME=`hostname`
export HADOOP_USER_NAME=hdfs

if [ -z $HADOOP_CONF_DIR ] ; then
    if [ $HADOOP_HOME ] && [ -d $HADOOP_HOME/etc/hive ] ; then
        export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hive
    else
        export HADOOP_CONF_DIR=/etc/hive/conf
    fi
fi

class_path=$PWD_PATH/conf:$HADOOP_CONF_DIR:$PWD_PATH/libs/*


java_cmd='';
if [ -z "$CUSTOMED_JAVA_CMD" ]; then
    java_cmd='java';
else
    java_cmd="$CUSTOMED_JAVA_CMD";
fi

$java_cmd $SERVICE_OPTS -cp $class_path $main_class "$@"
