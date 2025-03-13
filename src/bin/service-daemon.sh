#! /bin/bash
PWD_PATH=$(cd `dirname $0`/..; pwd);

export TOOL_PWD=$PWD_PATH;
cd $PWD_PATH
krbdebug=false
source $PWD_PATH/conf/env.sh

main_class="io.masterkun.nfsonhdfs.NfsServerAppStarter"
args='conf/nfs-server.yaml'
jmx_port=18381
export SERVICE_OPTS="$SERVICE_OPTS -Xms1G -Xmx1G -Xss256K -server"
export SERVICE_OPTS="$SERVICE_OPTS -XX:MaxTenuringThreshold=15"
export SERVICE_OPTS="$SERVICE_OPTS -XX:+HeapDumpOnOutOfMemoryError"
export SERVICE_OPTS="$SERVICE_OPTS -Djava.rmi.server.hostname=$(hostname)"
export SERVICE_OPTS="$SERVICE_OPTS -Dcom.sun.management.jmxremote=true"
export SERVICE_OPTS="$SERVICE_OPTS -Dcom.sun.management.jmxremote.port=$jmx_port"
export SERVICE_OPTS="$SERVICE_OPTS -Dcom.sun.management.jmxremote.ssl=false"
export SERVICE_OPTS="$SERVICE_OPTS -Dcom.sun.management.jmxremote.authenticate=false"

srv_name='nfs-server'
export LOG_DIR=$PWD_PATH/logs/
export LOG_NAME=$srv_name
export LOG_STDOUT=DENY
export HOSTNAME=`hostname`
export LOKI_HOST=$LOKI_HOST
export LOKI_PORT=$LOKI_PORT
export PROJECT_VERSION=$PROJECT_VERSION
export HADOOP_USER_NAME=hdfs
export PROFILE=$PROFILE

mkdir -p $LOG_DIR
export PID_DIR="$PWD_PATH/pids"
mkdir -p $PID_DIR
pid_file="$PID_DIR/$srv_name.pid"

if [ -z $HADOOP_CONF_DIR ] ; then
    if [ $HADOOP_HOME ] && [ -d $HADOOP_HOME/etc/hive ] ; then
        export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hive
    else
        export HADOOP_CONF_DIR=/etc/hive/conf
    fi
fi

class_path=$PWD_PATH/conf:$HADOOP_CONF_DIR:$PWD_PATH/libs/*

export SERVICE_OPTS="$SERVICE_OPTS -Dproject.name=${srv_name}-$(hostname)"
#export SERVICE_OPTS="$SERVICE_OPTS -Dlog4j2.enable.threadlocals=true"
#export SERVICE_OPTS="$SERVICE_OPTS -Dlog4j2.enable.direct.encoders=true"
#export SERVICE_OPTS="$SERVICE_OPTS -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
export SERVICE_OPTS="$SERVICE_OPTS -Dsun.security.krb5.debug=false"

java_cmd='';
if [ -z "$CUSTOMED_JAVA_CMD" ]; then
    java_cmd='java';
else
    java_cmd="$CUSTOMED_JAVA_CMD";
fi

# shellcheck disable=SC2120
function start() {
    echo "Starting ${srv_name} ..."
    if [[ -n $script ]]; then
        sh $script $pid_file >> $LOG_DIR/$LOG_NAME.out 2>&1 &
        echo "Started ${srv_name} with pid ${pid}"
        return $?
    fi
    if [ -f $pid_file ]; then
      if kill -0 `cat $pid_file` > /dev/null 2>&1; then
        echo $srv_name running as process `cat $pid_file`.  Stop it first.
        exit 1
      fi
    fi
    nohup $java_cmd $SERVICE_OPTS -cp $class_path $main_class $args > $LOG_DIR/$LOG_NAME.out 2>&1 &
    pid=$!
    echo $pid > $pid_file
    echo "Started ${srv_name} with pid ${pid}"
}

function stop() {
    if [ -f $pid_file ]; then
        pid=`cat $pid_file`
        stop_pid $pid
    else
        pids=`ps -ef|grep "${main_class}" | grep -v grep | awk '{print $2}'`
        if [ -z "${pids}" ]; then
            echo "No ${srv_name} pid file found"
        else
            echo "No ${srv_name} pid file found，but [${pids}] looks like a ${srv_name} process"
        fi
    fi
}


function stop_pid() {
    pid=$1
    echo "Stopping ${srv_name} with pid $pid ..."
    if kill -0 $pid > /dev/null 2>&1; then
        kill $pid
        for a in {1..10}
        do
            if kill -0 $pid > /dev/null 2>&1; then
                sleep 1
            else
                break
            fi
        done
        if kill -0 $pid > /dev/null 2>&1; then
            echo "$srv_name not stopped in 10 seconds, try force kill"
            kill -9 $pid
        fi
    fi
    rm -f $pid_file
    echo "Stopped ${srv_name} "
}

if [ "$1" = "start" ]; then
    start
elif [ "$1" = "stop" ]; then
    stop
elif [ "$1" = "restart" ]; then
    stop
    start
fi
