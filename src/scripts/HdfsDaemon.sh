#!/bin/sh

# Setup variables
EXEC=/usr/lib/bigtop-utils/jsvc
JAVA_HOME=/usr/jdk64/jdk1.8.0_40/
CLASS_PATH="/usr/hdp/2.3.0.0-2557/hbase/lib/commons-daemon-1.0.13.jar":"/HdfsFileDaemon.jar"
CLASS=Daemon.Main
USER=root
PID=/tmp/HdfsFileDaemon.pid
LOG_OUT=/tmp/HdfsFileDaemon.out
LOG_ERR=/tmp/HdfsFileDaemon.err
# SOLR host, HDFS uri, HDFS file path to monitor, HBase ZooKeepers (zk1, zk2, zk3)
ARGS='http://adis-dal06.cloud.hortonworks.com:8983/solr/fiserv hdfs://adis-dal01.cloud.hortonworks.com:8020 /falcon adis-dal01.cloud.hortonworks.com,adis-dal02.cloud.hortonworks.com,adis-dal03.cloud.hortonworks.com'

do_exec()
{
    $EXEC -home "$JAVA_HOME" -cp $CLASS_PATH -user $USER -outfile $LOG_OUT -errfile $LOG_ERR -pidfile $PID $1 $CLASS $ARGS
}

case "$1" in
    start)
        do_exec
            ;;
    stop)
        do_exec "-stop"
            ;;
    restart)
        if [ -f "$PID" ]; then
            do_exec "-stop"
            do_exec
        else
            echo "service not running, will do nothing"
            exit 1
        fi
            ;;
    *)
            echo "usage: daemon {start|stop|restart}" >&2
            exit 3
            ;;
esac
