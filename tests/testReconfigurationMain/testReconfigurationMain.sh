#!/bin/bash
LOGFILE=/tmp/log
# set this path to use binaries directly from IDE set to build
# automatically instead of  having to recompile using ant
IDE_BUILD_PATH=./build/classes/
LIB_PATH=./lib/*
ANT_BUILD_PATH=./dist/*
CLASSPATH=.:$IDE_BUILD_PATH:$ANT_BUILD_PATH:$LIB_PATH

GP_PROPERTIES=`dirname $0`/gigapaxos1.properties
TESTING_PROPERTIES=`dirname $0`/testing1.properties

if [[ ! -z $1 ]]; then
  $GP_PROPERTIES=$1
fi
if [[ ! -z $2 ]]; then
  $TESTING_PROPERTIES=$2
fi

SSH_OPTIONS="-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/keyStore/node100.jks"

# kill existing instances
KILL_TARGET=TESTReconfigurationMain
kill -9 `ps -ef|grep $KILL_TARGET|grep -v grep|awk '{print $2}'` 2>/dev/null

java -ea -Xms4096M -cp $CLASSPATH $SSH_OPTIONS \
-DgigapaxosConfig=$GP_PROPERTIES \
-DtestingConfig=$TESTING_PROPERTIES \
-Djava.util.logging.config.file=logging.properties \
edu.umass.cs.reconfiguration.testing.TESTReconfigurationMain #2>$LOGFILE
