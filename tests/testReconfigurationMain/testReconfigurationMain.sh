#!/bin/bash
LOGFILE=/tmp/log
# set this path to use binaries directly from IDE set to build
# automatically instead of  having to recompile using ant
IDE_BUILD_PATH=./build/classes/:./lib/*
ANT_BUILD_PATH=./dist/*
CLASSPATH=.:$IDE_BUILD_PATH:$ANT_BUILD_PATH
SSH_OPTIONS="-Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks"

# kill existing instances
KILL_TARGET=TESTReconfigurationMain
kill -9 `ps -ef|grep $KILL_TARGET|grep -v grep|awk '{print $2}'` 2>/dev/null

java -ea -Xms4096M -cp $CLASSPATH $SSH_OPTIONS -DgigapaxosConfig=$1 -DtestingConfig=$2 -Djava.util.logging.config.file=logging.properties edu.umass.cs.reconfiguration.testing.TESTReconfigurationMain 2>>$LOGFILE
