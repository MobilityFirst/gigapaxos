#!/bin/bash
# set this path to use binaries directly from IDE set to build
# automatically instead of  having to recompile using ant
IDE_BUILD_PATH=./build/classes/:./lib/*
ANT_BUILD_PATH=./jars/*
CLASSPATH=.:$IDE_BUILD_PATH:$ANT_BUILD_PATH

SSH_OPTIONS="-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore.jks \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/trustStore.jks"

# kill existing instances
KILL_TARGET=TESTPaxosMain
kill -9 `ps -ef|grep $KILL_TARGET|grep -v grep|awk '{print $2}'` 2>/dev/null

java -ea -Xms4096M -cp $CLASSPATH -DgigapaxosConfig=$1 \
-DtestingConfig=$2 \
-Djava.util.logging.config.file=conf/logging.properties \
$SSH_OPTIONS edu.umass.cs.gigapaxos.testing.TESTPaxosMain -c #2>$LOGFILE
