#!/bin/bash

HEAD=`dirname $0`
CLASSPATH=$CLASSPATH:`ls $HEAD/../dist/gigapaxos-[0-9].[0-9].jar`
LOG_PROPERTIES=logging.properties
LOG4J_PROPERTIES=log4j.properties
GP_PROPERTIES=gigapaxos.properties

JAVA=java
JVMFLAGS="-ea -Djava.util.logging.config.file=$LOG_PROPERTIES \
 -DgigapaxosConfig=$GP_PROPERTIES \
-Dlog4j.configuration=log4j.properties"

ACTIVE="active"
RECONFIGURATOR="reconfigurator"

SSL_OPTIONS="-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/keyStore/node100.jks"

if [[ $2 == "all" ]]; then

# get reconfigurators
  reconfigurators=`cat $GP_PROPERTIES|grep "^[ \t]*$RECONFIGURATOR"|\
sed s/"^.*$RECONFIGURATOR."//g|sed s/"=.*$"//g`

# get actives
  actives=`cat $GP_PROPERTIES|grep "^[ \t]*$ACTIVE"|\
sed s/"^.*$ACTIVE."//g|sed s/"=.*$"//g`

servers="$actives $reconfigurators"

fi

function start_server {

  $JAVA $JVMFLAGS $SSL_OPTIONS \
edu.umass.cs.reconfiguration.ReconfigurableNode "$@"&

}

case $1 in

start)

if [[ $servers != "" ]]; then

  echo Starting $servers

      start_server $servers

else 
  for i in $@; do
    if [[ $i != $1 ]]; then
      start_server $i
      echo
    fi
  done
fi
;;

stop)
if [[ $servers != "" ]]; then

    KILL_TARGET="ReconfigurableNode $i"
    kill -9 `ps -ef|grep "$KILL_TARGET"|grep -v grep|awk '{print $2}'` 2>/dev/null

else
  for i in $@; do
    if [[ $i != $1 ]]; then
      KILL_TARGET="ReconfigurableNode $i"
      kill -9 `ps -ef|grep "$KILL_TARGET"|grep -v grep|awk '{print $2}'` 2>/dev/null
    fi
  done
fi

esac
