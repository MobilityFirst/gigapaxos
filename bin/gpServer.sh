#!/bin/bash

HEAD=`dirname $0`
CLASSPATH=$CLASSPATH:`ls $HEAD/../dist/gigapaxos-[0-9].[0-9].jar`
LOG_PROPERTIES=logging.properties
LOG4J_PROPERTIES=log4j.properties
GP_PROPERTIES=gigapaxos.properties

JAVA=java

ACTIVE="active"
RECONFIGURATOR="reconfigurator"

SSL_OPTIONS="-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/keyStore/node100.jks"

# separate out JVM args
declare -a args
index=0
for arg in "$@"; do
  if [[ ! -z `echo $arg|grep "\-D.*="` ]]; then
    JVMARGS="$JVMARGS $arg"
    key=`echo $arg|grep "\-D.*="|sed s/-D//g|sed s/=.*//g`
    value=`echo $arg|grep "\-D.*="|sed s/-D//g|sed s/.*=//g`
    if [[ $key == "gigapaxosConfig" ]]; then
      GP_PROPERTIES=$value
    fi
  else 
    args[$index]=$arg
    index=`expr $index + 1`
  fi
done
#echo $JVMARGS "|" ${args[*]}

JVMARGS="-ea -cp $CLASSPATH -Djava.util.logging.config.file=$LOG_PROPERTIES \
 -DgigapaxosConfig=$GP_PROPERTIES \
-Dlog4j.configuration=log4j.properties"

if [[ ${args[1]} == "all" ]]; then

  # get reconfigurators
    reconfigurators=`cat $GP_PROPERTIES|grep "^[ \t]*$RECONFIGURATOR"|\
  sed s/"^.*$RECONFIGURATOR."//g|sed s/"=.*$"//g`
  
  # get actives
    actives=`cat $GP_PROPERTIES|grep "^[ \t]*$ACTIVE"|\
  sed s/"^.*$ACTIVE."//g|sed s/"=.*$"//g`
  
  servers="$actives $reconfigurators"
  
else 

  servers="${args[@]:1}"

fi


function start_server {

  $JAVA $JVMARGS $SSL_OPTIONS \
edu.umass.cs.reconfiguration.ReconfigurableNode "$@"&

}

case ${args[0]} in

start)

if [[ $servers != "" ]]; then

  echo Starting $servers
  start_server $servers
else 
  for i in ${args[*]}; do
    if [[ $i != ${args[0]} ]]; then
      start_server $i
    fi
  done
fi
;;

stop)
  echo killing $servers
  for i in $servers; do
      KILL_TARGET="ReconfigurableNode $i"
      kill -9 `ps -ef|grep "$KILL_TARGET"|grep -v grep|awk '{print $2}'` 2>/dev/null
  done

esac
