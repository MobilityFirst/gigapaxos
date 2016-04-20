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

if [[ -z "$@" ]]; then
  echo "Usage: "`dirname $0`/`basename $0`" [-Dsysprop1=val1 \
-Dsysprop2=val2 ...] [-appArgs=\"-opt1=val1 -flag2 \
-str3="\\""\"hello world""\\""\"" ...\"] \
 stop|start  all|server_names"
fi

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
  elif [[ ! -z `echo $arg|grep "\-appArgs.*="` ]]; then
    APP_ARGS="`echo $arg|grep "\-appArgs.*="|sed s/\-appArgs=//g`"
    APP=`grep "^[ \t]*APPLICATION[ \t]*=" $GP_PROPERTIES|sed s/^.*=//g`
    if [[ -z $APP ]]; then
      APP="edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp"
    fi
  else 
    args[$index]=$arg
    index=`expr $index + 1`
  fi
done
#echo $JVMARGS "|" ${args[*]}

# can add more JVM args here
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
edu.umass.cs.reconfiguration.ReconfigurableNode $APP_ARGS $servers&

}

case ${args[0]} in

start)

if [[ $servers != "" ]]; then
  echo "[$APP $APP_ARGS]"
  start_server $servers
fi
;;

stop)
  echo Killing $servers
  for i in $servers; do
      KILL_TARGET="ReconfigurableNode .* $i"
      kill -9 `ps -ef|grep "$KILL_TARGET"|grep -v grep|awk '{print $2}'` 2>/dev/null
  done

esac
