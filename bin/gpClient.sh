#!/bin/bash

HEAD=`dirname $0`
CLASSPATH=`ls $HEAD/../dist/gigapaxos-[0-9].[0-9].jar`:$CLASSPATH
LOG_PROPERTIES=logging.properties
GP_PROPERTIES=gigapaxos.properties
JVMARGS="-ea -cp $CLASSPATH -Djava.util.logging.config.file=$LOG_PROPERTIES \
 -DgigapaxosConfig=$GP_PROPERTIES"

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
  else
    args[$index]=$arg
    index=`expr $index + 1`
  fi
done
#echo $JVMARGS "|" ${args[*]}

APP=`cat $GP_PROPERTIES|grep "^[ \t]*APPLICATION="|                \
sed s/"^[ \t]*APPLICATION="//g`

if [[ $APP == "edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp" ]];
then CLIENT=edu.umass.cs.gigapaxos.examples.noop.NoopPaxosAppClient
else if [[ $APP == \
"edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp" || $APP == "" ]]; then
CLIENT=edu.umass.cs.reconfiguration.examples.NoopAppClient 
else
CLIENT=$1
fi 
fi

echo "Running $CLIENT"

java $JVMARGS $SSL_OPTIONS $CLIENT "${@:2}"
