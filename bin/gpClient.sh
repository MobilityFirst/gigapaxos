#!/bin/bash

HEAD=`dirname $0`

# use jars and classes from default location if available
FILESET=`ls $HEAD/../jars/*.jar $HEAD/../jars/*.class 2>/dev/null`
DEFAULT_GP_CLASSPATH=`echo $FILESET|sed -E s/"[ ]+"/:/g`
# developers can use quick build 
DEV_MODE=0
if [[ $DEV_MODE == 1 ]]; then
# Use binaries before jar if available. Convenient to use with
# automatic building in IDEs.
  DEFAULT_GP_CLASSPATH=$HEAD/../build/classes:$DEFAULT_GP_CLASSPATH
  ENABLE_ASSERTS="-ea"
fi

# Wipe out any existing classpath, otherwise remote installations will
# copy unnecessary jars. Set default classpath to jars in ../jars by
# default. It is important to ensure that ../jars does not have
# unnecessary jars to avoid needless copying in remote installs.
export CLASSPATH=$DEFAULT_GP_CLASSPATH


VERBOSE=1

CONF=conf
# look for file in conf if it does not exist
function set_default_conf {
  default=$1
  if [[ ! -e $defaul && -e $CONF/$default ]]; then
    echo $CONF/$default
  else
    echo $default
  fi
}

LOG_PROPERTIES=$(set_default_conf "logging.properties")
GP_PROPERTIES=$(set_default_conf "gigapaxos.properties")

ACTIVE="active"
RECONFIGURATOR="reconfigurator"

SSL_OPTIONS="-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=conf/keyStore/node100.jks"

# remove classpath
ARGS_EXCEPT_CLASSPATH=`echo "$@"|\
sed -E s/"\-(cp|classpath) [ ]*[^ ]* "/" "/g`

# reconstruct classpath by adding supplied classpath
CLASSPATH="`echo "$@"|grep " *\-\(cp\|classpath\) "|\
sed -E s/"^.* *\-(cp|classpath)  *"//g|\
sed s/" .*$"//g`:$CLASSPATH"

DEFAULT_CLIENT_ARGS=`echo "$@"|sed s/".*-D[^ ]* "//g`

# separate out JVM args
declare -a args
index=0
for arg in $ARGS_EXCEPT_CLASSPATH; do
  if [[ ! -z `echo $arg|grep "\-D.*="` ]]; then
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

DEFAULT_JVMARGS="$ENABLE_ASSERTS -cp $CLASSPATH \
-Djava.util.logging.config.file=$LOG_PROPERTIES \
-Dlog4j.configuration=log4j.properties \
-DgigapaxosConfig=$GP_PROPERTIES"

JVM_APP_ARGS="$DEFAULT_JVMARGS `echo $ARGS_EXCEPT_CLASSPATH`"

APP=`cat $GP_PROPERTIES|grep "^[ \t]*APPLICATION="|\
sed s/"^[ \t]*APPLICATION="//g`

# default clients
if [[ $APP == "edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp" && \
-z `echo "$@"|grep edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp` ]];
then
  DEFAULT_CLIENT="edu.umass.cs.gigapaxos.examples.noop.NoopPaxosAppClient \
$DEFAULT_CLIENT_ARGS"
elif [[ $APP == \
"edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp" && \
-z `echo "$@"|grep edu.umass.cs.gigapaxos.examples.noop.NoopApp` ]];
then
  DEFAULT_CLIENT="edu.umass.cs.reconfiguration.examples.NoopAppClient \
$DEFAULT_CLIENT_ARGS"
fi

echo "java $SSL_OPTIONS $JVM_APP_ARGS" 

java $SSL_OPTIONS $JVM_APP_ARGS $DEFAULT_CLIENT
