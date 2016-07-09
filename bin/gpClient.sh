#!/bin/bash

BINFILE=`readlink $0`
if [[ -z $BINFILE ]]; then
  BINFILE=$0
fi
BINDIR=`dirname $BINFILE`
HEAD=`cd $BINDIR/..; pwd`

VERBOSE=1

# use jars and classes from default location if available
FILESET=`ls $HEAD/jars/*.jar $HEAD/jars/*.class 2>/dev/null`
DEFAULT_GP_CLASSPATH=`echo $FILESET|sed -E s/"[ ]+"/:/g`
# developers can use quick build 
DEV_MODE=1
if [[ $DEV_MODE == 1 ]]; then
# Use binaries before jar if available. Convenient to use with
# automatic building in IDEs.
  DEFAULT_GP_CLASSPATH=$HEAD/build/classes:$HEAD/build/test/classes:$DEFAULT_GP_CLASSPATH
  ENABLE_ASSERTS="-ea"
fi

# Wipe out any existing classpath, otherwise remote installations will
# copy unnecessary jars. Set default classpath to jars in ../jars by
# default. It is important to ensure that ../jars does not have
# unnecessary jars to avoid needless copying in remote installs.
export CLASSPATH=$DEFAULT_GP_CLASSPATH

CONF=conf
CONFDIR=$HEAD/$CONF

# look for file in conf if it does not exist
function set_default_conf {
  default=$1
  if [[ ! -e $default && (-e $CONFDIR/$default \
    || -L $CONFDIR/$default) ]]; then
    echo $CONF/$default
  else
    echo $default
  fi
}

# default java.util.logging.config.file
DEFAULT_LOG_PROPERTIES=$CONFDIR/logging.properties
LOG_PROPERTIES=$(set_default_conf $DEFAULT_LOG_PROPERTIES)

# default log4j properties (used by c3p0)
DEFAULT_LOG4J_PROPERTIES=$CONFDIR/log4j.properties
LOG4J_PROPERTIES=$(set_default_conf $DEFAULT_LOG4J_PROPERTIES)

# default gigapaxos properties
DEFAULT_GP_PROPERTIES=$CONFDIR/gigapaxos.properties
GP_PROPERTIES=$(set_default_conf $DEFAULT_GP_PROPERTIES)

#DEFAULT_KEYSTORE=$CONFDIR/keyStore.jks
KEYSTORE=$(set_default_conf $CONFDIR/keyStore.jks)

#DEFAULT_TRUSTSTORE=$CONFDIR/trustStore.jks
TRUSTSTORE=$(set_default_conf $CONFDIR/trustStore.jks)

ACTIVE="active"
RECONFIGURATOR="reconfigurator"

SSL_OPTIONS="-Djavax.net.ssl.keyStorePassword=qwerty \
-Djavax.net.ssl.keyStore=$KEYSTORE \
-Djavax.net.ssl.trustStorePassword=qwerty \
-Djavax.net.ssl.trustStore=$TRUSTSTORE"

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

if [[ -z $APP ]]; then
  APP="edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp"
fi

# default clients
if [[ $APP == "edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp" \
  && -z $DEFAULT_CLIENT_ARGS ]];
then
  DEFAULT_CLIENT="edu.umass.cs.gigapaxos.examples.noop.NoopPaxosAppClient"
elif [[ $APP == \
"edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp" && \
-z $DEFAULT_CLIENT_ARGS ]];
then
  DEFAULT_CLIENT="edu.umass.cs.reconfiguration.examples.NoopAppClient"
fi

if [[ -z "$@" || $@ == "-help" ]]; then
  echo "Usage: gpClient.sh [JVMARGS] CLIENT_CLASS_NAME"
  echo "Example: gpClient.sh -cp jars/myclient.jar \
edu.umass.cs.reconfiguration.examples.NoopAppClient" 
  exit
fi

echo "java $SSL_OPTIONS $JVM_APP_ARGS $DEFAULT_CLIENT" 
java $SSL_OPTIONS $JVM_APP_ARGS $DEFAULT_CLIENT

