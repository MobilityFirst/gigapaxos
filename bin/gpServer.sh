#!/bin/bash 


APP_ARGS_KEY="appArgs"
APP_RESOURCES_KEY="appResourcePath"
DEBUG_KEY="debug"

# Usage notes printing
if [[ -z "$@" || -z `echo "$@"|grep \
"[ ]*\(start\|stop\|restart\|clear\|forceclear\) "` ]];
then
  echo "Usage: "`dirname $0`/`basename $0`" [JVMARGS] \
[-D$APP_RESOURCES_KEY=APP_RESOURCES_DIR] \
[-D$APP_ARGS_KEY=\"APP_ARGS\"] \
[-$DEBUG_KEY] \
stop|start|restart|clear|forceclear all|server_names"
echo "Examples:"
echo "    `dirname $0`/`basename $0` start AR1"
echo "    `dirname $0`/`basename $0` start AR1 AR2 RC1"
echo "    `dirname $0`/`basename $0` start all"
echo "    `dirname $0`/`basename $0` stop AR1 RC1"
echo "    `dirname $0`/`basename $0` stop all"
echo "    `dirname $0`/`basename $0` \
-DgigapaxosConfig=/path/to/gigapaxos.properties start all"
echo "    `dirname $0`/`basename $0` -cp myjars1.jar:myjars2.jar \
-DgigapaxosConfig=/path/to/gigapaxos.properties \
-D$APP_RESOURCES_KEY=/path/to/app/resources/dir/ \
-D$APP_ARGS_KEY=\"-opt1=val1 -flag2 \
-str3=\\""\"quoted arg example\\""\" -n 50\" \
-$DEBUG_KEY\
 start all

 Note: -$DEBUG_KEY option is insecure and should only be used during testing and development." 
exit
fi

BINFILE=`readlink $0`
if [[ -z $BINFILE ]]; then
  BINFILE=$0
fi
BINDIR=`dirname $BINFILE` 

. "$BINDIR"/gpEnv.sh

HEAD=`cd $BINDIR/..; pwd` 

# use jars and classes from default location if available
FILESET=`ls $HEAD/jars/*.jar $HEAD/jars/*.class 2>/dev/null`
DEFAULT_GP_CLASSPATH=`echo $FILESET|sed -E s/"[ ]+"/:/g`

# developers can use quick build 
DEV_MODE=1
if [[ $DEV_MODE == 1 ]]; then
# Use binaries before jar if available. Convenient to use with
# automatic building in IDEs.
  if [[ -e $HEAD/build/classes ]]; then
    DEFAULT_GP_CLASSPATH=$HEAD/build/classes:$DEFAULT_GP_CLASSPATH
  fi
  if [[ -e $HEAD/build/test/classes ]]; then
    DEFAULT_GP_CLASSPATH=$HEAD/build/test/classes:$DEFAULT_GP_CLASSPATH
  fi
  ENABLE_ASSERTS="-ea"
fi

# Wipe out any existing classpath, otherwise remote installations will
# copy unnecessary jars. Set default classpath to jars in ../jars by
# default. It is important to ensure that ../jars does not have
# unnecessary jars to avoid needless copying in remote installs.
export CLASSPATH=$DEFAULT_GP_CLASSPATH

# should not be changed
CONF=conf
CONFDIR=$HEAD/$CONF

# look for file in conf if default does not exist
function set_default_conf {
  default=$1
  if [[ -e $CONFDIR/$default ]]; then
    echo $CONFDIR/$default
  elif [[ -L $CONFDIR/$default && \
    -e `readlink $CONFDIR/$default` ]]; then
    echo `readlink $CONFDIR/$default`
  fi 
}

# default java.util.logging.config.file
DEFAULT_LOG_PROPERTIES=logging.properties
LOG_PROPERTIES=$(set_default_conf $DEFAULT_LOG_PROPERTIES)


# default log4j properties (used by c3p0)
DEFAULT_LOG4J_PROPERTIES=log4j.properties
LOG4J_PROPERTIES=$(set_default_conf $DEFAULT_LOG4J_PROPERTIES)

# default gigapaxos properties
DEFAULT_GP_PROPERTIES=gigapaxos.properties
GP_PROPERTIES=$(set_default_conf $DEFAULT_GP_PROPERTIES)

DEFAULT_KEYSTORE=keyStore.jks
KEYSTORE=$(set_default_conf $DEFAULT_KEYSTORE)

DEFAULT_TRUSTSTORE=trustStore.jks
TRUSTSTORE=$(set_default_conf $DEFAULT_TRUSTSTORE)

# 0 to disable
VERBOSE=2

JAVA=java

ACTIVE="active"
RECONFIGURATOR="reconfigurator"

DEFAULT_APP_RESOURCES=app_resources

DEFAULT_KEYSTORE_PASSWORD="qwerty"
DEFAULT_TRUSTSTORE_PASSWORD="qwerty"

DEBUG_MODE=false
DEBUG_PORT=10000
# remove classpath from args
ARGS_EXCEPT_CLASSPATH_DEBUG=`echo "$@"|\
sed s/"\-$DEBUG_KEY"/""/g|\
sed -E s/"\-(cp|classpath) [ ]*[^ ]* "/" "/g`

# set JVM args except classpath, app args, options
SUPPLIED_JVMARGS="`echo $ARGS_EXCEPT_CLASSPATH_DEBUG|\
sed s/-$APP_ARGS_KEY.*$//g|\
sed -E s/"[ ]*(start|stop|restart|clear|forceclear) .*$"//g`"

# extract classpath in args
ARG_CLASSPATH="`echo "$@"|grep " *\-\(cp\|classpath\) "|\
sed -E s/"^.* *\-(cp|classpath)  *"//g|\
sed s/" .*$"//g`"
# Reconstruct final classpath as the classpath supplied in args plus
# the default gigapaxos classpath. 
if [[ ! -z $ARG_CLASSPATH ]]; then
  CLASSPATH=$ARG_CLASSPATH:$DEFAULT_GP_CLASSPATH
fi


# separate out gigapaxos.properties and appArgs
declare -a args
index=1
start_stop_found=0
for arg in "$@"; do
  if [[ ! -z `echo $arg|grep "\-D.*="` ]]; then
    # JVM args and gigapaxos properties file
    key=`echo $arg|grep "\-D.*="|sed s/-D//g|sed s/=.*//g`
    value=`echo $arg|grep "\-D.*="|sed s/-D//g|sed s/.*=//g`
    if [[ $key == "gigapaxosConfig" ]]; then
      GP_PROPERTIES=$value
    elif [[ ! -z `echo $arg|grep "\-D$APP_RESOURCES_KEY="` ]]; 
    then
      # app args
      APP_RESOURCES="`echo $arg|grep "\-D$APP_RESOURCES_KEY="|\
        sed s/\-D$APP_RESOURCES_KEY=//g`"
    elif [[ ! -z `echo $arg|grep "\-D$APP_ARGS_KEY="` ]]; then
      # app args
      APP_ARGS="`echo $arg|grep "\-D$APP_ARGS_KEY="|\
        sed s/\-D$APP_ARGS_KEY=//g`"
    fi
  elif [[ $arg == "-$DEBUG_KEY" ]]; then
    DEBUG_MODE=true
  elif [[ $arg == "start" || $arg == "stop" || $arg == "restart" \
    || $arg == "clear" || $arg == "forceclear" ]]; 
  then
    # server names
    args=(${@:$index})
  fi
  index=`expr $index + 1`
done
# args has "start|stop|restart all|server_names"

# gigapaxos properties file must exist at this point
if [[ -z $GP_PROPERTIES || ! -e $GP_PROPERTIES ]]; then
  (>&2 echo "Error: Unable to find file \
$DEFAULT_GP_PROPERTIES")
  exit 1
fi

APP_RESOURCES=`echo $APP_RESOURCES|sed s/"\/$"//g`
APP_RESOURCES_SIMPLE=`echo $APP_RESOURCES|sed s/"^.*\/"//g`

# get APPLICATION from gigapaxos.properties file
APP=`grep "^[ \t]*APPLICATION[ \t]*=" $GP_PROPERTIES|sed s/^.*=//g`
  if [[ $APP == "" ]]; then
    # default app
    APP="edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp"
  fi

INSTALL_PATH=`echo $APP|sed s/".*\."//g`

if [[ $INSTALL_PATH_PREFIX != "" ]]; then
  INSTALL_PATH=$INSTALL_PATH_PREFIX/$INSTALL_PATH
fi

function get_simple_name {
  name=$1
  echo $CONF/`echo $name|sed s/".*\/"//g`
}



# set servers to start here
if [[ ${args[1]} == "all" ]]; then

  # get reconfigurators
    reconfigurators=`cat $GP_PROPERTIES|grep -v "^[ \t]*#"|\
      grep "^[ \t]*$RECONFIGURATOR"|\
      sed s/"^.*$RECONFIGURATOR\."//g|sed s/"=.*$"//g`
  
  # get actives
	actives=`cat $GP_PROPERTIES|grep -v "^[ \t]*#"|\
      grep "^[ \t]*$ACTIVE"| sed \ s/"^.*$ACTIVE\."//g|\
      sed s/"=.*$"//g`
  
  servers="$actives $reconfigurators"

echo servers=[$servers]
  
else 
  servers="${args[@]:1}"

fi

# get value for key from cmdline_args or gigapaxos properties file or
# default_value_container in that order
function get_value {
  key=$1
  cmdline_args=$2
  default_value_container=$3
  # search cmdline_args
  record=`echo $cmdline_args|grep $key`
  # else search gigapaxos properties
  if [[ -z $record ]]; then
	record=`cat $GP_PROPERTIES|grep -v "^[ \t]*#"|grep $key`
  fi
  # else search $default_value_container
  if [[ -z $record ]]; then
    record=$default_value_container
  fi
  # container either has the value or is the value itself
  if [[ ! -z `echo $record|grep "="` ]]; then
    value=`echo $record| sed s/"^.*$key="//g|\
      sed s/" .*$"//g`
  else 
    value=$default_value_container
  fi
}

# printing with levels
function print {
  level=$1
  msg=$2
  if [[ $VERBOSE -ge $level ]]; then
    i=0
    while [[ $i -lt $level ]]; do
      echo -n "  "
      i=`expr $i + 1`
    done
    echo $msg
  fi
  if [[ $level == 9 ]]; then
    exit
  fi
}

# files for remote transfers if needed
function get_file_list {
  cmdline_args=$@
  jar_files="`echo $CLASSPATH|sed s/":"/" "/g`"
  get_value "javax.net.ssl.keyStore" "$cmdline_args" "$KEYSTORE"
  KEYSTORE=$value
  get_value "javax.net.ssl.keyStorePassword" "$cmdline_args" \
    "$DEFAULT_KEYSTORE_PASSWORD"
  keyStorePassword=$value
  get_value "javax.net.ssl.trustStore" "$cmdline_args" "$TRUSTSTORE"
  TRUSTSTORE=$value
  get_value "javax.net.ssl.trustStorePassword" "$cmdline_args" \
    "$DEFAULT_TRUSTSTORE_PASSWORD"
  trustStorePassword=$value
  get_value "java.util.logging.config.file" "$cmdline_args" $LOG_PROPERTIES
  LOG_PROPERTIES=$value
  get_value "log4j.configuration" "$cmdline_args" $LOG4J_PROPERTIES
  LOG4J_PROPERTIES=$value

  conf_transferrables="$GP_PROPERTIES $KEYSTORE $TRUSTSTORE $LOG_PROPERTIES\
     $LOG4J_PROPERTIES $APP_RESOURCES"
  print 3 "transferrables="$jar_files $conf_transferrables
}

# trims conf_transferrables only to files that exist
function trim_file_list {
  list=$1
  for i in $list; do
    if [[ -e $i ]]; then
      existent="$existent $i"
    fi
  done
  conf_transferrables=$existent
}


get_file_list "$@"
trim_file_list "$conf_transferrables"

# disabling warnings to prevent manual override; can supply ssh keys
# here if needed, but they must be the same on the local host and on
# the first host that continues the installation.
SSH="ssh -x -o StrictHostKeyChecking=no"

RSYNC_PATH="mkdir -p $INSTALL_PATH $INSTALL_PATH/$CONF"
RSYNC="rsync --force -aL "

username=`grep "USERNAME=" $GP_PROPERTIES|grep -v "^[ \t]*#"|\
  sed s/"^[ \t]*USERNAME="//g`
if [[ -z $username ]]; then
  username=`whoami`
fi

function append_to_ln_cmd {
  src_file=$1
  default=$2
  unlink_first=$3
  simple=`echo $1|sed s/".*\/"//g`
  simple_default=`echo $2|sed s/".*\/"//g`

  link_target="$INSTALL_PATH/$(get_simple_name $default)"
  if [[ $link_target != /* ]]; then
    link_target="~/$link_target"
  fi
  link_src="$INSTALL_PATH/$CONF/$simple"
  if [[ $link_src != /* ]]; then
    link_src="~/$link_src"
  fi
  cur_link="ln -fs $link_src $link_target "
  if [[ ! -z $unlink_first ]]; then
    cur_link="if [[ -L $link_target ]]; then \
      unlink $link_target; fi; $cur_link"
  fi

  if [[ -e $1 && $simple != $simple_default ]]; then
    if [[ -z $LINK_CMD ]]; then
      LINK_CMD=";$cur_link"
    else
      LINK_CMD="$LINK_CMD; $cur_link "
    fi
  fi
}
# This sym link stuff is now only for ease of seeing which
# configuration files are currently being used.
append_to_ln_cmd $GP_PROPERTIES $DEFAULT_GP_PROPERTIES
append_to_ln_cmd $KEYSTORE $DEFAULT_KEYSTORE
append_to_ln_cmd $TRUSTSTORE $DEFAULT_TRUSTSTORE
append_to_ln_cmd $LOG_PROPERTIES $DEFAULT_LOG_PROPERTIES
append_to_ln_cmd $LOG4J_PROPERTIES $DEFAULT_LOG4J_PROPERTIES
append_to_ln_cmd $APP_RESOURCES $DEFAULT_APP_RESOURCES true

function rsync_symlink {
  address=$1
  print 1 "Transferring conf files to $address:$INSTALL_PATH"

  print 2 "$RSYNC --rsync-path=\"$RSYNC_PATH $LINK_CMD && rsync\" \
    $conf_transferrables $username@$address:$INSTALL_PATH/$CONF/"
  $RSYNC --rsync-path="$RSYNC_PATH $LINK_CMD && rsync" \
    $conf_transferrables $username@$address:$INSTALL_PATH/$CONF/
}

LOCAL_SSL_KEYFILES="-Djavax.net.ssl.keyStore=$KEYSTORE \
-Djavax.net.ssl.trustStore=$TRUSTSTORE"

REMOTE_SSL_KEYFILES="-Djavax.net.ssl.keyStore=$(get_simple_name $KEYSTORE) \
-Djavax.net.ssl.trustStore=$(get_simple_name $TRUSTSTORE)"

COMMON_JVMARGS="$ENABLE_ASSERTS \
-Djavax.net.ssl.keyStorePassword=$keyStorePassword \
-Djavax.net.ssl.trustStorePassword=$trustStorePassword"

# Can add more JVM args here. JVM args specified on the command-line
# will override defaults specified here.
DEFAULT_JVMARGS="-cp $CLASSPATH $COMMON_JVMARGS \
$LOCAL_SSL_KEYFILES \
-Djava.util.logging.config.file=$LOG_PROPERTIES \
-Dlog4j.configuration=$LOG4J_PROPERTIES \
-DgigapaxosConfig=$GP_PROPERTIES"

JVMARGS="$DEFAULT_JVMARGS $SUPPLIED_JVMARGS"

DEFAULT_REMOTE_JVMARGS="$COMMON_JVMARGS \
$REMOTE_SSL_KEYFILES \
-Djava.util.logging.config.file=$(get_simple_name $LOG_PROPERTIES) \
-Dlog4j.configuration=$(get_simple_name $LOG4J_PROPERTIES) \
-DgigapaxosConfig=$(get_simple_name $DEFAULT_GP_PROPERTIES)"

REMOTE_JVMARGS="$SUPPLIED_JVMARGS $DEFAULT_REMOTE_JVMARGS"

# gets address and port of server from gigapaxos properties file
function get_address_port {
  server=$1
  # for verbose printing
  addressport=`grep "\($ACTIVE\|$RECONFIGURATOR\).$server=" \
    $GP_PROPERTIES| grep -v "^[ \t]*#"|\
    sed s/"^[ \t]*$ACTIVE.$server="//g|\
    sed s/"^[ \t]*$RECONFIGURATOR.$server="//g`
  address=`echo $addressport|sed s/:.*//g`
  if [[ $addressport == "" ]]; then
    non_existent="$server $non_existent"
    return
  fi
  # check if interface is local. Some unixes don't have ifconfig, so
  # we use "ip address" instead for those OSes.
  ifconfig_found=`type ifconfig 2>/dev/null`
  if [[ -z $ifconfig_found ]]; then
    ifconfig_found=`type ip 2>/dev/null`
    ifconfig_cmd="ip address"
  else 
    ifconfig_cmd="ifconfig"
  fi
}

# start server if local, else append to non_local list
function start_server {
  server=$1
  get_address_port $server

  DEBUG_ARGS=""
  if [ "$DEBUG_MODE" = true ]; then
    DEBUG_ARGS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$DEBUG_PORT"
    if [[ $VERBOSE == 2 ]]; then
      echo "Debug: $server at $address:$DEBUG_PORT"
    fi
    DEBUG_PORT=$((DEBUG_PORT+1))
  fi

  if [[ $ifconfig_found != "" && `$ifconfig_cmd|grep $address` != "" ]]; then
    if [[ $VERBOSE == 2 ]]; then
      echo "$JAVA $DEBUG_ARGS $JVMARGS \
        edu.umass.cs.reconfiguration.ReconfigurableNode $server&"
    fi
    $JAVA $DEBUG_ARGS $JVMARGS \
      edu.umass.cs.reconfiguration.ReconfigurableNode $server&
  else
    # first rsync files to remote server
    non_local="$server=$addressport $non_local"
    echo "Starting remote server $server"
    print 1 "Transferring jar files $jar_files to $address:$INSTALL_PATH"
    print 2 "$RSYNC --rsync-path=\"$RSYNC_PATH && rsync\" \
      $jar_files $username@$address:$INSTALL_PATH/jars/ "
    $RSYNC --rsync-path="$RSYNC_PATH && rsync" \
      $jar_files $username@$address:$INSTALL_PATH/jars/ 
    rsync_symlink $address

    # then start remote server
    print 2 "$SSH $username@$address \"cd $INSTALL_PATH; nohup \
      $JAVA $DEBUG_ARGS $REMOTE_JVMARGS \
      -cp \`ls jars/*|awk '{printf \$0\":\"}'\` \
      edu.umass.cs.reconfiguration.ReconfigurableNode \
      $APP_ARGS $server \""
    
    $SSH $username@$address "cd $INSTALL_PATH; nohup \
      $JAVA $DEBUG_ARGS $REMOTE_JVMARGS \
      -cp \`ls jars/*|awk '{printf \$0\":\"}'\` \
      edu.umass.cs.reconfiguration.ReconfigurableNode \
      $APP_ARGS $server "&
  fi
}

function start_servers {
if [[ $servers != "" ]]; then
  # print app and app args
  if [[ $APP_ARGS != "" ]]; then
    echo "[$APP $APP_ARGS]"
  else
    echo "[$APP]"
  fi
  # start servers
  for server in $servers; do
    start_server $server
  done
  if [[ $non_local != "" && $VERBOSE != 0 ]]; then
    echo "Ignoring non-local server(s) \" $non_local\""
  fi
fi
}

function stop_servers {
  for i in $servers; do
      get_address_port $i
      KILL_TARGET="ReconfigurableNode .*$i"
      if [[ ! -z $ifconfig_found && `$ifconfig_cmd|grep $address` != "" ]]; 
      then
        pid=`ps -ef|grep "$KILL_TARGET"|grep -v grep|\
          awk '{print $2}' 2>/dev/null`
        if [[ $pid != "" ]]; then
          foundservers="$i($pid) $foundservers"
          pids="$pids $pid"
        fi
      else 
        # remote kill
        echo "Stopping remote server $server"
        echo $SSH $username@$address "\"kill -9 \`ps -ef|\
          grep \"$KILL_TARGET\"|grep -v grep|awk \
          '{print \$2}'\` 2>/dev/null\""
        $SSH $username@$address "kill -9 \`ps -ef|\
          grep \"$KILL_TARGET\"|grep -v grep|awk \
          '{print \$2}'\` 2>/dev/null"
      fi
  done
  if [[ `echo $pids|sed s/" *"//g` != "" ]]; then
    echo killing $foundservers
    kill -9 $pids 2>/dev/null
  fi
}

function clear_all {
if [[ ! -z `echo "$@"|grep "clear[ ]*all"` ]]; then
  if [[ -z `echo "$@"|grep "forceclear[ ]*all"` ]]; then
    read -p "Are you sure you want to wipe out all paxos state? " yn
    case $yn in
        [Yy]* );; 
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";exit;;
    esac
  fi
  # else go ahead and force clear
          stop_servers
          for server in $servers; do
            get_address_port $server
            if [[ ! -z $ifconfig_found && `$ifconfig_cmd|grep $address` != "" ]];
            then
              print 3 "$JAVA $JVMARGS  \
                edu.umass.cs.reconfiguration.ReconfigurableNode \
                clear $server"
              $JAVA $JVMARGS \
                edu.umass.cs.reconfiguration.ReconfigurableNode \
                clear $server
      
            else
              # remote clear
              echo "Clearing state on remote server $server"
              print 2 "$SSH $username@$address \"cd $INSTALL_PATH; nohup \
                $JAVA $REMOTE_JVMARGS \
                -cp \`ls jars/*|awk '{printf \$0\":\"}'\` \
                edu.umass.cs.reconfiguration.ReconfigurableNode \
                clear $server \""
              $SSH $username@$address "cd $INSTALL_PATH; nohup \
                $JAVA $REMOTE_JVMARGS \
                -cp \`ls jars/*|awk '{printf \$0\":\"}'\` \
                edu.umass.cs.reconfiguration.ReconfigurableNode \
                clear $server "&
    
              fi
            done;
elif [[ ! -z `echo "$@"|grep "clear|forceclear"` ]]; then
  echo; echo "The 'clear' and 'forceclear' options can be \
    used only as 'clear all' or 'forceclear all'"
fi
}

case ${args[0]} in

start)
  start_servers
;;

restart)
    stop_servers
    start_servers
;;

stop)
  stop_servers
;;

clear)
  clear_all "$@"
;;

forceclear)
  clear_all "$@"

esac
