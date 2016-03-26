# unless recursive_install is disabled!/bin/bash

# You should normally only need to change the first local_install_dir
# variable below and remote_user if different from the local user. 
# 
# Here are other requirements: (1) passwordless ssh must be set up
# from the local to remote hosts; (2) for recursive installation,
# passwordless ssh must be set up from the first to all remote hosts
# as this is needed to use the first machine as a pivot to install on
# all other machines, which is handy if your local machine has poorer
# network connectivity to a remote cluster but the remote machines are
# well connected between themselves. ssh keys if any can be specified
# in the SSH macro further below.

# Set recursive_install below to 0 to disable it and instead install
# to all remote machines from the local host. With recursive_install
# disabled, passwordless ssh is required only from the local host to
# all remote hosts, but not from the first to the rest.
recursive_install=0

package_name=gigapaxos #GNS
# absolute path of the install on the local host
local_install_dir=~/$package_name 

remote_user=ec2-user

# need self-hostname for ec2 installation if recursive_install is 1
install_host=lime

# relative to remote home. Currently, remote_install_dir has to be the
# same as local_install_dir.
remote_install_dir=$package_name 

# Regular expression for local jars to be transferred. This regex is
# used by rsync, so it must be a single file or a regex, not an
# explicit list of files.
local_jar_files=dist/$package_name-[0-9].[0-9]*.jar 

# everything that needs to be transferred; path relative to
# local_install_dir.
transfer_list="$local_jar_files\
  gigapaxos.properties testing.properties\
  logging.properties tests scripts"

# disabling warnings to prevent manual override; can supply ssh keys
# here if needed, but they must be the same on the local host and on
# the first host that continues the installation.
SSH="ssh -x -o StrictHostKeyChecking=no"

RSYNC="rsync --force -R"

# paths relative to $local_install_dir
gigapaxos_properties=gigapaxos.properties
testing_properties=testing.properties

# command(s) that need to be run 
# can add other targets here
server_kill_targets="PaxosServer\|TESTPaxos\|ReconfigurableNode" 
client_kill_targets="TESTPaxosClient" # can add other targets here

# command for distributed test
#paxos_server=reconfiguration.ReconfigurableNode
paxos_server=gigapaxos.PaxosServer
paxos_client=gigapaxos.testing.TESTPaxosClient
# command to run single node test(s) remotely
#paxos_server=gigapaxos.testing.TESTPaxosMain

# just removes the entire log directory
#cmd_prefix="rm -rf paxos_logs; "
# deletes log data; slower compared to the above option
cmd_suffix=" -c"

log_file_prefix=/tmp/log

pkg_prefix=edu.umass.cs.

# can specify executable and hosts file as $1 and $2 provided
# first argument is not "kill" or "-c"
if [[ ! -z $1 && ! $1 == "kill" ]]; then
  if [[ ! -z $1 ]]; then
    gigapaxos_properties=$1;
    transfer_list="$transfer_list $1"
  fi
  if [[ ! -z $2 ]]; then
    testing_properties=$2
    transfer_list="$transfer_list $2"
  fi
  if [[ ! -z $3 ]]; then
    paxos_server=$3
  fi
  if [[ ! -z $4 ]]; then
    paxos_client=$4
  fi
  if [[ ! -z $5 ]]; then
    cmd_suffix=$5
  fi
fi


client_kill_targets=$client_kill_targets"\|"`echo $paxos_client|sed s/".*\."//g`

tmp_paxos_server=`cat $testing_properties|grep SERVER_BINARY|awk -F "=" '{print $2}'`
if [[ ! -z $tmp_paxos_server ]]; then
  paxos_server=$tmp_paxos_server
fi

tmp_paxos_client=`cat $testing_properties|grep CLIENT_BINARY|awk -F "=" '{print $2}'`
if [[ ! -z $tmp_paxos_client ]]; then
  paxos_client=$tmp_paxos_client
fi

tmp_cmd_suffix=`cat $testing_properties|grep CMD_OPTIONS|awk -F "=" '{print $2}'`
if [[ ! -z $tmp_cmd_suffix ]]; then
  cmd_suffix=$tmp_cmd_suffix
fi


run_server_cmd="cd; cd $remote_install_dir;\
  "$cmd_prefix"\
  java -ea -Xmx4096M -cp ./$local_jar_files \
  -DgigapaxosConfig=$gigapaxos_properties\
  -DtestingConfig=$testing_properties\
  -Djava.util.logging.config.file=logging.properties\
   $pkg_prefix$paxos_server\
  "$cmd_suffix
run_client_cmd="cd; cd $remote_install_dir;\
  "$cmd_prefix"\
  java -ea -Xmx1046M -cp ./$local_jar_files\
  -DgigapaxosConfig=$gigapaxos_properties\
  -DtestingConfig=$testing_properties\
  -Djava.util.logging.config.file=logging.properties\
   $pkg_prefix$paxos_client\
   "$cmd_suffix

hosts=`cat $local_install_dir/$gigapaxos_properties|grep -v "^#"|\
  grep "^active.\|^reconfigurator.\|^clients"|awk -F "=|:" \
  '{print $2}'|uniq`;
clients=`cat $local_install_dir/$gigapaxos_properties|grep -v "^#"|\
  grep "^clients"|awk -F "=|:" '{print $2}'|uniq`;
first_host=`echo $hosts|sed s/" .*$"//g`

# but if first argument is "kill", we just kill and exit
if [[ $1 == "kill" ]]; then
  for i in `echo $hosts`; do
    # get node_id from node_config file
    node_id=`cat $gigapaxos_properties|grep $i|awk '{print $1}'`
    # kill running instances of this command and (re-)start
    echo $SSH $remote_user@$i "\"kill -9 \`ps -ef|\
      grep \"$server_kill_targets\|$client_kill_targets\"\
      |grep -v grep|awk '{print \$2}'\`;$command\""
    $SSH $remote_user@$i "kill -9 \`ps -ef|grep \
    \"$server_kill_targets\|$client_kill_targets\"|\
    grep -v grep|awk '{print \$2}'\` 2>/dev/null; $command"
  done
  exit
fi


# this bash script file
me=`dirname $0`/`basename $0`

# transfers transer_list to host argument
function transfer {
  # transfer each entry in list
  for i in `echo $transfer_list`; do
    if [[ -e $i ]]; then
      echo [`hostname`] $RSYNC --rsh=\"$SSH\" $i\
         $remote_user@$1:$remote_install_dir
      $RSYNC --rsh="$SSH" $i $remote_user@$1:$remote_install_dir
    fi
  done
}

# executes command remotely 
function run_remote_command { remote_host=$1
  # process identifiers to be killed
  kill_targets=$2 
  # needed to kill with precision
  node_id=$3 
  # command to be executed remotely
  command=$4 

  # kill running instances of this command and (re-)start
  echo [`hostname`] "$SSH" $remote_user@$remote_host "\"kill -9\
    \`ps -ef|grep \"$kill_targets\"|grep \"$node_id\"|grep -v \
    grep|awk '{print \$2}'\` 2>/dev/null;\""
  $SSH $remote_user@$remote_host "kill -9 \`ps -ef|grep \
    \"$kill_targets\"|grep \"$node_id\"|grep -v grep|awk \
    '{print \$2}'\` 2>/dev/null;"
  # without some sleep the subsequent command does not execute;
  sleep 2
  echo [`hostname`] "$SSH" $remote_user@$remote_host "\"$command\""
  $SSH $remote_user@$remote_host "$command"
}

# start client(s)
function start_clients {
  type=$1
   
  # The sleep duration here needs to be long enough to ensure that the
  # server installation on the remote hosts is complete, otherwise the
  # client requests will fail. There is currently no automatic way to
  # determine when the remote installs are complete.
  sleep_secs=20
  echo [`hostname`] "Waiting for $sleep_secs"s" before starting client";
  i=$sleep_secs
  while [[ $i -gt 0 ]]; do
    echo -n " $i "
    i=`expr $i - 1`
    sleep 1
  done
   
  for client in $clients; do
    # clients on the local host can not be started from a remote host
    if [[ ($type == "local" && ($client == "localhost" || $client == \
      "127.0.0.1" )) || ($type == "global" && ($client != "localhost"\
       && $client != "127.0.0.1" )) || $type == "all" ]]; then
      #echo "Starting client $client"
      run_remote_command $client $client_kill_targets\
         $client_kill_targets "$run_client_cmd" &
    fi
  done

}
 

# from local host to first host
#if [[ `hostname -f` != $first_host ]]; then
if [[ `hostname -f` == $install_host ]]; then
  if [[ $first_host != "" && $first_host != "localhost" && $first_host != "127.0.0.1" ]]; then
    echo [`hostname`] "Installing on `echo $first_host`"
    transfer $first_host
    echo [`hostname`] $RSYNC --rsh="$SSH" $me\
      $remote_user@$first_host:$remote_install_dir
    $RSYNC --rsh="$SSH" $me $remote_user@$first_host:$remote_install_dir
  fi
fi

# If I am the local host, execute install script (me) on the first
# remote host and exit; else continue installation from the first
# remote host.
if [[ $recursive_install == 1 ]]; then
  #if [[ `hostname -f` != $first_host ]]; then
  if [[ `hostname -f` == $install_host && $first_host != "localhost" && $first_host != "127.0.0.1" ]]; then
    # execute myself on the first remote host and exit
    echo [`hostname`] "$SSH" $remote_user@$first_host\
       "$remote_install_dir/$me $1 $2 &"
    $SSH $remote_user@$first_host "$remote_install_dir/$me $1 $2" &
    #echo [`hostname`] Installing remote servers other than $first_host; echo

    start_clients "local"
 
    exit

  else # on first host
    # change paths so that remote becomes local
    local_install_dir=$remote_install_dir
    echo [`hostname`] Continuing installation from `hostname -f`
  fi
fi



# everything below executes only at the first remote host unless
# recursive_install is disabled

# transfer to remaining hosts
if [[ -e $local_install_dir ]]; then
  cd $local_install_dir;
fi
for i in `echo $hosts|grep -v "localhost\|127.0.0.1"|sed s/$first_host//g`; do
  if [[ $i != "" ]]; then
    echo; echo [`hostname`] "Installing on `echo $i`"
    transfer $i
  fi
done 

echo; echo "[`hostname`] (Re-)starting servers"

# start servers
for node_id in `cat $gigapaxos_properties|grep -v "^#"|\
  grep "^active.\|^reconfigurator."|sed s/"active."//g|\
  sed s/"reconfigurator."//g|awk -F "=|:" '{print $1}'`;\
  do


    host=`cat $gigapaxos_properties|grep -v "^#"|\
      awk -F "=|:" -v id=$node_id '{if($1==("active."id) || $1==("reconfigurator."id))\
        print $2}'`
echo $host
    run_remote_command $host $server_kill_targets $node_id\
      "$run_server_cmd $node_id 2>$log_file_prefix$node_id "&
done


exit
# stuff below is not needed unless we want to start the client
# recursively from the first remote host

echo; echo; echo "[`hostname`] (Re-)starting client(s)"
if [[ $recursive_install == "1" && $first_host != "localhost" && $first_host != "127.0.0.1" ]]; then
  start_clients "global"
else if [[ $first_host == "localhost" || $first_host == "127.0.0.1" ]]; then
  start_clients "all"
fi
fi
