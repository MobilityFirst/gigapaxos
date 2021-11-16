Prerequisites: `Java 1.8+`, `ant`, `bash` 

Tutorial: HTTP APIs 
------------------- 
You can also use our HTTP APIs to interact with GigaPaxos. Checkout the most recent source code if you haven't done so:

```
git clone https://github.com/MobilityFirst/gigapaxos
cd gigapaxos
ant jar
```

This tutorial shows you how to use HTTP APIs to send a request to GigaPaxos. 

#### Start up servers
First start up servers with the config file <tt>conf/examples/http.properties</tt>:

```
bin/gpServer.sh -DgigapaxosConfig=conf/examples/http.properties start all
```

The config file <tt>conf/examples/http.properties</tt> looks as
    
    APPLICATION=edu.umass.cs.reconfiguration.http.HttpActiveReplicaTestApp
      
    ENABLE_ACTIVE_REPLICA_HTTP=true
    
    # format: active.<active_server_name>=host:port
    active.AR0=127.0.0.1:2000
    
    # format: reconfigurator.<active_server_name>=host:port
    reconfigurator.RC0=127.0.0.1:3000

It enables ActiveReplica HTTP server ([HttpActiveReplica.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/http/HttpActiveReplica.java>)) by setting <tt>ENABLE_ACTIVE_REPLICA_HTTP=true</tt>. The `Replicable` app being used is `edu.umass.cs.reconfiguration.http.HttpActiveReplicaTestApp`.

If the HTTP server is successfully booted up, you should see a message:

    ...
    HttpActiveReplica ready on /127.0.0.1:2300
    ...

, which means the HTTP server is running on [http://127.0.0.1:2300](<http://127.0.0.1:2300>).

#### Send requests through HTTP API
You may use your browser to issue requests to GigaPaxos by embedding your request into `URI`. For example, to send a coordinated request to the default service name "HttpActiveReplicaTestApp0", open the link with your browser:
[http://127.0.0.1:2300/?name=HttpActiveReplicaTestApp0&qval=abc](<http://127.0.0.1:2300/?name=HttpActiveReplicaTestApp0&qval=abc>)

Note, you must specify the service name `name` and request value `qval` fields in the request. 

If the request gets executed successfully, your browser should show you a message like

    RESPONSE:
    
    {"COORD":true,"STOP":false,"QVAL":"abc","EPOCH":0,"RVAL":"ACK","type":400,"QID":607203182,"NAME":"HttpActiveReplicaTestApp0"}

By default, the request is coordinated if the field "coord" is unspecified. To send an uncoordinated request, your need to set "coord=false". Open the link with your browser to send an uncoordinated request:
[http://127.0.0.1:2300/?name=HttpActiveReplicaTestApp0&qval=def&coord=false](<http://127.0.0.1:2300/?name=HttpActiveReplicaTestApp0&qval=def&coord=false>)

#### Create and delete service names
GigaPaxos reconfigurators also support HTTP APIs to create or delete a service name and request active replicas of a service name.
As you may already noticed, the HTTP server of reconfigurator RC0 is running at [http:/127.0.0.1:3300](http:/127.0.0.1:3300).

To create a service name <i>Alice</i>, open the link below:
[http://127.0.0.1:3300/?type=CREATE&name=Alice](http://127.0.0.1:3300/?type=CREATE&name=Alice)

Besides `type=CREATE`, `type=234` also represents [CreateServiceName](https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/reconfigurationpackets/CreateServiceName.java) request.

Now you can send a request against this newly created service name, e.g., open the following link to send a coordinated request to active replica AR0 to update the state associated with the service name <i>Alice</i>:
[http://localhost:2300/?name=Alice&qval=abc](<http://localhost:2300/?name=Alice&qval=abc>)

To delete this service name, open the link below:
[http://127.0.0.1:3300/?type=DELETE&name=Alice](http://127.0.0.1:3300/?type=DELETE&name=Alice)

Besides `type=DELETE`, `type=235` represents [DeleteServiceName](https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/reconfigurationpackets/DeleteServiceName.java) request.

#### Stop servers
To stop and cleanup the service:
```
bin/gpServer.sh -DgigapaxosConfig=conf/examples/http.properties forceclear all
```


