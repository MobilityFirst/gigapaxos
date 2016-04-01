# gigapaxos

Obtaining gigapaxos
-------------------
Option 1: Binary:
- Download the latest stable binary from <tt>http://date.cs.umass.edu/gigapaxos</tt>

Option 2: Source:
- Download gigapaxos from <tt>https://github.com/MobilityFirst/gigapaxos</tt> 
- In the main directory called gigapaxos, type <tt>ant</tt>, which will create a jar file 
  <tt>dist/gigapaxos-\<version\>.jar</tt>.

GigaPaxos overview
------------------
GigaPaxos is a group-scalable replicated state machine (RSM) system, i.e., it allows 
applications to easily create and manage a very large number of separate RSMs. Clients can 
associate each service with a separate RSM of its own from a subset of a pool of server 
machines. Thus, different services may be replicated on different sets of machine in 
accordance with their fault tolerance or performance requirements. The underlying consensus 
protocol for each RSM is Paxos, however it is carefully engineered so as to be extremely 
lightweight and fast. For example, each RSM uses only ~300 bytes of memory when it is idle 
(i.e., not actively processing requests), so commodity machines can participate in millions 
of different RSMs. When actively processing requests, the message overhead per request is 
similar to Paxos, but automatic batching of requests and Paxos messages significantly 
improves the throughput by reducing message overhead, especially when the number of 
different RSM groups is small, for example, gigapaxos achieves a small request throughput
of nearly 100K/s on commodity machines. The lightweight API for creating and interacting 
with  different RSMs allows applications to “carelessly” create consensus groups on the fly 
for even small shared objects, e.g. a simple counter or a lightweight stateful servlet. 
GigaPaxos also has extensive support for reconfiguration, i.e., the membership of 
different RSMs can be programmatically changed by applications by writing their own policy
classes.

GigaPaxos has a simple <tt>Replicable</tt> wrapper API that any “black-box” application can implement 
in order for it to be automatically be replicated and reconfigured as needed by GigaPaxos. 
This API requires three methods to be implemented: 

    execute(name, request) 
    checkpoint(name)
    restore(name, state) 

to respectively execute a request, obtain a state checkpoint, or 
to roll back the state of a service named “name”. GigaPaxos ensures that applications 
implementing the Replicable interface are also automatically <tt>Reconfigurable</tt>, 
i.e., their replica locations are automatically changed in accordance with an application-
specified policy.

Tutorial 1: Single-machine <tt>Replicable</tt> test-drive
---------------------------------------------------------

The default gigapaxos.properties file in the top-level directory has two sets of
entries respectively for "active" replica servers and "reconfigurator" servers. 
Every line in the former starts with the string <tt>active.</tt> followed by a string
that is the name of that active server (e.g., <tt>100, 101 or 102</tt> below) followed
by the separator '=' and a <tt>host:port</tt> address listening address for that server.
Likewise, every line in the latter starts with the string <tt>reconfigurator.</tt> 
followed by the separator and its <tt> host:port</tt> information

The <tt>APPLICATION</tt> parameter below specifies which application we will be 
using. The default is <tt>edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp</tt>,
so uncomment the <tt>APPLICATION</tt> line below (by removing the leading <tt>#</tt>) as 
we will be using this simpler "non-reconfigurable" <tt>NooPaxosApp</tt> application in 
this first tutorial. A non-reconfigurable application's replicas can not be moved 
around by gigapaxos, but a <tt>Reconfigurable</tt> application (such as <tt>NoopApp</tt>)
replicas can.

    #APPLICATION=edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp
    
    active.100=127.0.0.1:2000<br>
    active.101=127.0.0.1:2001<br>
    active.102=127.0.0.1:2002<br>

    reconfigurator.RC0=127.0.0.1:3100
    reconfigurator.RC1=127.0.0.1:3101
    reconfigurator.RC2=127.0.0.1:3102

At least one active server is needed to use gigapaxos. Three or more active 
servers are needed in order to tolerate a single active server failure. 
At least one reconfigurator is needed in order to be able to reconfigure RSMs
on active servers, and three or more for tolerating reconfigurator server
failures. Both actives and reconfigurators use consensus, so they need at 
least 2f+1 replicas in order to make progress despite up to f failures. 
Reconfigurators form the "control plane" of giagpaxos while actives form 
the "data plane" that is responsible for executing client requests.

For the single machine test, you can leave the default gigapaxos.properties
file as above unchanged with 3 actives and 3 reconfigurators even though
we won't really be using the reconfigurators at all. 

Run the servers as follows from the top-level directory:
    
    ./bin/gpServer.sh start all

If any actives or reconfigurators or other servers are already listening
on those ports, you will see errors in the log file (<tt> /tmp/gigapaxos.log
</tt>) by default). To make sure that no servers are already running, do

    ./bin/gpServer.sh stop all

To start or stop a specific active or reconfigurator, replace <tt>all</tt> 
above with the name of an active (e.g., <tt>100</tt>) or reconfigurator 
(e.g., <tt>RC1</tt>) above.

Wait until you see </tt>all servers ready</tt> on the console before 
starting any clients.

Then, start the default client as follows from the top-level directory:

    ./bin/gpClient.sh

The client will by default use </tt>NoopPaxosAppClient</tt> if the application
is NoopPaxosApp, and will use <tt>NoopAppClient</tt> if the application is
the default <tt>NoopApp</tt>. As we are using the former app in this tutorial,
running the above script will launch <tt>NoopPaxosAppClient</tt>.

For any application, a default paxos group called <tt>\<app_name\>0</tt> will
be created by the servers, so in this example, our (only) paxos group will
be called <tt>NoopPaxosApp0</tt>. 

The <tt>NoopPaxosAppClient</tt> client will simply send a few requests to the
servers, wait for the responses, and print them on the console. The client
is really simple and illustrates how to send callback-based requests. You
can view its source here: 
<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/gigapaxos/examples/noop/NoopPaxosClient.java>

<tt>NoopPaxosApp</tt> is also a trivial instantiation of <tt>Replicable</tt> 
and its source is here:
<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/gigapaxos/examples/noop/NoopPaxosApp.java>

You can verify that stopping one of the actives as follows will not affect the 
system's liveness, however, any requests going to the failed server will of 
course not get responses. The <tt>sendRequest</tt> method in 
<tt>NoopPaxosAppClient</tt> by default sends each request to a random replica, 
so roughly a third of the requests will be lost with a single failure. 

    bin/gpServer.sh stop 101

Next, browse through the methods in <tt>NooPaxosAppClient</tt>'s parent 
<tt>PaxosClientAsync</tt> at
<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/gigapaxos/PaxosClientAsync.java>
and use one of the <tt>sendRequest</tt> methods therein to direct all requests 
to a specific active server and verify that all requests (almost always) succeed 
despite a single active failure. You can also verify that with two failures, no
requests will succeed.

Tutorial 2: Single-machine <tt>Reconfigurable</tt> test-drive
-------------------------------------------------------------

For this test, we will set <tt>APPLICATION</tt> to the default 
<tt>NoopApp</tt> by simply re-commenting that line as in the
default gigapaxos.properties file.

Run the servers and clients exactly as before. You will see console 
output showing that <tt>NoopAppClient</tt> creates a few names and 
successfully sends a few requests to them. A <tt>Reconfigurable</tt>
application must implement slightly different semantics from just a
<tt>Replicable</tt> application. You can browse through 
the source of <tt>NoopApp</tt> and <tt>NoopAppClient</tt> and the 
documentation therein below:

<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/examples/noopsimple/NoopApp.java>

<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/examples/NoopAppClient.java>

Repeat the same failure scenario as above and verify that the
actives exhibit the same liveness properties as before.

Coming soon: tutorials for writing your own sophisticated reconfiguration policies.
