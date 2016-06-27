# gigapaxos

Obtaining gigapaxos
-------------------

Option 1: Binary:
- Download the latest stable binary from <tt>http://date.cs.umass.edu/gigapaxos</tt>

Option 2: Source:
- Download gigapaxos from <tt>https://github.com/MobilityFirst/gigapaxos</tt> 
- In the main directory called gigapaxos, type <tt>ant</tt>, which will create a jar file 
  <tt>dist/gigapaxos-\<version\>.jar</tt>. Make sure that <tt>ant</tt>
uses java1.8 or higher.

GigaPaxos overview 
------------------ 

GigaPaxos is a group-scalable
replicated state machine (RSM) system, i.e., it allows applications to
easily create and manage a very large number of separate RSMs. Clients
can associate each service with a separate RSM of its own from a
subset of a pool of server machines. Thus, different services may be
replicated on different sets of machine in accordance with their fault
tolerance or performance requirements. The underlying consensus
protocol for each RSM is Paxos, however it is carefully engineered so
as to be extremely lightweight and fast. For example, each RSM uses
only ~300 bytes of memory when it is idle (i.e., not actively
processing requests), so commodity machines can participate in
millions of different RSMs. When actively processing requests, the
message overhead per request is similar to Paxos, but automatic
batching of requests and Paxos messages significantly improves the
throughput by reducing message overhead, especially when the number of
different RSM groups is small, for example, gigapaxos achieves a
small-noop-request throughput of roughly 80K/s per core (and
proportionally more on multicore) on commodity machines. The
lightweight API for creating and interacting with  different RSMs
allows applications to “carelessly” create consensus groups on the fly
for even small shared objects, e.g. a simple counter or a lightweight
stateful servlet. GigaPaxos also has extensive support for
reconfiguration, i.e., the membership of different RSMs can be
programmatically changed by applications by writing their own policy
classes.

GigaPaxos has a simple <tt>Replicable</tt> wrapper API that any “black-box” application can implement 
in order for it to be automatically be replicated and reconfigured as needed by GigaPaxos. 
This API requires three methods to be implemented: 

    boolean execute(Request request) 
    String checkpoint(String name)
    boolean restore(String name, String state) 

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
    
    active.100=127.0.0.1:2000
    active.101=127.0.0.1:2001
    active.102=127.0.0.1:2002

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

For the single-machine, local test, except for setting
<tt>APPLICATION</tt> to <tt>NoopPaxosApp</tt>, you can leave the
default gigapaxos.properties file as above unchanged with 3 actives
and 3 reconfigurators even though we won't really be using the
reconfigurators at all. 

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

The client will by default use <tt>NoopPaxosAppClient</tt> if the application
is <tt>NoopPaxosApp</tt>, and will use <tt>NoopAppClient</tt> if the application is
the default <tt>NoopApp</tt>. As we are using the former app in this tutorial,
running the above script will launch <tt>NoopPaxosAppClient</tt>.

For any application, a default paxos group called <tt>\<app_name\>0</tt> will
be created by the servers, so in this example, our (only) paxos group will
be called <tt>NoopPaxosApp0</tt>. 

The <tt>NoopPaxosAppClient</tt> client will simply send a few requests to the
servers, wait for the responses, and print them on the console. The client
is really simple and illustrates how to send callback-based requests. You
can view its source here: 
[NoopPaxosClient.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/gigapaxos/examples/noop/NoopPaxosAppClient.java>)

<tt>NoopPaxosApp</tt> is a trivial instantiation of <tt>Replicable</tt> 
and its source is here:
[NoopPaxosApp.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/gigapaxos/examples/noop/NoopPaxosApp.java>)

You can verify that stopping one of the actives as follows will not affect the 
system's liveness, however, any requests going to the failed server will of 
course not get responses. The <tt>sendRequest</tt> method in 
<tt>NoopPaxosAppClient</tt> by default sends each request to a random replica, 
so roughly a third of the requests will be lost with a single failure. 

    bin/gpServer.sh stop 101

Next, browse through the methods in <tt>NooPaxosAppClient</tt>'s parent 
[PaxosClientAsync.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/gigapaxos/PaxosClientAsync.java>)
and use one of the <tt>sendRequest</tt> methods therein to direct all requests 
to a specific active server and verify that all requests (almost always) succeed 
despite a single active failure. You can also verify that with two failures, no
requests will succeed.

Tutorial 2: Single-machine <tt>Reconfigurable</tt> test-drive
-------------------------------------------------------------

For this test, we will use a fresh gigapaxos install and set
<tt>APPLICATION</tt> to the default <tt>NoopApp</tt> by simply
re-commenting that line as in the default gigapaxos.properties file as
shown below.

    #APPLICATION=edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp  
    #APPLICATION=edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp # default

Note: It is important to use a fresh gigapaxos install as the
<tt>APPLICATION</tt> can not be changed midway in an existing
gigapaxos directory; doing so will lead to errors as gigapaxos will
try to feed requests to the application that the application will fail
to parse. An alternative to a fresh install is to remove all gigapaxos
logs as follows from their default locations (or from their
non-default locations if you changed them in gigapaxos.properties):

    rm -rf ./paxos_logs ./reconfiguration_DB

Next, run the servers and clients exactly as before. You will see
console output showing that <tt>NoopAppClient</tt> creates a few names
and successfully sends a few requests to them. A
<tt>Reconfigurable</tt> application must implement slightly different
semantics from just a <tt>Replicable</tt> application. You can browse
through the source of <tt>NoopApp</tt> and <tt>NoopAppClient</tt> and
the documentation therein below:

[NoopApp.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/examples/noopsimple/NoopApp.java>)
 [[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/reconfiguration/examples/noopsimple/NoopApp.html>)

[NoopAppClient.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/examples/NoopAppClient.java>)
[[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/reconfiguration/examples/NoopAppClient.html>)

Step 1: Repeat the same failure scenario as above and verify that the
actives exhibit the same liveness properties as before.

Step 2: Set the property RECONFIGURE_IN_PLACE=true in gigapaxos.properties
in order to enable *trivial* reconfiguration, which means reconfiguring
a replica group to the same replica group while going through all of
the motions of the three-phase reconfiguration protocol (i.e.,
<tt>STOP</tt> the
previous epoch at the old replica group, <tt>START</tt> the new epoch in the
new replica group after having them fetch the final epoch state from
the old epoch's replica group, and finally having the old replica
group <tt>DROP</tt> all state from the previous epoch). 

The default reconfiguration policy trivially reconfigures the replica
group after *every* request. This policy is clearly an overkill as the
overhead of reconfiguration will typically be much higher than
processing a single application request (but it allows us to
potentially create a new replica at every new location from near which
even a single client request originates). Our goal here is to just
test a proof-of-concept and understand how to implement other more
practical policies.

Step 3: Run <tt>NoopAppClient</tt> by simply invoking the client command
like before:

    bin/gpClient.sh

<tt>NoopApp</tt> should print console output upon every
reconfiguration when its <tt>restore</tt> method will be called with a
<tt>null</tt> argument to wipe out state corresponding to the current
epoch and again immediately after when it is initialized with the
state corresponding to the next epoch for the service name being
reconfigured.

Step 4: Inspect the default *reconfiguration policy* in

[DemandProfile.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/reconfigurationutils/DemandProfile.java>)
[[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/reconfiguration/reconfigurationutils/DemandProfile.html>)

and the abstract class

[AbstractDemandProfile.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/reconfigurationutils/AbstractDemandProfile.java>)
[[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/reconfiguration/reconfigurationutils/AbstractDemandProfile.java>)

that any application-specific reconfiguration policy is expected to
extend in order to achieve its reconfiguration goals.

Change the default reconfiguration policy in <tt>DemandProfile</tt> so
that the default service name <tt>NoopApp0</tt> is reconfigured less
often. For example, you can set
<tt>MIN_REQUESTS_BEFORE_RECONFIGURATION</tt> and/or
<tt>MIN_RECONFIGURATION_INTERVAL</tt> to higher values. There are two
ways to do this: (i) the quick and dirty way is to change
<tt>DemandProfile.java</tt> directly and recompile gigapaxos from
source; (ii) the cleaner and recommended way is to write your own
policy implementation, say <tt>MyDemandProfile</tt>, that either
extends <tt>DemandProfile</tt> or extends
<tt>AbstractDemandProfile</tt> directly and specify it in
gigapaxos.properties by setting the <tt>DEMAND_PROFILE_TYPE</tt>
property by uncommenting the corresponding line and replacing the
value with the canonical class name of your demand profile
implementation as shown below. With this latter approach, you just
need the gigapaxos binaries and don't have to recompile it from
source. You do need to compile and generate the class file(s) for your
policy implementation.

	#DEMAND_PROFILE_TYPE=edu.umass.cs.reconfiguration.reconfigurationutils.DemandProfile


If all goes well, with the above changes, you should see NoopApp
reconfiguring itself less frequently as per the specification in your
reconfiguration policy!

Troubleshooting tips: If you run into errors:

(1) Make sure the canonical class name of your policy class is
correctly specified in gigapaxos.properties and the class exists in
your classpath. If the simple policy change above works as expected by
directly modifying the default <tt>DemandProfile</tt> implementation
and recompiling gigapaxos from source, but with your own demand
profile implementation you get <tt>ClassNotFoundException</tt> or
other runtime errors, the most likely reason is that the JVM can not
find your policy class.

(2) Make sure that all three constructors of DemandProfile that
respectively take a <tt>DemandProfile</tt>, <tt>String</tt>, and
<tt>JSONObject</tt> are overridden with the corresponding default
implementation that simply invokes <tt>super(arg)</tt>; all three
constructors are necessary for gigapaxos' reflection-based demand
profile instance creation to work correctly.

Step 5: Inspect the code in

[NoopAppClient.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/examples/NoopAppClient.java>)
[[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/reconfiguration/examples/NoopAppClient.html>)

to see how it is creating a service name by sending a
<tt>CREATE_SERVICE_NAME</tt> request. A service name corresponds to an
RSM, but note that there is no API to specify the set of active
replicas that should manage the RSM for the name being created. This
is because gigapaxos randomly chooses the initial replica group for
each service at creation time. Applications are expected to
reconfigure the replica group as needed after creation by using a
policy class as described above.

Once a service has been created, application requests can be sent to
it also using one of the <tt>sendRequest</tt> methods as exemplified
in <tt>NoopAppClient</tt>.

Deleting a service is as simple as issuing a
<tt>DELETE_SERVICE_NAME</tt> request using the same
<tt>sendRequest</tt> API as <tt>CREATE_SERVICE_NAME</tt> above.

Note that unlike <tt>NoopPaxosAppClient</tt> above,
<tt>NoopAppClient</tt> as well as the corresponding app,
<tt>NoopApp</tt> use a different request type called <tt>
AppRequest</tt> as opposed to the default <tt>RequestPacket</tt>
packet type. Reconfigurable gigapaxos applications can define their
own extensive request types as needed for different types of requests.
The set of request types that an application processes is conveyed to
gigapaxos via the <tt>Replicable.getRequestTypes()</tt> that the
application needs to implement.


Applications can also specify whether a request should be
paxos-coordinated or served locally by an active replica. By default,
all requests are executed locally unless the request is of type
<tt>ReplicableRequest</tt> and its <tt>needsCoordination</tt> method
is true (as is the case for <tt>AppRequest</tt> by default).

Verify that you can create, send application requests to, and delete a
new service using the methods above. 

A list of all relevant classes for Tutorial 2 mentioned above is
listed below for convenience:

[NoopAppClient.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/examples/NoopAppClient.java>)
[[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/reconfiguration/examples/NoopAppClient.html>)

[NoopApp.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/examples/noopsimple/NoopApp.java>)
 [[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/reconfiguration/examples/noopsimple/NoopApp.html>)

[AppRequest.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/examples/AppRequest.java>)
 [[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/reconfiguration/examples/AppRequest.html>)

[ReplicableRequest.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/interfaces/ReplicableRequest.java>)
 [[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/reconfiguration/interfaces/ReplicableRequest.html>)

[ClientRequest.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/gigapaxos/interfaces/ClientRequest.java>)
 [[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/gigapaxos/interfaces/ClientRequest.html>)

[ReconfigurableAppClientAsync.java](<https://github.com/MobilityFirst/gigapaxos/blob/master/src/edu/umass/cs/reconfiguration/ReconfigurableAppClientAsync.java>)
[[doc]](<https://mobilityfirst.github.io/gigapaxos/doc/edu/umass/cs/reconfiguration/ReconfigurableAppClientAsync.html>)
