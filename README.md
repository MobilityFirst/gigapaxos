# XDN - Replicating Blackbox Stateful Services to Live on the Edge

## Project structure
```
xdn/
├─ bin/                                   // binaries
├─ cli/                                   // xdn cli for developers
├─ conf/                                  // configuration
├─ dns/                                   // nameserver for xdn
├─ docs/
├─ lib/
├─ services/
├─ src/
│  ├─ edu.umass.cs
│  │  ├─ reconfigurator/
│  │  │  ├─ reconfiguration/
│  │  │  │  ├─ ReconfigurableNode.java     // the main() method for a Node
├─ tests/
├─ build.xml
├─ LICENSE.txt
├─ README.md
```

## System requirements
We developed and tested XDN on rs630 machines in the CloudLab, generally with
the following system requirements:
- Linux 5.15 or newer with x86-64 architecture
- libfuse3
- Java 21+
- docker 26+, accessible without `sudo`

<a name="getting-started"></a>
## Getting started

1. Pull XDN source code:
    ```
   git pull https://github.com/fadhilkurnia/gigapaxos.git
   cd gigapaxos
   git checkout fadhil-dist-deploy
    ```
2. Compile XDN source code:
    ```
   ant jar
    ```
3. Make symbolic link of XDN's binaries to `/usr/bin/`:
    ```
   # xdn's filesystem and its statediff applicator:
   sudo ln -s "$PWD"/bin/fuselog /usr/bin/fuselog
   sudo ln -s "$PWD"/bin/fuselog-apply /usr/bin/fuselog-apply
   
   # xdn's cli:
   sudo ln -s "$PWD"/bin/xdn /usr/bin/xdn
    ``` 

## Single machine deployment

1. Start the reconfigurator and active replicas, using the default configuration
   (`./conf/gigapaxos.properties`):
    ```
   sudo ./bin/gpServer.sh start all
    ```
   Which will start these 4 local servers:
    - 1 reconfigurator at localhost:3000
    - 3 active replicas at localhost:2000, localhost:2001, and localhost:2002

2. Prepare the containerized service, `tpcc-web`, that we will launch.
    ```
   tar -xf ./bin/tpcc-web.tar.gz -C ./bin/
   docker load --input ./bin/tpcc-web.tar
    ```
   Make sure that the image is registered in Docker's local registry:
    ```
   $ docker images
    REPOSITORY       TAG       IMAGE ID       CREATED      SIZE
    tpcc-web         latest    2fbddacd86c8   4 days ago   191MB
    ```

3. With `xdn` command line, launch the containerized service:
    ```
   xdn launch tpcc-web --image=tpcc-web --port=8000 --state=/app/data/
    ```
   Which will result in the following output, if success:
    ```
   Launching tpcc-web service with the following configuration:
     docker image  : tpcc-web
     http port     : 8000
     consistency   : linearizability
     deterministic : false
     state dir     : /app/data/
   
   The service is successfully launched 🎉🚀
   Access your service at the following permanent URL:
     > http://tpcc-web.xdn.io/     (unimplemented for now)
   
   
   Retrieve the service's replica locations with this command:
     xdn service info tpcc-web
   Destroy the replicated service with this command:
     xdn service destroy tpcc-web
    ```

   We can also verify that the replicated services run in the background using `docker ps`.


4. Access the replicated services in the active replicas:
    ```
   # access from the first active replica:
   curl -v http://localhost:2300/ -H "XDN: tpcc-web"
   
   # access from the second active replica:
   curl -v http://localhost:2301/ -H "XDN: tpcc-web"
   
   # access from the third active replica:
   curl -v http://localhost:2302/ -H "XDN: tpcc-web"
    ```

5. (Optional) Update local host so we can access the web service via xdn domain:
    ```
   sudo vim /etc/hosts
    ```
    ```
   # /etc/hosts file
    127.0.0.1       tpcc-web.ar0.xdn.io
    127.0.0.1       tpcc-web.ar1.xdn.io
    127.0.0.1       tpcc-web.ar2.xdn.io
    ```
   Then you can access the replicated web service with those host, without XDN custom header:
    ```
   curl -v http://tpcc-web.ar0.xdn.io:2300/
   curl -v http://tpcc-web.ar1.xdn.io:2301/
   curl -v http://tpcc-web.ar2.xdn.io:2301/
    ```

> To stop xdn, we need to stop the reconfigurator and active replicas, unmount the filesystem,
> and clean the state:
> ```
>  sudo ./bin/gpServer.sh forceclear all && sudo rm -rf /tmp/gigapaxos
>  sudo umount /tmp/xdn/state/fuselog/mnt/<active-replica-id>/<service-name>/
>  sudo rm -rf /tmp/xdn
> ```


## Distributed deployment

Assuming we have 4 machines with the following IP address:
- `10.10.1.1` for the first active replica.
- `10.10.1.2` for the second active replica.
- `10.10.1.3` for the third active replica.
- `10.10.1.5` for the reconfiguration.

as what we have specified in the `./conf/gigapaxos.cloudlab.properties`.
If the machines have different IP address, you need to modify the config file.

1. Do all the instructions in the [Getting Started](#getting-started), for each of the machine.

2. Start the server in each machine.
```
# at machine 10.10.1.1:
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.cloudlab.properties start AR1

# at machine 10.10.1.2:
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.cloudlab.properties start AR2

# at machine 10.10.1.3:
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.cloudlab.properties start AR3

# at machine 10.10.1.5:
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.cloudlab.properties start RC1
```
