# Getting started with XDN

!!! info
    This page explains how to deploy a blackbox stateful service on an existing XDN provider.

## Deploy a blackbox service

First, get the client binary, `xdn`, by cloning our Github repository.

``` sh
git clone https://github.com/fadhilkurnia/xdn
cd xdn/bin/
export PATH=$(pwd):$PATH
```

Then check that you can successfully run the client binary.
``` sh
xdn --help
```
   
!!! tip
    We use x86 Linux machine by default. 
    For ARM64 machine, such as MacBook with Apple Silicon, go to `xdn/bin/arm64` directory instead.

Finally, launch a blackbox stateful service on XDN. Let's use `bookcatalog` as the service name.
``` sh
xdn launch bookcatalog \
   --image=fadhilkurnia/xdn-bookcatalog \ 
   --port=80 \
   --consistency=linearizable \
   --deterministic=true \
   --state=/app/data/
```
If successful, you will see the following output below, then you can access the stateful service by visiting
http://bookcatalog.xdnapp.com/.
   ```
   Launching bookcatalog service with the following configuration:
     docker image  : fadhilkurnia/xdn-bookcatalog
     http port     : 80
     consistency   : linearizable
     deterministic : true
     state dir     : /app/data/
   
   The service is successfully launched 🎉🚀
   Access your service at the following permanent URL:
     > http://bookcatalog.xdnapp.com/
   
   
   Retrieve the service's replica locations with this command:
     xdn service info bookcatalog
   Destroy the replicated service with this command:
     xdn service destroy bookcatalog
   ```

Let's dechiper what just happened when we deploy a stateful service with the command above.

- **Blackbox Service.** XDN handles arbitrary stateful service, the `--image` specifies the Docker image of the
  containerized service. XDN doesn't need to know how and with what programming language the service was implemented.
- **HTTP Interface.** XDN acts as proxy for all incoming HTTP request, so XDN can coordinate the requests among the 
  replicas. The `--port` option specifies the port where the service listen to incoming HTTP requests. When unspecified,
  XDN asssumes the default HTTP port of 80.
- **Service Properties**. The `--deterministic` option specifies whether the web service is deterministic or not. Other
  than determinism, XDN allows developer to specify other properties of the service and its requests so XDN can use an
  optimized replication protocol, depending on the service's properties.
- **Consistency Model.** The `--consistency` option specifies the consistency model the developer wants for the
  replicated service. The default value is `linearizable`. Check out [this page](3-flexible-consistency.md) to see how
  to use different consistency model.
- **State Directory.** The `--state` option specifies the directory where the web service stores its state. For example,
  it is commonly `/var/lib/mysql` in MySQL and `/var/lib/pgsql/data` in PostgreSQL. When not specified, XDN will 
  snapshot the entire data in the container `/`. 
- **XDN Provider.** Here, we are using an existing XDN provider, accessible at `xdnapp.com`. You can use another XDN
  Provider using `--control-plane=<control_plane_url>` option. Alternatively, you can be your own XDN Provider! check
  out [this page](2-become-operator.md).

## Other example services

Other than the `fadhilkurnia/xdn-bookcatalog` Docker image that we use previously, we have prepared Docker images for
other stateful services, as can be seen below.

<table>
<tr>
	<th>Docker Image</th>
    <th>Description</th>
    <th>Example Launch Command</th>
</tr>
<tr>
    <td>fadhilkurnia/xdn-bookcatalog</td>
    <td>Book catalog web app, storing updatable list of books. <br>Tech: Go, SQLite.</td>
    <td>
      ``` bash
      xdn launch alice-catalog \
           --image=fadhilkurnia/xdn-bookcatalog \
           --state=/data/ \
           --deterministic
      ```
    </td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-bookcatalog-nd</td>
    <td>Book catalog web app that is non-deterministic because it stores the update timestamp. <br>Tech: Go, SQLite.</td>
    <td>
      ``` bash
      xdn launch bob-catalog \
          --image=fadhilkurnia/xdn-bookcatalog-nd \
          --state=/data/
      ```
    </td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-tpcc</td>
    <td>App for a wholesale parts supplier that owns multiple warehouse, implementing <br>the TPC-C benchmark. <br>Tech: Python, SQLite.</td>
    <td>
      ``` bash
      xdn launch charlie-tpcc \
          --image=fadhilkurnia/xdn-tpcc \
          --state=/app/data/
      ```
    </td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-todo</td>
    <td>Todo application, enabling users to list and modify <br>their todo items. <br>Tech: NodeJS, SQLite.</td>
    <td>
      ``` bash
      xdn launch charlie-tpcc \
          --image=fadhilkurnia/xdn-todo \
          --state=/home/node/app/var/db
      ```
    </td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: <br>movie review app</td>
    <td></td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: <br>smallbank app</td>
    <td></td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: <br>e-commerce</td>
    <td></td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: <br>social network app</td>
    <td></td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: <br>hotel reservation app</td>
    <td></td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: SEATS<br>Stonebreaker Electronic<br>Airline Ticketing System (SEATS)</td>
    <td></td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: <br>KV App (YCSB)</td>
    <td></td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: <br>NoOp</td>
    <td></td>
</tr>

</table>

## Deploy a multi-container service
Some stateful service have multiple containers, typically having frontend, backend, and database in their own container.

XDN supports deployment of stateful service with multiple containers. Developers need to specify the properties of
the service, including the containers of that service. 
An example of that service properties declaration is shown below, in `wordpress.yaml` file.
```yaml
# wordpress.yaml
---
name: myblog
components:
   - wordpress:
        image: wordpress:6.5.4-fpm-alpine
        port: 80
        entry: true
        environments:
           - WORDPRESS_CONFIG_EXTRA:
                define('FORCE_SSL_ADMIN', false);
                define('FORCE_SSL_LOGIN', false);
   - database:
        image: mysql:8.4.0
        expose: 3306
        stateful: true
        environments:
           - MYSQL_ROOT_PASSWORD: supersecret
deterministic: false
state: database:/var/lib/mysql/
consistency: linearizability
```

Then, to deploy that multi-container service, use the following command:
``` bash
xdn launch myblog --file=wordpress.yaml
```
If successful, you wil see the following output.
```
Launching bookcatalog service with the following configuration:
  docker image  : wordpress:6.5.4-fpm-alpine,mysql:8.4.0
  http port     : 80
  consistency   : linearizable
  deterministic : false
  state dir     : database:/var/lib/mysql/

The service is successfully launched 🎉🚀
Access your service at the following permanent URL:
  > http://myblog.xdnapp.com/


Retrieve the service's replica locations with this command:
  xdn service info myblog
Destroy the replicated service with this command:
  xdn service destroy myblog
```

Limitations:

- XDN only supports at most one stateful container. Multiple stateful container requires snapshot transaction support,
  currently unimplemented.
- XDN only supports at most one entry container. Supporting multiple entry containers require developer to declare the
  ports of all those entry containers, making the specification more complex. Currently, this feature is not a priority
  for XDN.
