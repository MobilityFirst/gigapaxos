# Getting started with XDN

> This page shows you how to deploy a blackbox stateful service on an existing XDN provider.

1. Get the client binary, `xdn`, by cloning our repository.
   ```
   git clone https://github.com/fadhilkurnia/xdn
   cd xdn/bin/
   export PATH=$(pwd):$PATH
   ```
   Then check that you can successfully run the client binary.
   ```
   xdn --help
   ```
   > We use x86 Linux machine by default. 
   > For ARM64 machine, such as MacBook with Apple Silicon, go to `xdn/bin/arm64` directory instead.
2. Finally, launch a blackbox stateful service on XDN. Let's use `bookcatalog` as the service name.
   ```
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

| Docker Image                      | Description                                                                                       | Example Launch Command                                           |
|-----------------------------------|---------------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| `fadhilkurnia/xdn-bookcatalog`    | Book catalog web app, storing updatable list of books.                                            | `xdn launch alice-catalog --image=fadhilkurnia/xdn-bookcatalog`  |
| `fadhilkurnia/xdn-bookcatalog-nd` | Book catalog web app that is non-deterministic because it stores the update timestamp             | `xdn launch bob-catalog --image=fadhilkurnia/xdn-bookcatalog-nd` |
| `fadhilkurnia/xdn-tpcc`           | App for a wholesale parts supplier that owns multiple warehouse, implementing the TPC-C benchmark | `xdn launch charlie-wholesale --image=fadhilkurnia/xdn-tpcc-web` |
| `fadhilkurnia/xdn-todo`           | Todo application, enabling users to list and modify their todo items                              | `xdn launch dave-todo --image=fadhilkurnia/xdn-todo`             |
| Coming soon: movie review app     |                                                                                                   |                                                                  |
| Coming soon: smallbank app        |                                                                                                   |                                                                  |
| Coming soon: e-commerce           |                                                                                                   |                                                                  |
| Coming soon: social network       |                                                                                                   |                                                                  |
| Coming soon: hotel app            |                                                                                                   |                                                                  |
| Coming soon: seats                | Stonebreaker Electronic Airline Ticketing System (SEATS)                                          |                                                                  |
| Coming soon: NoOp                 |                                                                                                   |                                                                  |
| Coming soon: YCSB                 |                                                                                                   |                                                                  |

## Deploying multi-container service
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
```
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
