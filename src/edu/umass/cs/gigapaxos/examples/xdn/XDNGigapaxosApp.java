package edu.umass.cs.gigapaxos.examples.xdn;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.http.HttpActiveReplicaRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;

import java.io.*;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class XDNGigapaxosApp implements Replicable {

    private String activeReplicaID;

    private HashMap<String, Integer> activeServicePorts;

    private HttpClient serviceClient = HttpClient.newHttpClient();

    public XDNGigapaxosApp(String[] args) {
        activeReplicaID = args[args.length - 1];
        activeServicePorts = new HashMap<>();
    }

    @Override
    public boolean execute(Request request) {
        if (request instanceof HttpActiveReplicaRequest harRequest) {
            harRequest.setResponse("ok");
        }

        if (request instanceof XDNRequest xdnRequest) {
            System.out.println(">>>>> XDNGigaPaxosApp execute XDNRequest request=" + xdnRequest);
            return forwardHttpRequestToContainerizedService(xdnRequest);
        }

        return true;
    }

    private boolean forwardHttpRequestToContainerizedService(XDNRequest xdnRequest) {
        try {
            // create http request
            HttpRequest httpRequest = convertXDNRequestToHttpRequest(xdnRequest);
            if (httpRequest == null) {
                return false;
            }

            if (Objects.equals(activeReplicaID, "AR0")) {
                System.out.println(">>>>>> XDNGigapaxosApp request = " + httpRequest);
            }

            // forward request to the containerized service, and get the http response
            HttpResponse<byte[]> response = serviceClient.send(httpRequest,
                    HttpResponse.BodyHandlers.ofByteArray());

            if (Objects.equals(activeReplicaID, "AR0"))
                System.out.println(">>>>>> XDNGigapaxosApp response = " +
                        new String(response.body(), StandardCharsets.UTF_8));

            // convert the response into netty's http response
            io.netty.handler.codec.http.HttpResponse nettyHttpResponse =
                    createNettyHttpResponse(response);

            // store the response in the xdn request, later to be returned to the end client.
            xdnRequest.setHttpResponse(nettyHttpResponse);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // convertXDNRequestToHttpRequest converts Netty's HTTP request into Java's HTTP request
    private HttpRequest convertXDNRequestToHttpRequest(XDNRequest xdnRequest) {
        try {
            // preparing url to the containerized service
            String url = String.format("http://127.0.0.1:%d%s",
                    this.activeServicePorts.get(xdnRequest.getServiceName()),
                    xdnRequest.getHttpRequest().uri());

            // preparing the HTTP request body, if any
            // TODO: handle non text body, ie. file or binary data
            HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.noBody();
            if (xdnRequest.getHttpContent() != null &&
                    xdnRequest.getHttpContent().content() != null) {
                bodyPublisher = HttpRequest
                        .BodyPublishers
                        .ofString(xdnRequest.getHttpContent().content()
                                .toString(StandardCharsets.UTF_8));
            }

            // preparing the HTTP request builder
            HttpRequest.Builder httpReqBuilder = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .method(xdnRequest.getHttpRequest().method().toString(),
                            bodyPublisher);

            // preparing the HTTP headers, if any.
            // note that the code need to be run with the following flag:
            // "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host",
            // otherwise setting those restricted headers here will later trigger
            // java.lang.IllegalArgumentException, such as: restricted header name: "Host".
            if (xdnRequest.getHttpRequest().headers() != null) {
                Iterator<Map.Entry<String, String>> it = xdnRequest.getHttpRequest()
                        .headers().iteratorAsString();
                while (it.hasNext()) {
                    Map.Entry<String, String> entry = it.next();
                    httpReqBuilder.setHeader(entry.getKey(), entry.getValue());
                }
            }

            return httpReqBuilder.build();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private io.netty.handler.codec.http.HttpResponse createNettyHttpResponse(
            HttpResponse<byte[]> httpResponse) {
        // copy http headers, if any
        HttpHeaders headers = new DefaultHttpHeaders();
        for (String headerKey : httpResponse.headers().map().keySet()) {
            for (String headerVal : httpResponse.headers().allValues(headerKey)) {
                headers.add(headerKey, headerVal);
            }
        }

        // by default, we have an empty header trailing for the response
        HttpHeaders trailingHeaders = new DefaultHttpHeaders();

        // build the http response
        io.netty.handler.codec.http.HttpResponse result = new DefaultFullHttpResponse(
                getNettyHttpVersion(httpResponse.version()),
                HttpResponseStatus.valueOf(httpResponse.statusCode()),
                Unpooled.copiedBuffer(httpResponse.body()),
                headers,
                trailingHeaders
        );
        return result;
    }

    private HttpVersion getNettyHttpVersion(HttpClient.Version httpClientVersion) {
        switch (httpClientVersion) {
            case HTTP_1_1, HTTP_2 -> {
                return HttpVersion.HTTP_1_1;
            }
        }
        return HttpVersion.HTTP_1_1;
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return this.execute(request);
    }

    @Override
    public String checkpoint(String name) {
        System.out.println("BookCatalogApp - checkpoint ... name=" + name);
        return null;
    }

    @Override
    public boolean restore(String name, String state) {
        System.out.println("BookCatalogApp - restore name=" + name + " state=" + state);

        // Corner case when name is empty, which fundamentally should not happen.
        if (name == null || name.equals("")) {
            System.err.println("restore(.) is called with empty service name");
            return false;
        }

        // Case-1: gigapaxos started meta service with name == XDNGigaPaxosApp0 and state == {}
        if (name.equals(this.getClass().getSimpleName() + "0") &&
                state != null && state.equals("{}")) {
            return true;
        }

        // Case-2: initialize a brand-new service name.
        // Note that in gigapaxos, initialization is a special case of restore
        // with state == initialState. In XDN, the initialState is always started with "init:".
        // Example of the initState is "init:bookcatalog:8000:linearizable:true:/app/data",
        if (state != null && state.startsWith("init:")) {
            return initContainerizedService(name, state);
        }

        // Case-3: the actual restore, i.e., initialize service in new epoch (>0) with state
        // obtained from the latest checkpoint (possibly from different active replica).
        if (state != null && state.startsWith("checkpoint:")) {
            System.err.println("unimplemented! restore(.) with latest checkpointed state");
            return false;
        }

        // Unknown cases, should not be triggered
        System.err.println("unknown restore case, name=" + name + " state=" + state);
        return false;
    }

    private int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        Request req = XDNRequest.createFromString(stringified);

        // handle request from existing http interface
        if (req == null) {
            System.out.println("XDNGigaPaxosApp - use default request");
            try {
                req = new HttpActiveReplicaRequest(stringified.getBytes());
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        return req;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        HashSet<IntegerPacketType> packetTypes = new HashSet<>();
        packetTypes.add(XDNRequest.PacketType.XDN_SERVICE_HTTP_REQUEST);
        return packetTypes;
    }

    /**
     * initContainerizedService initializes a containerized service, in idempotent manner.
     *
     * @param serviceName  name of the to-be-initialized service.
     * @param initialState the initial state with "init:" prefix.
     * @return false if failed to initialized the service.
     */
    private boolean initContainerizedService(String serviceName, String initialState) {

        System.out.println(">>> initContainerizedService serviceName=" + serviceName + " initState=" + initialState);

        // if the service is already initialized previously, in this active replica,
        // then stop and remove the previous service.
        if (activeServicePorts.containsKey(serviceName)) {
            boolean isSuccess = false;
            isSuccess = stopContainer(serviceName);
            if (!isSuccess) {
                return false;
            }
            isSuccess = removeContainer(serviceName);
            if (!isSuccess) {
                return false;
            }
        }

        // it is possible to have the previous service still running, but the serviceName
        // does not exist in our memory, this is possible when XDN crash while docker is still
        // running.
        if (isContainerRunning(serviceName)) {
            boolean isSuccess = false;
            isSuccess = stopContainer(serviceName);
            if (!isSuccess) {
                return false;
            }
            isSuccess = removeContainer(serviceName);
            if (!isSuccess) {
                return false;
            }
        }

        // decode and validate the initial state
        String[] decodedInitialState = initialState.split(":");
        if (decodedInitialState.length < 6 || !decodedInitialState[0].equals("init")) {
            System.err.println("incorrect initial state, example of expected state is" +
                    " 'init:bookcatalog:8000:linearizable:true:/app/data'");
            return false;
        }
        String dockerImageName = decodedInitialState[1];
        String dockerPortStr = decodedInitialState[2];
        String consistencyModel = decodedInitialState[3];
        boolean isDeterministic = decodedInitialState[4].equalsIgnoreCase("true");
        String stateDir = decodedInitialState[5];
        int dockerPort = 0;
        try {
            dockerPort = Integer.parseInt(dockerPortStr);
        } catch (NumberFormatException e) {
            System.err.println("incorrect docker port: " + e);
            return false;
        }
        int publicPort = getRandomNumber(50000, 65000);

        // actually start the containerized service, via command line
        boolean isSuccess = startContainer(serviceName, dockerImageName, publicPort, dockerPort);
        if (!isSuccess) {
            return false;
        }

        // store the service's public port for request forwarding.
        activeServicePorts.put(serviceName, publicPort);

        return true;
    }

    private boolean isContainerRunning(String containerName) {
        // TODO: implement me
        return false;
    }


    /**
     * startContainer runs the bash command below to start running a docker container.
     * TODO: use the stateDir argument.
     */
    private boolean startContainer(String serviceName, String imageName,
                                   int publicPort, int internalPort) {
        try {
            String command = String.format("docker run -d --name=%s.%s.xdn.io -p %d:%d %s",
                    serviceName, activeReplicaID, publicPort, internalPort, imageName);
            System.out.println(">>>> running command " + command);
            Process process = new ProcessBuilder(command.split("\\s+")).start();

            // read the output of the command
            InputStream inputStream = process.getInputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }

            // read the error of the command
            InputStream errInputStream = process.getErrorStream();
            BufferedReader errBufferedReader = new BufferedReader(
                    new InputStreamReader(errInputStream));
            while ((line = errBufferedReader.readLine()) != null) {
                System.out.println(line);
            }

            // get the process exit code
            int exitCode = process.waitFor();
            System.out.println("exit code: " + exitCode);
            return true;

        } catch (IOException e) {
            System.out.println("failed to run command: " + e);
            return false;
        } catch (InterruptedException e) {
            System.out.println("docker run process is interrupted: " + e);
            return false;
        }
    }

    private boolean stopContainer(String serviceName) {
        try {
            String command = String.format("docker container stop %s.%s.xdn.io",
                    serviceName, activeReplicaID);
            System.out.println(">>>> running command " + command);
            Process process = new ProcessBuilder(command.split("\\s+")).start();

            // read the output of the command
            InputStream inputStream = process.getInputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }

            // read the error of the command
            InputStream errInputStream = process.getErrorStream();
            BufferedReader errBufferedReader = new BufferedReader(
                    new InputStreamReader(errInputStream));
            while ((line = errBufferedReader.readLine()) != null) {
                System.out.println(line);
            }

            // get the process exit code
            int exitCode = process.waitFor();
            System.out.println("exit code: " + exitCode);

            return true;
        } catch (IOException e) {
            System.out.println("failed to run command: " + e);
            return false;
        } catch (InterruptedException e) {
            System.out.println("docker stop process is interrupted: " + e);
            return false;
        }
    }

    private boolean removeContainer(String serviceName) {
        try {
            String command = String.format("docker container rm %s.%s.xdn.io",
                    serviceName, activeReplicaID);
            System.out.println(">>>> running command " + command);
            Process process = new ProcessBuilder(command.split("\\s+")).start();

            // read the output of the command
            InputStream inputStream = process.getInputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }

            // read the error of the command
            InputStream errInputStream = process.getErrorStream();
            BufferedReader errBufferedReader = new BufferedReader(
                    new InputStreamReader(errInputStream));
            while ((line = errBufferedReader.readLine()) != null) {
                System.out.println(line);
            }

            // get the process exit code
            int exitCode = process.waitFor();
            System.out.println("exit code: " + exitCode);

            return true;
        } catch (IOException e) {
            System.out.println("failed to run command: " + e);
            return false;
        } catch (InterruptedException e) {
            System.out.println("docker stop process is interrupted: " + e);
            return false;
        }
    }


}
