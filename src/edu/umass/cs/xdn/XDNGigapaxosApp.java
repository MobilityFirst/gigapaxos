package edu.umass.cs.xdn;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.reconfiguration.http.HttpActiveReplicaRequest;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.ZipFiles;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;

import java.io.*;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class XDNGigapaxosApp implements Replicable, Reconfigurable, BackupableApplication {

    private final boolean IS_USE_FUSE = false;

    private String activeReplicaID;

    private HashMap<String, Integer> activeServicePorts;
    private ConcurrentHashMap<String, XDNServiceProperties> serviceProperties;

    private HttpClient serviceClient = HttpClient.newHttpClient();

    public XDNGigapaxosApp(String[] args) {
        activeReplicaID = args[args.length - 1].toLowerCase();
        activeServicePorts = new HashMap<>();
        serviceProperties = new ConcurrentHashMap<>();

        // TODO: check if FUSE program is available
    }

    @Override
    public boolean execute(Request request) {
        System.out.println(">> " + this.activeReplicaID + " XDNApp execution:   " + request.getClass().getSimpleName());

        if (request instanceof HttpActiveReplicaRequest harRequest) {
            harRequest.setResponse("ok");
        }

        if (request instanceof XDNRequest.XDNStatediffApplyRequest xdnRequest) {
            return applyStatediff(request.getServiceName(), xdnRequest.getStatediff());
        }

        if (request instanceof XDNRequest xdnRequest) {
            return forwardHttpRequestToContainerizedService(xdnRequest);
        }

        // TODO: handle statediff in backups where execute is simply applying the statediff

        return true;
    }

    private boolean forwardHttpRequestToContainerizedService(XDNRequest xdnRequest) {
        try {
            // create http request
            HttpRequest httpRequest = convertXDNRequestToHttpRequest(xdnRequest);
            if (httpRequest == null) {
                return false;
            }

            // forward request to the containerized service, and get the http response
            HttpResponse<byte[]> response = serviceClient.send(httpRequest,
                    HttpResponse.BodyHandlers.ofByteArray());

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
        // TODO: upgrade netty that support HTTP2
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

        // A corner case when name is empty, which fundamentally should not happen.
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
        // with state == initialState. In XDN, the initialState is always started with "xdn:init:".
        // Example of the initState is "xdn:init:bookcatalog:8000:linearizable:true:/app/data",
        if (state != null && state.startsWith("xdn:init:")) {
            return initContainerizedService(name, state);
        }

        // Case-3: the actual restore, i.e., initialize service in new epoch (>0) with state
        // obtained from the latest checkpoint (possibly from different active replica).
        if (state != null && state.startsWith("xdn:checkpoint:")) {
            // TODO: implement me
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
        System.out.println(">>> createFromString " + stringified);
        if (stringified.startsWith(XDNRequest.XDNStatediffApplyRequest.
                XDN_PREFIX_STATEDIFF_REQUEST)) {
            return XDNRequest.XDNStatediffApplyRequest.createFromString(stringified);
        }

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

        // ecode and validate the initial state
        String[] decodedInitialState = initialState.split(":");
        if (decodedInitialState.length < 7 || !initialState.startsWith("xdn:init:")) {
            System.err.println("incorrect initial state, example of expected state is" +
                    " 'xdn:init:bookcatalog:8000:linearizable:true:/app/data'");
            return false;
        }
        String dockerImageNames = decodedInitialState[2];
        String dockerPortStr = decodedInitialState[3];
        String consistencyModel = decodedInitialState[4];
        boolean isDeterministic = decodedInitialState[5].equalsIgnoreCase("true");
        String stateDir = decodedInitialState[6];
        int dockerPort = 0;
        try {
            dockerPort = Integer.parseInt(dockerPortStr);
        } catch (NumberFormatException e) {
            System.err.println("incorrect docker port: " + e);
            return false;
        }

        // TODO: assign port systematically to avoid port conflict
        int publicPort = getRandomNumber(50000, 65000);

        XDNServiceProperties prop = new XDNServiceProperties();
        prop.serviceName = serviceName;
        prop.dockerImages.addAll(List.of(dockerImageNames.split(",")));
        prop.exposedPort = dockerPort;
        prop.consistencyModel = consistencyModel;
        prop.isDeterministic = isDeterministic;
        prop.stateDir = stateDir;
        prop.mappedPort = publicPort;

        // actually start the containerized service, via command line
        boolean isSuccess = startContainer(prop);
        if (!isSuccess) {
            return false;
        }

        // store the service's public port for request forwarding.
        activeServicePorts.put(serviceName, publicPort);
        serviceProperties.put(serviceName, prop);

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
    private boolean startContainer(XDNServiceProperties properties) {

        if (IS_USE_FUSE) {
            return startContainerWithFSMount(properties);
        }

        String containerName = String.format("%s.%s.xdn.io",
                properties.serviceName, this.activeReplicaID);
        String startCommand = String.format("docker run -d --name=%s -p %d:%d %s",
                containerName, properties.mappedPort, properties.exposedPort,
                properties.dockerImages.get(0));
        int exitCode = runShellCommand(startCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to start container");
            return false;
        }

        return true;
    }

    private boolean startContainerWithFSMount(XDNServiceProperties properties) {
        String containerName = String.format("%s.%s.xdn.io",
                properties.serviceName, this.activeReplicaID);

        // remove previous directory, if exist
        String stateDirPath = String.format("/tmp/xdn/state/%s/", containerName);
        String cleanupCommand = String.format("rm -rf %s", stateDirPath);
        int exitCode = runShellCommand(cleanupCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to remove previous state directory");
            return false;
        }

        // prepare state directory
        String mkdirCommand = String.format("mkdir -p %s", stateDirPath);
        exitCode = runShellCommand(mkdirCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to create state directory");
            return false;
        }

        // TODO: copy initial state from the image into the host directory

        // prepare socket file for the filesystem
        String fsSocketPath = "/tmp/xdn/fuselog/socket/";
        mkdirCommand = String.format("mkdir -p %s", fsSocketPath);
        exitCode = runShellCommand(mkdirCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to create socket directory");
            return false;
        }

        // TODO: mount the filesystem
        String mountCommand = String.format("fuselog --socket=%s%s %s",
                fsSocketPath, containerName, containerName);
        exitCode = runShellCommand(mountCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to mount filesystem");
            return false;
        }

        // start the docker container
        String startCommand = String.format("docker run -d --name=%s -p %d:%d " +
                        "--mount type=bind,source=%s,target=%s %s",
                containerName, properties.exposedPort, properties.mappedPort,
                stateDirPath, properties.stateDir, properties.dockerImages.get(0));
        exitCode = runShellCommand(startCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to start container");
            return false;
        }

        return true;
    }

    private boolean stopContainer(String serviceName) {
        String containerName = String.format("%s.%s.xdn.io", serviceName, this.activeReplicaID);
        String stopCommand = String.format("docker container stop %s", containerName);

        int exitCode = runShellCommand(stopCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to stop container");
            return false;
        }

        return true;
    }

    private boolean removeContainer(String serviceName) {
        String containerName = String.format("%s.%s.xdn.io", serviceName, this.activeReplicaID);
        String removeCommand = String.format("docker container rm %s", containerName);

        int exitCode = runShellCommand(removeCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to remove container");
            return false;
        }

        return true;
    }

    private String copyContainerDirectory(String serviceName) {
        // gather the required service properties
        XDNServiceProperties serviceProperty = serviceProperties.get(serviceName);
        String containerName = String.format("%s.%s.xdn.io", serviceName, this.activeReplicaID);
        String statediffDirPath = String.format("/tmp/xdn/statediff/%s", containerName);
        String statediffZipPath = String.format("/tmp/xdn/zip/%s.zip", containerName);

        // create /tmp/xdn/statediff/ and /tmp/xdn/zip/ directories, if needed
        createXDNStatediffDirIfNotExist();

        // remove previous statediff, if any
        int exitCode = runShellCommand(String.format("rm -rf %s", statediffDirPath), false);
        if (exitCode != 0) {
            System.err.println("failed to remove previous statediff");
            return null;
        }

        // remove previous statediff archive, if any
        exitCode = runShellCommand(String.format("rm -rf %s", statediffZipPath), false);
        if (exitCode != 0) {
            System.err.println("failed to remove previous statediff archive");
            return null;
        }

        // get the statediff into statediffDirPath
        String command = String.format("docker cp %s:%s %s",
                containerName,
                serviceProperty.stateDir + "/.",
                statediffDirPath + "/");
        exitCode = runShellCommand(command, false);
        if (exitCode != 0) {
            System.err.println("failed to copy statediff");
            return "null";
        }

        // archive the statediff into statediffZipPath
        ZipFiles.zipDirectory(new File(statediffDirPath), statediffZipPath);

        // convert the archive into String
        try {
            byte[] statediffBytes = Files.readAllBytes(Path.of(statediffZipPath));
            return String.format("xdn:statediff:%s:%s",
                    this.activeReplicaID,
                    new String(statediffBytes, StandardCharsets.ISO_8859_1));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private int runShellCommand(String command) {
        return runShellCommand(command, true);
    }

    private int runShellCommand(String command, boolean isSilent) {
        try {
            // prepare to start the command
            ProcessBuilder pb = new ProcessBuilder(command.split("\\s+"));
            if (isSilent) {
                pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
                pb.redirectError(ProcessBuilder.Redirect.DISCARD);
            }

            if (!isSilent) {
                System.out.println("command: " + command);
            }

            // run the command as a new OS process
            Process process = pb.start();

            // print out the output in stderr, if needed
            if (!isSilent) {
                InputStream errInputStream = process.getErrorStream();
                BufferedReader errBufferedReader = new BufferedReader(
                        new InputStreamReader(errInputStream));
                String line;
                while ((line = errBufferedReader.readLine()) != null) {
                    System.out.println(line);
                }
            }

            int exitCode = process.waitFor();

            if (!isSilent) {
                System.out.println("exit code: " + exitCode);
            }

            return exitCode;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void createXDNStatediffDirIfNotExist() {
        try {
            String statediffDirPath = "/tmp/xdn";
            Files.createDirectories(Paths.get(statediffDirPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            String statediffDirPath = "/tmp/xdn/statediff";
            Files.createDirectories(Paths.get(statediffDirPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            String statediffDirPath = "/tmp/xdn/zip";
            Files.createDirectories(Paths.get(statediffDirPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String createXDNStateDirrIfNotExist(String containerName) {
        try {
            String xdnWorkdirPath = "/tmp/xdn";
            Files.createDirectories(Paths.get(xdnWorkdirPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            String statedirPath = "/tmp/xdn/state";
            Files.createDirectories(Paths.get(statedirPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            String containerStatedirPath = String.format("/tmp/xdn/state/%s", containerName);
            Files.createDirectories(Paths.get(containerStatedirPath));
            return containerStatedirPath;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**********************************************************************************************
     *             Begin implementation methods for BackupableApplication interface               *
     *********************************************************************************************/

    @Override
    public String captureStatediff(String serviceName) {
        return copyContainerDirectory(serviceName);
    }

    @Override
    public boolean applyStatediff(String serviceName, String statediff) {
        System.out.println(">>>>>> applying statediff: " + serviceName + " " + statediff);

        // validate the statediff
        if (statediff == null || !statediff.startsWith("xdn:statediff:")) {
            System.out.println("invalid XDN statediff format, ignoring it");
            return false;
        }

        // get the primary ID, ignore if I'm the primary
        String suffix = statediff.substring("xdn:statediff:".length());
        int primaryIDEndIdx = suffix.indexOf(":");
        String primaryID = suffix.substring(0, primaryIDEndIdx);
        if (primaryID.equals(activeReplicaID)) {
            System.out.println(">> I'm the coordinator, ignoring statediff. coordinatorID=" + primaryID + " myID=" + activeReplicaID);
            return true;
        }

        // gather the required service properties
        XDNServiceProperties serviceProperty = serviceProperties.get(serviceName);
        String containerName = String.format("%s.%s.xdn.io", serviceName, this.activeReplicaID);
        String statediffDirPath = String.format("/tmp/xdn/statediff/%s", containerName);
        String statediffZipPath = String.format("/tmp/xdn/zip/%s.zip", containerName);

        // remove previous statediff archive, if any
        int exitCode = runShellCommand(String.format("rm -rf %s", statediffZipPath), false);
        if (exitCode != 0) {
            return false;
        }

        // convert the String statediff back to archive
        String statediffString = suffix.substring(primaryIDEndIdx + 1);
        try (FileOutputStream fos = new FileOutputStream(statediffZipPath)) {
            fos.write(statediffString.getBytes(StandardCharsets.ISO_8859_1));
            fos.flush();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        // remove previous statediff, if any
        exitCode = runShellCommand(String.format("rm -rf %s", statediffDirPath), false);
        if (exitCode != 0) {
            return false;
        }

        // unarchive the statediff
        ZipFiles.unzip(statediffZipPath, statediffDirPath);

        // copy back the statediff into the service container
        String copyCommand = String.format("docker cp %s %s:%s",
                statediffDirPath + "/.", containerName, serviceProperty.stateDir);
        exitCode = runShellCommand(copyCommand, false);
        if (exitCode != 0) {
            return false;
        }

        // restart the service container
        String restartCommand = String.format("docker container restart %s", containerName);
        exitCode = runShellCommand(restartCommand, false);
        if (exitCode != 0) {
            return false;
        }

        return true;
    }

    /**********************************************************************************************
     *             End of implementation methods for BackupableApplication interface              *
     *********************************************************************************************/


    /**********************************************************************************************
     *                  Begin implementation methods for Replicable interface                     *
     *********************************************************************************************/

    private static class XDNStopRequest implements ReconfigurableRequest {

        private final String serviceName;
        private final int epochNumber;

        public XDNStopRequest(String serviceName, int epochNumber) {
            this.serviceName = serviceName;
            this.epochNumber = epochNumber;
        }

        @Override
        public IntegerPacketType getRequestType() {
            return ReconfigurableRequest.STOP;
        }

        @Override
        public String getServiceName() {
            return serviceName;
        }

        @Override
        public int getEpochNumber() {
            return epochNumber;
        }

        @Override
        public boolean isStop() {
            return true;
        }
    }

    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        System.out.println(">> XDNGigapaxosApp - getStopRequest name:" + name + " epoch:" + epoch);
        return new XDNStopRequest(name, epoch);
    }

    @Override
    public String getFinalState(String name, int epoch) {
        // TODO: implement me
        System.out.println(">> XDNGigapaxosApp - getFinalState name:" + name + " epoch:" + epoch);
        return "";
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {
        // TODO: implement me
        System.out.println(">> XDNGigapaxosApp - putInitialState name:" + name + " epoch:" + epoch + " state:" + state);
        return;
    }

    @Override
    public boolean deleteFinalState(String name, int epoch) {
        // TODO: implement me
        System.out.println(">> XDNGigapaxosApp - deleteFinalState name:" + name + " epoch:" + epoch);
        return true;
    }

    @Override
    public Integer getEpoch(String name) {
        // TODO: implement me
        System.out.println(">> XDNGigapaxosApp - getEpoch name:" + name);
        return 0;
    }

    /**********************************************************************************************
     *                   End implementation methods for Replicable interface                      *
     *********************************************************************************************/

}
