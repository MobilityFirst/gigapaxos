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
import edu.umass.cs.xdn.request.*;
import edu.umass.cs.xdn.service.InitializedService;
import edu.umass.cs.xdn.service.ServiceComponent;
import edu.umass.cs.xdn.service.ServiceProperty;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.json.JSONException;

import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class XDNGigapaxosApp implements Replicable, Reconfigurable, BackupableApplication {

    private final boolean IS_USE_FUSE = false;
    private final String FUSELOG_BIN_PATH = "/users/fadhil/fuse/fuselog";
    private final String FUSELOG_APPLY_BIN_PATH = "/users/fadhil/fuse/apply";

    private String activeReplicaID;

    private HashMap<String, Integer> activeServicePorts;

    // FIXME: need to check
    //  (1) multicontainer with primary-backup, how to restart?
    //  (2) multicontainer with primary-backup via FUSE
    //  (3) multicontainer with paxos / active replication
    // private ConcurrentHashMap<String, XDNServiceProperties> serviceProperties;

    private final ConcurrentHashMap<String, InitializedService> services;
    private final HashMap<String, SocketChannel> fsSocketConnection;
    private final HashMap<String, Boolean> isServiceActive;
    private final HttpClient serviceClient = HttpClient.newHttpClient();

    public XDNGigapaxosApp(String[] args) {
        activeReplicaID = args[args.length - 1].toLowerCase();
        activeServicePorts = new HashMap<>();
        services = new ConcurrentHashMap<>();
        fsSocketConnection = new HashMap<>();
        isServiceActive = new HashMap<>();

        if (IS_USE_FUSE) {
            // validate the operating system as currently FUSE is only supported on Linux
            String osName = System.getProperty("os.name");
            if (!osName.equalsIgnoreCase("linux")) {
                throw new RuntimeException("Error: FUSE can only be used in Linux");
            }

            var fuselogBinary = new File(FUSELOG_BIN_PATH);
            var fuselogApplyBinary = new File(FUSELOG_APPLY_BIN_PATH);
            assert fuselogBinary.exists() && fuselogApplyBinary.exists();
        }

    }

    @Override
    public boolean execute(Request request) {
        System.out.println(">> " + this.activeReplicaID + " XDNApp execution:   " + request.getClass().getSimpleName());
        String serviceName = request.getServiceName();

        if (request instanceof HttpActiveReplicaRequest harRequest) {
            harRequest.setResponse("ok");
            return true;
        }

        if (request instanceof XDNStatediffApplyRequest xdnRequest) {
            return applyStatediff(serviceName, xdnRequest.getStatediff());
        }

        if (request instanceof XDNHttpRequest xdnRequest) {
            return forwardHttpRequestToContainerizedService(xdnRequest);
        }

        System.out.println("Error: executing unknown request type " +
                request.getClass().getSimpleName());

        return true;
    }

    private void deactivate(String serviceName) {
        System.out.println(">> " + activeReplicaID + " deactivate...");

        try {
            // without FUSE (i.e., using Zip archive, we need the service container to be running
            if (!IS_USE_FUSE) {
                return;
            }

            // stop container
            boolean isSuccess = stopContainer(serviceName);
            if (!isSuccess) {
                System.err.println("failed to deactivate active container");
                return;
            }

            // disconnect filesystem socket
            SocketChannel socketChannel = fsSocketConnection.get(serviceName);
            if (socketChannel != null) {
                socketChannel.close();
                fsSocketConnection.remove(serviceName);
            }

            // unmount filesystem
            String containerName = String.format("%s.%s.xdn.io", serviceName, this.activeReplicaID);
            String stateDirPath = String.format("/tmp/xdn/fuselog/state/%s/", containerName);
            String unmountCommand = String.format("umount %s", stateDirPath);
            int errCode = runShellCommand(unmountCommand, false);
            if (errCode != 0) {
                System.err.println("failed to unmount filesystem");
                return;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void activate(String serviceName) {
        System.out.println(">> " + activeReplicaID + " activate...");

        // without FUSE (i.e., using Zip archive, we need the service container to be running
        if (!IS_USE_FUSE) {
            return;
        }

        // mount the filesystem
        String containerName = String.format("%s.%s.xdn.io", serviceName, this.activeReplicaID);
        String stateDirPath = String.format("/tmp/xdn/fuselog/state/%s/", containerName);
        String fsSocketDir = "/tmp/xdn/fuselog/socket/";
        String fsSocketFile = String.format("%s%s.sock", fsSocketDir, containerName);
        String mountCommand = String.format("%s -o allow_other -f -s %s", FUSELOG_BIN_PATH, stateDirPath);
        var t = new Thread() {
            public void run() {
                Map<String, String> envVars = new HashMap<>();
                envVars.put("FUSELOG_SOCKET_FILE", fsSocketFile);
                int exitCode = runShellCommand(mountCommand, false, envVars);
                if (exitCode != 0) {
                    System.err.println("failed to mount filesystem");
                    return;
                }
            }
        };
        t.start();

        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // connect into filesystem socket
        try {
            UnixDomainSocketAddress address = UnixDomainSocketAddress.of(Path.of(fsSocketFile));
            SocketChannel socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
            boolean isConnEstablished = socketChannel.connect(address);
            if (!isConnEstablished) {
                System.err.println("failed to connect to the filesystem");
                return;
            }

            fsSocketConnection.put(serviceName, socketChannel);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // start container
        String startCommand = String.format("docker start %s", containerName);
        int exitCode = runShellCommand(startCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to start container");
            return;
        }

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
        if (name == null || name.isEmpty()) {
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
            isServiceActive.put(name, true);
            return initContainerizedService2(name, state);
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

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        System.out.println(">>> createFromString " + stringified);

        // case-1: handle all xdn requests with prefix "xdn:"
        if (stringified.startsWith(XDNRequest.SERIALIZED_PREFIX)) {

            // handle a statediff request
            if (stringified.startsWith(XDNStatediffApplyRequest.SERIALIZED_PREFIX)) {
                Request r = XDNStatediffApplyRequest.createFromString(stringified);
                if (r == null) {
                    Exception e = new RuntimeException(
                            "Invalid serialized format for xdn statediff request");
                    throw new RequestParseException(e);
                }
                return r;
            }

            // handle an http request
            if (stringified.startsWith(XDNHttpRequest.SERIALIZED_PREFIX)) {
                Request r = XDNHttpRequest.createFromString(stringified);
                if (r == null) {
                    Exception e = new RuntimeException(
                            "Invalid serialized format for xdn http request");
                    throw new RequestParseException(e);
                }
                return r;
            }

            // handle a forwarded request
            if (stringified.startsWith(XDNHttpForwardRequest.SERIALIZED_PREFIX)) {
                Request r = XDNHttpForwardRequest.createFromString(stringified);
                if (r == null) {
                    Exception e = new RuntimeException(
                            "Invalid serialized format for xdn http forward request");
                    throw new RequestParseException(e);
                }
                return r;
            }

            // handle a forwarded response
            if (stringified.startsWith(XDNHttpForwardResponse.SERIALIZED_PREFIX)) {
                Request r = XDNHttpForwardResponse.createFromString(stringified);
                if (r == null) {
                    Exception e = new RuntimeException(
                            "Invalid serialized format for xdn http forward response");
                    throw new RequestParseException(e);
                }
                return r;
            }

            Exception e = new RuntimeException(
                    "Invalid serialized format for xdn request");
            throw new RequestParseException(e);
        }


        // case-2: handle the default HttpActiveReplica request (for backward compatibility)
        System.out.println("XDNGigaPaxosApp - use default request");
        Request req;
        try {
            req = new HttpActiveReplicaRequest(stringified.getBytes());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        return req;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        HashSet<IntegerPacketType> packetTypes = new HashSet<>();
        packetTypes.add(XDNRequestType.XDN_SERVICE_HTTP_REQUEST);
        packetTypes.add(XDNRequestType.XDN_STATEDIFF_APPLY_REQUEST);
        packetTypes.add(XDNRequestType.XDN_HTTP_FORWARD_REQUEST);
        packetTypes.add(XDNRequestType.XDN_HTTP_FORWARD_RESPONSE);
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

        // decode and validate the initial state
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

        // create docker network, via command line
        String networkName = String.format("net::%s:%s", activeReplicaID, prop.serviceName);
        int exitCode = createDockerNetwork(networkName);
        if (exitCode != 0) {
            return false;
        }

        // actually start the containerized service, via command line
        boolean isSuccess = startContainer(prop, networkName);
        if (!isSuccess) {
            return false;
        }

        // store the service's public port for request forwarding.
        activeServicePorts.put(serviceName, publicPort);
        // serviceProperties.put(serviceName, prop);

        return true;
    }

    /**
     * initContainerizedService2 initializes a containerized service, in idempotent manner.
     * This is a new implementation of initContainerizedService, but with support on multiple
     * components.
     *
     * @param serviceName  name of the to-be-initialized service.
     * @param initialState the initial state with "xdn:init:" prefix.
     * @return false if failed to initialize the service.
     */
    private boolean initContainerizedService2(String serviceName, String initialState) {
        String validInitialStatePrefix = "xdn:init:";

        // validate the initial state
        if (!initialState.startsWith(validInitialStatePrefix)) {
            throw new RuntimeException("invalid initial state");
        }

        // decode the initial state, containing the service property
        ServiceProperty property = null;
        String networkName = String.format("net::%s:%s", activeReplicaID, serviceName);
        int allocatedPort = getRandomPort();
        try {
            initialState = initialState.substring(validInitialStatePrefix.length());
            property = ServiceProperty.createFromJSONString(initialState);
        } catch (JSONException e) {
            throw new RuntimeException("invalid initial state as JSON: " + e);
        }

        // prepare container names for each service component
        List<String> containerNames = new ArrayList<>();
        int idx = 0;
        for (ServiceComponent c : property.getComponents()) {
            String containerName = String.format("%d.%s.%s.xdn.io",
                    idx, serviceName, activeReplicaID);
            containerNames.add(containerName);
            idx++;
        }

        // prepare the initialized service
        InitializedService service = new InitializedService(
                property,
                serviceName,
                networkName,
                allocatedPort,
                containerNames
        );

        // TODO: remove already running containers, if any

        // create docker network, via command line
        int exitCode = createDockerNetwork(networkName);
        if (exitCode != 0) {
            return false;
        }

        // TODO: prepare statediff directory, if required
        String fuseMountSource = null;
        String fuseMountTarget = null;

        // actually start the service, run each component as container,
        // in the same order as they are specified.
        idx = 0;
        for (ServiceComponent c : property.getComponents()) {
            boolean isSuccess = startContainer(
                    c.getImageName(),
                    containerNames.get(idx),
                    networkName,
                    c.getComponentName(),
                    c.getExposedPort(),
                    c.getEntryPort(),
                    c.isEntryComponent() ? allocatedPort : null,
                    IS_USE_FUSE ? fuseMountSource : null,
                    IS_USE_FUSE ? fuseMountTarget : null,
                    c.getEnvironmentVariables());
            if (!isSuccess) {
                throw new RuntimeException("failed to start container for component " +
                        c.getComponentName());
            }

            idx++;
        }

        // store all the service metadata
        services.put(serviceName, service);
        activeServicePorts.put(serviceName, allocatedPort);

        return true;
    }

    private boolean isContainerRunning(String containerName) {
        // TODO: implement me
        return false;
    }

    /**
     * startContainer runs the bash command below to start running a docker container.
     */
    private boolean startContainer(XDNServiceProperties properties, String networkName) {
        if (IS_USE_FUSE) {
            return startContainerWithFSMount(properties, networkName);
        }

        String containerName = String.format("%s.%s.xdn.io",
                properties.serviceName, this.activeReplicaID);
        String startCommand = String.format("docker run -d --name=%s --network=%s --publish=%d:%d %s",
                containerName, networkName, properties.mappedPort, properties.exposedPort,
                properties.dockerImages.get(0));
        int exitCode = runShellCommand(startCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to start container");
            return false;
        }

        return true;
    }

    private boolean startContainerWithFSMount(XDNServiceProperties properties, String networkName) {
        String containerName = String.format("%s.%s.xdn.io",
                properties.serviceName, this.activeReplicaID);

        // remove previous directory, if exist
        String stateDirPath = String.format("/tmp/xdn/fuselog/state/%s/", containerName);
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
        String fsSocketDir = "/tmp/xdn/fuselog/socket/";
        String fsSocketFile = String.format("%s%s.sock", fsSocketDir, containerName);
        mkdirCommand = String.format("mkdir -p %s", fsSocketDir);
        exitCode = runShellCommand(mkdirCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to create socket directory");
            return false;
        }

        // mount the filesystem
        String mountCommand = String.format("%s -o allow_other -f -s %s", FUSELOG_BIN_PATH, stateDirPath);
        var t = new Thread() {
            public void run() {
                Map<String, String> envVars = new HashMap<>();
                envVars.put("FUSELOG_SOCKET_FILE", fsSocketFile);
                int exitCode = runShellCommand(mountCommand, false, envVars);
                if (exitCode != 0) {
                    System.err.println("failed to mount filesystem");
                    return;
                }
            }
        };
        t.start();

        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // establish connection to the filesystem
        try {
            UnixDomainSocketAddress address = UnixDomainSocketAddress.of(Path.of(fsSocketFile));
            SocketChannel socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
            boolean isConnEstablished = socketChannel.connect(address);
            if (!isConnEstablished) {
                System.err.println("failed to connect to the filesystem");
                return false;
            }

            fsSocketConnection.put(properties.serviceName, socketChannel);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // start the docker container
        String startCommand = String.format("docker run -d --name=%s --network=%s --publish=%d:%d " +
                        "--mount type=bind,source=%s,target=%s %s",
                containerName, networkName, properties.mappedPort, properties.exposedPort,
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

    private int createDockerNetwork(String networkName) {
        String createNetCmd = String.format("docker network create %s",
                networkName);
        int exitCode = runShellCommand(createNetCmd, false);
        if (exitCode != 0 && exitCode != 1) {
            // 1 is the exit code of creating already exist network
            System.err.println("Error: failed to create network");
            return exitCode;
        }
        return 0;
    }

    private String copyContainerDirectory(String serviceName) {
        InitializedService service = services.get(serviceName);
        if (service == null) {
            throw new RuntimeException("unknown service " + serviceName);
        }

        // gather the required service properties
        String serviceStatediffName = String.format("%s.%s.xdn.io", serviceName, activeReplicaID);
        String statediffDirPath = String.format("/tmp/xdn/statediff/%s", serviceStatediffName);
        String statediffZipPath = String.format("/tmp/xdn/zip/%s.zip", serviceStatediffName);

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
                service.statefulContainer,
                service.stateDirectory + "/.",
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

    private String captureStatediffWithFuse(XDNServiceProperties properties) {
        try {
            SocketChannel fsConn = fsSocketConnection.get(properties.serviceName);

            // send get statediff command (g) to the filesystem
            fsConn.write(ByteBuffer.wrap("g\n".getBytes()));

            // wait for response indicating the size of the statediff
            ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
            sizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
            sizeBuffer.clear();
            System.out.println(">> reading response ...");
            int numRead = fsConn.read(sizeBuffer);
            if (numRead < 8) {
                System.err.println("failed to read size of the statediff");
                return null;
            }
            long statediffSize = sizeBuffer.getLong(0);
            System.out.println(">> statediff size=" + statediffSize);

            // read all the statediff
            ByteBuffer statediffBuffer = ByteBuffer.allocate((int) statediffSize);
            numRead = 0;
            while (numRead < statediffSize) {
                numRead += fsConn.read(statediffBuffer);
            }
            System.out.println("complete reading statediff ...");

            return String.format("xdn:statediff:%s:%s",
                    this.activeReplicaID,
                    new String(statediffBuffer.array(), StandardCharsets.ISO_8859_1));
        } catch (IOException e) {
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

    /**********************************************************************************************
     *             Begin implementation methods for BackupableApplication interface               *
     *********************************************************************************************/

    @Override
    public String captureStatediff(String serviceName) {

        if (IS_USE_FUSE) {
            throw new RuntimeException("unimplemented :(");
            // XDNServiceProperties prop = serviceProperties.get(serviceName);
            // assert prop != null;
            // return captureStatediffWithFuse(prop);
        }

        return copyContainerDirectory(serviceName);
    }

    @Override
    public boolean applyStatediff(String serviceName, String statediff) {
        System.out.println(">>>>>> applying statediff: " + serviceName + " " + statediff);

        InitializedService service = services.get(serviceName);
        if (service == null) {
            throw new RuntimeException("unknown service " + serviceName);
        }

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
            System.out.println(">> I'm the coordinator, ignoring statediff. coordinatorID="
                    + primaryID + " myID=" + activeReplicaID);
            return true;
        }
        String statediffString = suffix.substring(primaryIDEndIdx + 1);

        if (IS_USE_FUSE) {
            // FIXME: support multicontainer in FUSE
            if (isServiceActive.get(serviceName) != null && isServiceActive.get(serviceName)) {
                deactivate(serviceName);
                isServiceActive.put(serviceName, false);
            }
            return applyStatediffWithFuse(serviceName, statediffString);
        }

        // gather the required service properties
        String serviceStatediffName = String.format("%s.%s.xdn.io", serviceName, activeReplicaID);
        String statediffDirPath = String.format("/tmp/xdn/statediff/%s", serviceStatediffName);
        String statediffZipPath = String.format("/tmp/xdn/zip/%s.zip", serviceStatediffName);

        // remove previous statediff archive, if any
        int exitCode = runShellCommand(String.format("rm -rf %s", statediffZipPath), false);
        if (exitCode != 0) {
            return false;
        }

        // convert the String statediff back to archive
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
                statediffDirPath + "/.", service.statefulContainer, service.stateDirectory);
        exitCode = runShellCommand(copyCommand, false);
        if (exitCode != 0) {
            return false;
        }

        // restart the service container
        // TODO: may need to restart all containers own by this service
        String restartCommand = String.format("docker container restart %s", service.statefulContainer);
        exitCode = runShellCommand(restartCommand, false);
        if (exitCode != 0) {
            return false;
        }

        return true;
    }

    private boolean applyStatediffWithFuse(String serviceName, String statediff) {
        // TODO: when applying statediff the service need to be stopped/paused, the filesystem need to be stopped.

        try {
            // gather the required service properties
            String containerName = String.format("%s.%s.xdn.io", serviceName, this.activeReplicaID);

            // store statediff into an external file
            runShellCommand("mkdir -p /tmp/xdn/fuselog/statediff/", false);
            String tempStatediffFilePath = String.format("/tmp/xdn/fuselog/statediff/%s", containerName);
            FileOutputStream outputStream = new FileOutputStream(tempStatediffFilePath);
            outputStream.write(statediff.getBytes(StandardCharsets.ISO_8859_1));
            outputStream.flush();
            outputStream.close();

            String stateDir = String.format("/tmp/xdn/fuselog/state/%s/", containerName);
            String applyCommand = String.format("%s %s", FUSELOG_APPLY_BIN_PATH, stateDir);
            Map<String, String> env = new HashMap<>();
            env.put("FUSELOG_STATEDIFF_FILE", tempStatediffFilePath);
            int errCode = runShellCommand(applyCommand, false, env);
            if (errCode != 0) {
                System.err.println("failed to apply statediff");
                return false;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
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
        var exceptionMessage = String.format(
                "XDNGigapaxosApp.getFinalState is unimplemented, serviceName=%s epoch=%d",
                name, epoch);
        throw new RuntimeException(exceptionMessage);
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {
        var exceptionMessage = String.format(
                "XDNGigapaxosApp.putInitialState is unimplemented, serviceName=%s epoch=%d state=%s",
                name, epoch, state);
        throw new RuntimeException(exceptionMessage);
    }

    @Override
    public boolean deleteFinalState(String name, int epoch) {
        var exceptionMessage = String.format(
                "XDNGigapaxosApp.deleteFinalState is unimplemented, serviceName=%s epoch=%d",
                name, epoch);
        throw new RuntimeException(exceptionMessage);
    }

    @Override
    public Integer getEpoch(String name) {
        // TODO: store epoch for each service
        // var exceptionMessage = String.format(
        //         "XDNGigapaxosApp.getEpoch is unimplemented, serviceName=%s", name);
        // throw new RuntimeException(exceptionMessage);

        return 0;
    }

    /**********************************************************************************************
     *                   End implementation methods for Replicable interface                      *
     *********************************************************************************************/


    /**********************************************************************************************
     *                                  Begin utility methods                                     *
     *********************************************************************************************/

    private int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    /**
     * This method tries to find available port, most of the time.
     * It is possible, in race condition, that the port deemed as available
     * is being used by others even when this method return the port.
     *
     * @return port number that is potentially available
     */
    private int getRandomPort() {
        int maxAttempt = 5;
        int port = getRandomNumber(50000, 65000);
        boolean isPortAvailable = false;

        // check if port is already used by others
        while (!isPortAvailable && maxAttempt > 0) {
            try {
                // success connection means the port is already used
                Socket s = new Socket("localhost", port);
                s.close();
                port = getRandomNumber(50000, 65000);
            } catch (IOException e) {
                // unsuccessful connection could mean that the port is available
                isPortAvailable = true;
            }
            maxAttempt--;
        }

        return port;
    }

    private boolean startContainer(String imageName, String containerName, String networkName,
                                   String hostName, Integer exposedPort, Integer publishedPort,
                                   Integer allocatedHttpPort, String mountDirSource,
                                   String mountDirTarget, Map<String, String> env) {

        String publishPortSubCmd = "";
        if (publishedPort != null && allocatedHttpPort != null) {
            publishPortSubCmd = String.format("--publish=%d:%d", allocatedHttpPort, publishedPort);
        }
        if (publishedPort != null && allocatedHttpPort == null) {
            publishPortSubCmd = String.format("--publish=%d:%d", publishedPort, publishedPort);
        }

        // Note that exposed port will be ignored if there is published port
        String exposePortSubCmd = "";
        if (exposedPort != null && publishPortSubCmd.isEmpty()) {
            exposePortSubCmd = String.format("--expose=%d", exposedPort);
        }

        String mountSubCmd = "";
        if (mountDirSource != null && !mountDirSource.isEmpty() &&
                mountDirTarget != null && !mountDirTarget.isEmpty()) {
            mountSubCmd = String.format("--mount type=bind,source=%s,target=%s",
                    mountDirSource, mountDirTarget);
        }

        String envSubCmd = "";
        if (env != null) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> keyVal : env.entrySet()) {
                sb.append(String.format("--env %s=%s ", keyVal.getKey(), keyVal.getValue()));
            }
            envSubCmd = sb.toString();
        }

        String startCommand = String.format("docker run -d --name=%s --hostname=%s --network=%s %s %s %s %s %s",
                containerName, hostName, networkName, publishPortSubCmd, exposePortSubCmd,
                mountSubCmd, envSubCmd, imageName);
        int exitCode = runShellCommand(startCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to start container");
            return false;
        }

        return true;
    }

    private boolean forwardHttpRequestToContainerizedService(XDNHttpRequest xdnRequest) {
        String serviceName = xdnRequest.getServiceName();
        System.out.println(">> is service active: " + isServiceActive.get(serviceName));
        if (isServiceActive.get(serviceName) != null && !isServiceActive.get(serviceName)) {
            activate(serviceName);
            isServiceActive.put(serviceName, true);
        }

        try {
            // create http request
            HttpRequest httpRequest = convertXDNRequestToHttpRequest(xdnRequest);
            if (httpRequest == null) {
                return false;
            }

            System.out.println(">> " + activeReplicaID + " forwarding request to service ...");
            // forward request to the containerized service, and get the http response
            HttpResponse<byte[]> response = serviceClient.send(httpRequest,
                    HttpResponse.BodyHandlers.ofByteArray());

            // convert the response into netty's http response
            io.netty.handler.codec.http.HttpResponse nettyHttpResponse =
                    createNettyHttpResponse(response);

            // store the response in the xdn request, later to be returned to the end client.
            System.out.println(">> " + activeReplicaID + " storing response " + nettyHttpResponse);
            xdnRequest.setHttpResponse(nettyHttpResponse);
            return true;
        } catch (Exception e) {
            xdnRequest.setHttpResponse(createNettyHttpErrorResponse(e));
            e.printStackTrace();
            return false;
        }
    }

    // convertXDNRequestToHttpRequest converts Netty's HTTP request into Java's HTTP request
    private HttpRequest convertXDNRequestToHttpRequest(XDNHttpRequest xdnRequest) {
        try {
            // preparing url to the containerized service
            String url = String.format("http://127.0.0.1:%d%s",
                    this.activeServicePorts.get(xdnRequest.getServiceName()),
                    xdnRequest.getHttpRequest().uri());

            // preparing the HTTP request body, if any
            // TODO: handle non text body, ie. file or binary data
            HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.noBody();
            if (xdnRequest.getHttpRequestContent() != null &&
                    xdnRequest.getHttpRequestContent().content() != null) {
                bodyPublisher = HttpRequest
                        .BodyPublishers
                        .ofString(xdnRequest.getHttpRequestContent().content()
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

    private io.netty.handler.codec.http.HttpResponse createNettyHttpErrorResponse(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        sw.write("Failed to get response from the containerized service:\n");
        e.printStackTrace(pw);
        return new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                Unpooled.copiedBuffer(sw.toString().getBytes()));
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


    private int runShellCommand(String command) {
        return runShellCommand(command, true);
    }

    private int runShellCommand(String command, boolean isSilent) {
        return runShellCommand(command, isSilent, null);
    }

    private int runShellCommand(String command, boolean isSilent,
                                Map<String, String> environmentVariables) {
        try {
            // prepare to start the command
            ProcessBuilder pb = new ProcessBuilder(command.split("\\s+"));
            if (isSilent) {
                pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
                pb.redirectError(ProcessBuilder.Redirect.DISCARD);
            }

            if (environmentVariables != null) {
                Map<String, String> processEnv = pb.environment();
                processEnv.putAll(environmentVariables);
            }

            if (!isSilent) {
                System.out.println("command: " + command);
                if (environmentVariables != null) {
                    System.out.println(environmentVariables.toString());
                }
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


    /**********************************************************************************************
     *                                  End utility methods                                     *
     *********************************************************************************************/

}
