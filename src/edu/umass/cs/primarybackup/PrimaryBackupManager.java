package edu.umass.cs.primarybackup;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.primarybackup.packets.*;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PrimaryBackupManager<NodeIDType> implements AppRequestParser {

    private final boolean ENABLE_INTERNAL_REDIRECT_PRIMARY = true;

    private final NodeIDType myNodeID;
    private final PaxosManager<NodeIDType> paxosManager;
    private final Replicable paxosMiddlewareApp;
    private final Replicable replicableApp;
    private final BackupableApplication backupableApp;

    private final Map<String, PrimaryEpoch> currentPrimaryEpoch;
    private final Map<String, Role> currentRole;
    private final Map<String, String> currentPrimary;

    private final Messenger<NodeIDType, ?> messenger;

    // requests while waiting role change from PRIMARY_CANDIDATE to PRIMARY
    private final Queue<RequestAndCallback> outstandingRequests;

    // requests forwarded to the PRIMARY
    private final Map<Long, RequestAndCallback> forwardedRequests;

    public PrimaryBackupManager(NodeIDType nodeID,
                                Replicable replicableApp,
                                BackupableApplication backupableApp,
                                Stringifiable<NodeIDType> unstringer,
                                Messenger<NodeIDType, JSONObject> messenger,
                                boolean omitClientMessengerInitialization) {

        // Replicable and BackupableApplication interface must be implemented by the same
        // Application because captureStateDiff(.) in the BackupableApplication is invoked after
        // execute(.) in the Replicable.
        assert replicableApp.getClass().getSimpleName().
                equals(backupableApp.getClass().getSimpleName());

        this.myNodeID = nodeID;
        this.currentPrimaryEpoch = new ConcurrentHashMap<>();
        this.currentRole = new ConcurrentHashMap<>();
        this.currentPrimary = new ConcurrentHashMap<>();

        this.replicableApp = replicableApp;
        this.backupableApp = backupableApp;
        this.paxosMiddlewareApp = new PaxosMiddlewareApp(
                nodeID.toString(),
                this.replicableApp,
                this.backupableApp,
                this.currentPrimary,
                this.currentPrimaryEpoch,
                this.currentRole,
                this
        );

        this.setupPaxosConfiguration();
        this.paxosManager = new PaxosManager<>(
                this.myNodeID,
                unstringer,
                messenger,
                this.paxosMiddlewareApp,
                null,
                true);
        if (!omitClientMessengerInitialization) {
            this.paxosManager.initClientMessenger(new InetSocketAddress(
                            messenger.getNodeConfig().getNodeAddress(this.myNodeID),
                            messenger.getNodeConfig().getNodePort(this.myNodeID)),
                    messenger);
        }

        this.messenger = messenger;
        this.outstandingRequests = new ConcurrentLinkedQueue<>();
        this.forwardedRequests = new ConcurrentHashMap<>();

        System.out.printf(">> %s PrimaryBackupManager is initialized.\n", myNodeID);
    }

    // setupPaxosConfiguration sets Paxos configuration required for PrimaryBackup use case
    private void setupPaxosConfiguration() {
        String[] args = {
                String.format("%s=%b", PaxosConfig.PC.ENABLE_EMBEDDED_STORE_SHUTDOWN, true),
                String.format("%s=%b", PaxosConfig.PC.ENABLE_STARTUP_LEADER_ELECTION, false),
                String.format("%s=%b", PaxosConfig.PC.FORWARD_PREEMPTED_REQUESTS, false),
                String.format("%s=%d", PaxosConfig.PC.PACKET_DEMULTIPLEXER_THREADS, 0),
                String.format("%s=%b", PaxosConfig.PC.HIBERNATE_OPTION, true),
                String.format("%s=%b", PaxosConfig.PC.BATCHING_ENABLED, false),
        };
        Config.register(args);

        // TODO: investigate how to enable batching without stateDiff reordering
    }

    public PrimaryBackupManager(NodeIDType nodeID,
                                Replicable replicableApp,
                                BackupableApplication backupableApp,
                                Stringifiable<NodeIDType> unstringer,
                                Messenger<NodeIDType, JSONObject> messenger) {
        this(nodeID, replicableApp, backupableApp, unstringer, messenger, false);
    }

    public static Set<IntegerPacketType> getAllPrimaryBackupPacketTypes() {
        return new HashSet<>(List.of(PrimaryBackupPacketType.values()));
    }

    public boolean handlePrimaryBackupPacket(
            PrimaryBackupPacket packet, ExecutedCallback callback) {
        System.out.printf(">> PBManager-%s: handling packet %s\n", myNodeID, packet.getRequestType());

        // RequestPacket: client -> entry replica
        if (packet instanceof RequestPacket requestPacket) {
            return handleRequestPacket(requestPacket, callback);
        }

        // ForwardedRequestPacket: entry replica -> primary
        if (packet instanceof ForwardedRequestPacket forwardedRequestPacket) {
            return handleForwardedRequestPacket(forwardedRequestPacket, callback);
        }

        // ResponsePacket: primary -> entry replica
        if (packet instanceof ResponsePacket responsePacket) {
            return handleResponsePacket(responsePacket, callback);
        }

        // ChangePrimaryPacket: client -> entry replica
        if (packet instanceof ChangePrimaryPacket changePrimaryPacket) {
            return handleChangePrimaryPacket(changePrimaryPacket, callback);
        }

        // ApplyStateDiffPacket: primary -> replica
        if (packet instanceof ApplyStateDiffPacket) {
            throw new RuntimeException("ApplyStateDiffPacket can only be handled in " +
                    "PaxosMiddlewareApp via Paxos' execute(.), after agreement");
        }

        // StartEpochPacket: primary candidate -> replica
        if (packet instanceof StartEpochPacket) {
            throw new RuntimeException("StartEpochPacket can only be handled in PaxosMiddlewareApp "
                    + "via Paxos' execute(.), after agreement");
        }

        String exceptionMsg = String.format("unknown primary backup packet '%s'",
                packet.getClass().getSimpleName());
        throw new RuntimeException(exceptionMsg);
    }

    private boolean handleRequestPacket(RequestPacket packet, ExecutedCallback callback) {
        String serviceName = packet.getServiceName();

        Role currentServiceRole = this.currentRole.get(serviceName);
        if (currentServiceRole == null) {
            System.out.printf("Unknown service %s\n", serviceName);
            return true;
        }

        if (currentServiceRole == Role.PRIMARY) {
            return executeRequestCoordinateStateDiff(packet, callback);
        }

        if (currentServiceRole == Role.PRIMARY_CANDIDATE) {
            RequestAndCallback rc = new RequestAndCallback(packet, callback);
            outstandingRequests.add(rc);
            return true;
        }

        if (currentServiceRole == Role.BACKUP) {
            return handRequestToPrimary(packet, callback);
        }

        throw new RuntimeException(String.format("Unknown role %s for service %s\n",
                currentServiceRole, serviceName));
    }

    // TODO: handle a batch of request from outstanding queue, instead of handling it one by one.
    private boolean executeRequestCoordinateStateDiff(RequestPacket packet,
                                                      ExecutedCallback callback) {
        System.out.printf(">> PBManager-%s: handling request on primary %s\n",
                myNodeID, packet.toString());

        String serviceName = packet.getServiceName();

        // ensure this method is only invoked by the primary node
        Role currentServiceRole = this.currentRole.get(serviceName);
        assert currentServiceRole == Role.PRIMARY : String.format("%s my role for %s is %s",
                myNodeID, serviceName, currentServiceRole.toString());

        // RequestPacket -> AppRequest -> execute() -> AppResponse -> RequestPacket (with response)
        Request appRequest = null;
        try {
            // parse the encapsulated application request
            String encodedServiceRequest = new String(packet.getEncodedServiceRequest(),
                    StandardCharsets.ISO_8859_1);
            appRequest = replicableApp.getRequest(encodedServiceRequest);
        } catch (RequestParseException e) {
            throw new RuntimeException(e);
        }


        // execute the app request, and capture the stateDiff
        PrimaryEpoch currentEpoch = null;
        boolean isExecuteSuccess = false;
        String stateDiff = null;
        synchronized (this) {
            currentEpoch = this.currentPrimaryEpoch.get(serviceName);
            if (currentEpoch == null) {
                throw new RuntimeException("Unknown current primary epoch for " + serviceName);
            }
            isExecuteSuccess = replicableApp.execute(appRequest);
            if (!isExecuteSuccess) {
                throw new RuntimeException("Failed to execute request for " + serviceName);
            }
            stateDiff = backupableApp.captureStatediff(serviceName);
        }

        // propose the stateDiff
        System.out.printf(">>> %s:PBManager proposing epoch=%s statediff=%s\n",
                myNodeID, currentEpoch, stateDiff);
        PrimaryEpoch finalCurrentEpoch = currentEpoch;
        String finalStateDiff = stateDiff;
        this.paxosManager.propose(
                serviceName,
                new ApplyStateDiffPacket(serviceName, currentEpoch, stateDiff),
                (stateDiffPacket, handled) -> {
                    System.out.printf(">>> %s:PBManager epoch=%s statediff=%s is accepted\n",
                            myNodeID, finalCurrentEpoch, finalStateDiff);
                    callback.executed(packet, handled);
                });

        // put response if request is ClientRequest
        if (appRequest instanceof ClientRequest) {
            ClientRequest responsePacket = ((ClientRequest) appRequest).getResponse();
            packet.setResponse(responsePacket);
            System.out.printf(">> PBManager-%s: set response to %s\n",
                    myNodeID, responsePacket.toString());
        }

        return true;
    }

    private boolean handRequestToPrimary(RequestPacket packet, ExecutedCallback callback) {
        System.out.printf(">> PBManager-%s: handing request to primary %s\n",
                myNodeID, packet.toString());

        if (!ENABLE_INTERNAL_REDIRECT_PRIMARY) {
            askClientToContactPrimary(packet, callback);
        }

        // get the current primary for the serviceName
        String serviceName = packet.getServiceName();
        String currentPrimaryIDStr = currentPrimary.get(serviceName);
        if (currentPrimaryIDStr == null) {
            throw new RuntimeException("Unknown primary ID");
            // TODO: potential fix would be to ask the current Paxos' coordinator to be the Primary
        }
        System.out.printf(">> PBManager-%s: handing request to primary at %s\n",
                myNodeID, currentPrimaryIDStr);

        // store the request and callback, so later we can send the response back to client
        // after receiving the response from the primary
        RequestAndCallback rc = new RequestAndCallback(packet, callback);
        this.forwardedRequests.put(packet.getRequestID(), rc);

        // prepare the forwarded request
        ForwardedRequestPacket forwardPacket = new ForwardedRequestPacket(
                serviceName,
                myNodeID.toString(),
                packet.toString().getBytes(StandardCharsets.ISO_8859_1));
        NodeIDType currentPrimaryID = (NodeIDType) currentPrimaryIDStr;
        GenericMessagingTask<NodeIDType, PrimaryBackupPacket> m = new GenericMessagingTask<>(
                currentPrimaryID, forwardPacket);

        // send the forwarded request to the primary
        try {
            System.out.printf(">> PBManager-%s: is disconnected to %s? %b\n",
                    myNodeID, currentPrimaryID, this.messenger.isDisconnected(currentPrimaryID));
            this.messenger.send(m);
        } catch (IOException | JSONException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    private boolean askClientToContactPrimary(RequestPacket packet, ExecutedCallback callback) {
        throw new RuntimeException("unimplemented");
    }

    private boolean handleForwardedRequestPacket(
            ForwardedRequestPacket forwardedRequestPacket, ExecutedCallback callback) {
        System.out.printf(">> PBManager-%s: handling forwarded request %s\n",
                myNodeID, forwardedRequestPacket.toString());

        String groupName = forwardedRequestPacket.getServiceName();
        Role curentRole = null;
        synchronized (this) {
            curentRole = this.currentRole.get(groupName);
        }

        if (curentRole.equals(Role.BACKUP)) {
            throw new RuntimeException("Unimplemented: should re-forward request to primary");
        }

        if (curentRole.equals(Role.PRIMARY_CANDIDATE)) {
            throw new RuntimeException("Unimplemented: should buffer request");
        }

        if (curentRole.equals(Role.PRIMARY)) {
            byte[] encodedRequest = forwardedRequestPacket.getForwardedRequest();
            String encodedRequestString = new String(encodedRequest, StandardCharsets.ISO_8859_1);
            RequestPacket rp = RequestPacket.createFromString(encodedRequestString);
            this.executeRequestCoordinateStateDiff(rp, (executedRequest, handled) -> {
                System.out.printf(">> PBManager-%s: forwarded request is executed, forwarding " +
                                "response back to the entry replica in %s\n",
                        myNodeID, forwardedRequestPacket.getEntryNodeID());
                System.out.printf(">> PBManager-%s: response %s\n", myNodeID, executedRequest.toString());

                if (executedRequest instanceof ClientRequest requestWithResponse) {
                    ResponsePacket resp = new ResponsePacket(
                            executedRequest.getServiceName(),
                            rp.getRequestID(),
                            requestWithResponse.getResponse().toString().
                                    getBytes(StandardCharsets.ISO_8859_1));
                    GenericMessagingTask<NodeIDType, ResponsePacket> m = new GenericMessagingTask<>(
                            (NodeIDType) forwardedRequestPacket.getEntryNodeID(),
                            resp);
                    try {
                        messenger.send(m);
                    } catch (IOException | JSONException e) {
                        throw new RuntimeException(e);
                    }
                }

                // callback.executed(request, handled);
            });

            return true;
        }

        String exceptionMsg = String.format("%s:PrimaryBackupManager - unknown role for group '%s'",
                myNodeID, groupName);
        throw new RuntimeException(exceptionMsg);
    }

    private boolean handleResponsePacket(ResponsePacket responsePacket, ExecutedCallback
            callback) {
        try {
            byte[] encodedResponse = responsePacket.getEncodedResponse();
            Request appRequest = this.replicableApp.getRequest(
                    new String(encodedResponse, StandardCharsets.ISO_8859_1));

            if (appRequest instanceof ClientRequest appRequestWithResponse) {
                Long executedRequestID = responsePacket.getRequestID();
                RequestAndCallback rc = forwardedRequests.get(executedRequestID);
                if (rc == null) {
                    System.out.printf(">> PBManager-%s: unknown callback for RequestPacket-%d (%s)\n",
                            myNodeID, executedRequestID, this.getAllForwardedRequestIDs());
                    return true;
                }

                rc.requestPacket().setResponse(appRequestWithResponse);
                rc.callback().executed(rc.requestPacket(), true);
            }
        } catch (RequestParseException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    private boolean handleChangePrimaryPacket(ChangePrimaryPacket packet,
                                              ExecutedCallback callback) {

        // ignore ChangePrimary with incorrect nodeID
        if (!Objects.equals(packet.getNodeID(), myNodeID.toString())) {
            callback.executed(packet, false);
        }

        String groupName = packet.getServiceName();
        Role myCurrentRole = null;
        PrimaryEpoch curEpoch = null;
        synchronized (this) {
            myCurrentRole = this.currentRole.get(groupName);
            curEpoch = this.currentPrimaryEpoch.get(groupName);
        }
        if (myCurrentRole == null) {
            System.out.printf(">> %s unknown role for service name '%s'", myNodeID, groupName);
            return true;
        }
        if (myCurrentRole.equals(Role.PRIMARY)) {
            System.out.printf(">> %s already the primary for service name '%s'",
                    myNodeID,
                    groupName);
            return true;
        }

        if (curEpoch == null) {
            System.out.printf(">> %s unknown current epoch for service name '%s'",
                    myNodeID, groupName);
            return true;
        }
        PrimaryEpoch newEpoch = new PrimaryEpoch(
                myNodeID.toString(),
                curEpoch.counter + 1
        );

        this.paxosManager.tryToBePaxosCoordinator(groupName); // could still be fail
        this.currentRole.put(groupName, Role.PRIMARY_CANDIDATE);
        this.currentPrimaryEpoch.put(groupName, newEpoch);
        StartEpochPacket startPacket = new StartEpochPacket(groupName, newEpoch);
        this.paxosManager.propose(
                groupName,
                startPacket,
                (proposedPacket, isHandled) -> {
                    System.out.printf("\n\n>> %s I'M THE PRIMARY NOW FOR %s!!\n\n",
                            myNodeID, groupName);
                    currentRole.put(groupName, Role.PRIMARY);
                    currentPrimary.put(groupName, myNodeID.toString());
                    processOutstandingRequests();

                    callback.executed(packet, isHandled);
                }
        );
        return true;
    }

    private String getAllForwardedRequestIDs() {
        StringBuilder result = new StringBuilder();
        for (RequestAndCallback rc : forwardedRequests.values()) {
            result.append(rc.requestPacket().getRequestID()).append(", ");
        }
        return result.toString();
    }

    /**
     * Note that PlacementEpoch, being used in this method, is not the same as the primaryEpoch.
     * PlacementEpoch increases when placement changes (i.e., reconfiguration, possibly with the
     * same set of nodes), however, PrimaryEpoch can increase even if there is no reconfiguration.
     * PrimaryEpoch increases when a new Primary emerges, even within the same replica group.
     */
    public boolean createPrimaryBackupInstance(String groupName,
                                               int placementEpoch,
                                               String initialState,
                                               Set<NodeIDType> nodes) {
        System.out.printf(">> %s PrimaryBackupManager - createPrimaryBackupInstance | " +
                        "groupName: %s, placementEpoch: %d, initialState: %s, nodes: %s\n",
                myNodeID, groupName, placementEpoch, initialState, nodes.toString());

        boolean created = this.paxosManager.createPaxosInstanceForcibly(
                groupName, placementEpoch, nodes, this.paxosMiddlewareApp, initialState, 0);
        boolean alreadyExist = this.paxosManager.equalOrHigherVersionExists(
                groupName, placementEpoch);

        if (!created && !alreadyExist) {
            throw new PaxosInstanceCreationException((this
                    + " failed to create " + groupName + ":" + placementEpoch
                    + " with state [" + initialState + "]") + "; existing_version=" +
                    this.paxosManager.getVersion(groupName));
        }

        // FIXME: these three default replica groups must be handled with specific app that
        //  uses Paxos.
        if (groupName.equals(PaxosConfig.getDefaultServiceName()) ||
                groupName.equals(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString()) ||
                groupName.equals(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString())) {
            return true;
        }

        boolean isInitializationSuccess = initializePrimaryEpoch(groupName);
        if (!isInitializationSuccess) {
            System.out.printf("Failed to initialize replica group for %s\n", groupName);
            return false;
        }

        return true;
    }

    private boolean initializePrimaryEpoch(String groupName) {
        this.currentRole.put(groupName, Role.BACKUP);
        NodeIDType paxosCoordinatorID = this.paxosManager.getPaxosCoordinator(groupName);
        if (paxosCoordinatorID == null) {
            throw new RuntimeException("Failed to get paxos coordinator for " + groupName);
        }

        if (paxosCoordinatorID.equals(myNodeID)) {
            System.out.printf(">> %s Initializing primary epoch for %s\n", myNodeID, groupName);
            PrimaryEpoch zero = new PrimaryEpoch(myNodeID.toString(), 0);
            this.currentRole.put(groupName, Role.PRIMARY_CANDIDATE);
            this.currentPrimaryEpoch.put(groupName, zero);
            StartEpochPacket startPacket = new StartEpochPacket(groupName, zero);
            this.paxosManager.propose(
                    groupName,
                    startPacket,
                    (proposedPacket, isHandled) -> {
                        System.out.printf("\n\n>> %s I'M THE PRIMARY NOW FOR %s!!\n\n",
                                myNodeID, groupName);
                        currentRole.put(groupName, Role.PRIMARY);
                        currentPrimary.put(groupName, paxosCoordinatorID.toString());
                        currentPrimaryEpoch.put(groupName, zero);
                        processOutstandingRequests();
                    }
            );
        }

        // FIXME: as a temporary measure, we wait until StartEpochPacket is being agreed upon.
        //  This is needed for now as the current implementation has not handle the case when
        //  a node does not know the current Primary.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    private void processOutstandingRequests() {
        assert this.outstandingRequests != null;
        while (!this.outstandingRequests.isEmpty()) {
            RequestAndCallback rc = this.outstandingRequests.poll();
            executeRequestCoordinateStateDiff(rc.requestPacket(), rc.callback());
        }
    }

    // TODO: also handle deletion of PBInstance with placement epoch
    public boolean deletePrimaryBackupInstance(String groupName, int placementEpoch) {
        System.out.println(">> deletePrimaryBackupInstance - " + groupName);
        return this.paxosManager.deleteStoppedPaxosInstance(groupName, placementEpoch);
    }

    public Set<NodeIDType> getReplicaGroup(String groupName) {
        System.out.println(">> getReplicaGroup - " + groupName);
        return this.paxosManager.getReplicaGroup(groupName);
    }

    public final void stop() {
        this.paxosManager.close();
    }

    public boolean isCurrentPrimary(String groupName) {
        Role myCurrentRole = this.currentRole.get(groupName);
        if (myCurrentRole == null) {
            return false;
        }
        return myCurrentRole.equals(Role.PRIMARY);
    }

    private void restartPaxosInstance(String groupName) {
        this.paxosManager.restartFromLastCheckpoint(groupName);
    }


    //--------------------------------------------------------------------------------------------||
    //                  Begin implementation for AppRequestParser interface                       ||
    // Despite the interface name, the requests parsed here are intended for the replica          ||
    // coordinator packets, i.e., PrimaryBackupPacket, and not for AppRequest.                    ||
    //--------------------------------------------------------------------------------------------||

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        assert stringified != null;
        if (!stringified.startsWith(PrimaryBackupPacket.SERIALIZED_PREFIX)) {
            throw new RuntimeException(String.format("PBManager-%s: request for primary backup " +
                    "coordinator has invalid prefix %s", myNodeID, stringified));
        }

        if (stringified.startsWith(ForwardedRequestPacket.SERIALIZED_PREFIX)) {
            return ForwardedRequestPacket.createFromString(stringified);
        }

        if (stringified.startsWith(ResponsePacket.SERIALIZED_PREFIX)) {
            return ResponsePacket.createFromString(stringified);
        }

        throw new RuntimeException(String.format("PBManager-%s: Unknown encoded request %s\n",
                myNodeID, stringified));
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return getAllPrimaryBackupPacketTypes();
    }

    //--------------------------------------------------------------------------------------------||
    //                     End implementation for AppRequestParser interface                      ||
    //--------------------------------------------------------------------------------------------||

    //--------------------------------------------------------------------------------------------||
    // Begin implementation for PaxosMiddlewareApp.                                               ||
    // A middleware application that handle execute(.) before the PrimaryBackupManager can do     ||
    // execution in the BackupableApplication.                                                    ||
    //--------------------------------------------------------------------------------------------||

    /**
     * PaxosMiddlewareApp is the application of Paxos used in the PrimaryBackupManager.
     * As an application of Paxos, generally PaxosMiddlewareApp apply the statediffs being
     * agreed upon from Paxos. Thus, the execute() method simply apply the statediffs.
     * Additionally, PaxosMiddlewareApp needs to ignore statediff from 'stale' primary
     * to ensure primary integrity. i.e., making the execution as no-op.
     */
    public static class PaxosMiddlewareApp implements Replicable, Reconfigurable {

        private final String myNodeID;
        private final Set<IntegerPacketType> requestTypes;

        /**
         * currentPrimaryPtr is a pointer to the {@link PrimaryBackupManager#currentPrimary}
         */
        private final Map<String, String> currentPrimaryPtr;
        private final Map<String, PrimaryEpoch> currentEpochPtr;
        private final Map<String, Role> currentRolePtr;
        private final BackupableApplication backupableApplicationPtr;
        private final Replicable replicableAppPtr;
        private final PrimaryBackupManager managerPtr;

        public PaxosMiddlewareApp(String myNodeID,
                                  Replicable replicableAppPtr,
                                  BackupableApplication backupableApplicationPtr,
                                  Map<String, String> currentPrimaryPtr,
                                  Map<String, PrimaryEpoch> currentEpochPtr,
                                  Map<String, Role> currentRolePtr,
                                  PrimaryBackupManager managerPtr) {

            // Replicable and BackupableApplication interface must be implemented by the same
            // Application because captureStateDiff(.) in the BackupableApplication is invoked after
            // execute(.) in the Replicable.
            assert replicableAppPtr.getClass().getSimpleName().
                    equals(backupableApplicationPtr.getClass().getSimpleName());
            assert replicableAppPtr.hashCode() == backupableApplicationPtr.hashCode();

            this.myNodeID = myNodeID;
            this.replicableAppPtr = replicableAppPtr;
            this.backupableApplicationPtr = backupableApplicationPtr;
            this.currentPrimaryPtr = currentPrimaryPtr;
            this.currentEpochPtr = currentEpochPtr;
            this.currentRolePtr = currentRolePtr;
            this.managerPtr = managerPtr;

            // only two packet/request type that t
            Set<IntegerPacketType> types = new HashSet<>();
            types.add(PrimaryBackupPacketType.PB_START_EPOCH_PACKET);
            types.add(PrimaryBackupPacketType.PB_STATE_DIFF_PACKET);
            this.requestTypes = types;
        }

        @Override
        public Request getRequest(String stringified) throws RequestParseException {
            assert stringified != null;

            if (stringified.startsWith(StartEpochPacket.SERIALIZED_PREFIX)) {
                return StartEpochPacket.createFromString(stringified);
            }

            if (stringified.startsWith(ApplyStateDiffPacket.SERIALIZED_PREFIX)) {
                return ApplyStateDiffPacket.createFromString(stringified);
            }

            throw new RequestParseException(
                    new RuntimeException("unable to parse request " + stringified)
            );
        }

        @Override
        public Set<IntegerPacketType> getRequestTypes() {
            return this.requestTypes;
        }

        @Override
        public boolean execute(Request request) {
            return execute(request, false);
        }

        @Override
        public boolean execute(Request request, boolean doNotReplyToClient) {
            if (request == null) {
                return true;
            }

            System.out.printf(">> %s:PaxosMiddlewareApp execute %s\n",
                    myNodeID, request.getRequestType());

            if (request instanceof StartEpochPacket startEpochPacket) {
                return executeStartEpochPacket(startEpochPacket);
            }

            if (request instanceof ApplyStateDiffPacket stateDiffPacket) {
                return executeApplyStateDiffPacket(stateDiffPacket);
            }

            throw new RuntimeException(String.format("PaxosMiddlewareApp: Unknown execute handler" +
                    " for request %s: %s", request.getClass().getSimpleName(), request.toString()));
        }

        private boolean executeStartEpochPacket(StartEpochPacket packet) {
            System.out.printf(">>> %s:PaxosMiddlewareApp:executeStartEpoch epoch=%s\n",
                    myNodeID, packet.getStartingEpoch());
            String groupName = packet.getServiceName();
            PrimaryEpoch newPrimaryEpoch = packet.getStartingEpoch();
            PrimaryEpoch currentEpoch = this.currentEpochPtr.get(groupName);
            String newPrimaryID = newPrimaryEpoch.nodeID;

            // update my current epoch, if its unknown
            if (currentEpoch == null) {
                this.currentEpochPtr.put(groupName, newPrimaryEpoch);
                this.currentPrimaryPtr.put(groupName, newPrimaryID);
                currentEpoch = newPrimaryEpoch;
            }

            // receive smaller, ignore that epoch.
            // receive current epoch from myself, ignore the StartEpoch packet as it already
            // handled via callback of propose(StartEpoch)
            if (newPrimaryEpoch.compareTo(currentEpoch) <= 0) {
                return true;
            }

            // step down to be backup node
            if (newPrimaryEpoch.compareTo(currentEpoch) > 0) {
                Role myCurrentRole = currentRolePtr.get(groupName);

                if (myCurrentRole == null) {
                    currentRolePtr.put(groupName, Role.BACKUP);
                    currentEpochPtr.put(groupName, newPrimaryEpoch);
                    currentPrimaryPtr.put(groupName, newPrimaryID);
                    System.out.printf(">> %s putting current primary for %s as %s\n",
                            myNodeID, groupName, newPrimaryID);
                    return true;
                }

                if (myCurrentRole.equals(Role.BACKUP)) {
                    currentRolePtr.put(groupName, Role.BACKUP);
                    currentEpochPtr.put(groupName, newPrimaryEpoch);
                    currentPrimaryPtr.put(groupName, newPrimaryID);
                    System.out.printf(">> %s putting current primary for %s as %s\n",
                            myNodeID, groupName, newPrimaryID);
                    return true;
                }

                if (myCurrentRole.equals(Role.PRIMARY) ||
                        myCurrentRole.equals(Role.PRIMARY_CANDIDATE)) {
                    currentRolePtr.put(groupName, Role.BACKUP);
                    currentEpochPtr.put(groupName, newPrimaryEpoch);
                    currentPrimaryPtr.put(groupName, newPrimaryID);

                    System.out.printf(">> %s shutting down .... \n", myNodeID);
                    managerPtr.restartPaxosInstance(groupName);

                    // throw new RuntimeException(String.format("%s:PaxosMiddlewareApp: unhandled restart and " +
                    //        "re-execute", myNodeID));
                    return true;
                }

                throw new RuntimeException(String.format("PaxosMiddlewareApp: Unhandled case " +
                                "myEpoch=%s primaryEpoch=%s role=%s", currentEpoch, newPrimaryEpoch,
                        myCurrentRole));
            }

            throw new RuntimeException(String.format("PaxosMiddlewareApp: Unhandled case " +
                    "myEpoch=%s primaryEpoch=%s", currentEpoch, newPrimaryEpoch));
        }

        private boolean executeApplyStateDiffPacket(ApplyStateDiffPacket packet) {
            String groupName = packet.getServiceName();
            PrimaryEpoch primaryEpoch = packet.getPrimaryEpoch();
            PrimaryEpoch currentEpoch = currentEpochPtr.get(groupName);
            Role myCurrentRole = currentRolePtr.get(groupName);

            System.out.printf(">>> %s:PaxosMiddlewareApp:executeStateDiff role=%s myEpoch=%s epoch=%s stateDiff=%s\n",
                    myNodeID, myCurrentRole, currentEpoch, packet.getPrimaryEpoch(), packet.getStateDiff());

            // Invariant: when executing stateDiff for epoch e, this node must already execute
            //  startEpoch for epoch e.
            assert currentEpoch != null : "currentEpoch has not been updated";
            assert myCurrentRole != null : "Unknown role for " + groupName;

            // Case-1: lower epoch, ignoring stale stateDiff from older primary.
            if (primaryEpoch.compareTo(currentEpoch) < 0) {
                System.out.printf(">>> %s:PBManager ignoring stateDiff from old primary " +
                                "(%s, myEpoch=%s)\n",
                        myNodeID,
                        primaryEpoch,
                        currentEpoch);
                return true;
            }

            // Case-2: epoch is already current
            if (primaryEpoch.equals(currentEpoch)) {

                // Ignoring stateDiff generated by myself since myself is the primary and thus
                // already applied the stateDiff right after execution
                if (myCurrentRole.equals(Role.PRIMARY)) {
                    System.out.printf(">> PBManager-%s: ignoring stateDiff from myself\n",
                            myNodeID);
                    return true;
                }

                // This case should not happen, the epoch is already current yet this node
                // is still a primary candidate. The node should already change its role to PRIMARY
                // in the callback of propose(StartEpoch), before the node is able to propose a
                // stateDiff with current epoch.
                if (myCurrentRole.equals(Role.PRIMARY_CANDIDATE)) {
                    assert false : "executing stateDiff from myself while still being a candidate";
                    return true;
                }

                // As a backup, this node simply apply the stateDiff coming from the PRIMARY
                if (myCurrentRole.equals(Role.BACKUP)) {
                    this.backupableApplicationPtr.applyStatediff(groupName, packet.getStateDiff());
                    return true;
                }

                throw new RuntimeException(String.format("PaxosMiddlewareApp: Unhandled case " +
                        "myEpoch=primaryEpoch=%s role=%s", primaryEpoch, myCurrentRole));
            }

            // Case-3: get higher epoch
            if (primaryEpoch.compareTo(currentEpoch) > 0) {
                throw new RuntimeException(String.format("PaxosMiddlewareApp: Executing higher " +
                        "epoch=%s before executing startEpoch", primaryEpoch));
            }

            return true;
        }

        @Override
        public String checkpoint(String name) {
            System.out.printf(">>> %s PaxosMiddlewareApp checkpoint name=%s\n",
                    myNodeID, name);
            return this.replicableAppPtr.checkpoint(name);
        }

        @Override
        public boolean restore(String name, String state) {
            System.out.printf(">>> %s PaxosMiddlewareApp restore name=%s state=%s\n",
                    myNodeID, name, state);
            if (state == null || state.isEmpty()) {
                this.currentEpochPtr.remove(name);
                this.currentRolePtr.put(name, Role.BACKUP);
            }
            return this.replicableAppPtr.restore(name, state);
        }

        //----------------------------------------------------------------------------------------||
        //                   Begin implementation methods for Replicable interface                ||
        //----------------------------------------------------------------------------------------||

        private record PaxosMiddlewareStopRequest(String serviceName, int placementEpoch)
                implements ReconfigurableRequest {

            @Override
            public IntegerPacketType getRequestType() {
                return ReconfigurableRequest.STOP;
            }

            @Override
            public String getServiceName() {
                return this.serviceName;
            }

            @Override
            public int getEpochNumber() {
                return this.placementEpoch;
            }

            @Override
            public boolean isStop() {
                return true;
            }
        }

        @Override
        public ReconfigurableRequest getStopRequest(String name, int epoch) {
            return new PaxosMiddlewareStopRequest(name, epoch);
        }

        @Override
        public String getFinalState(String name, int epoch) {
            // TODO: implement me
            String exceptionMessage = String.format(
                    "PrimaryBackupManager.PaxosMiddlewareApp.getFinalState is unimplemented, " +
                            "serviceName=%s epoch=%d",
                    name, epoch);
            throw new RuntimeException(exceptionMessage);
        }

        @Override
        public void putInitialState(String name, int epoch, String state) {
            // TODO: implement me
            String exceptionMessage = String.format(
                    "PrimaryBackupManager.PaxosMiddlewareApp.putInitialState is unimplemented, " +
                            "serviceName=%s epoch=%d state=%s",
                    name, epoch, state);
            throw new RuntimeException(exceptionMessage);
        }

        @Override
        public boolean deleteFinalState(String name, int epoch) {
            // TODO: implement me
            String exceptionMessage = String.format(
                    "PrimaryBackupManager.PaxosMiddlewareApp.putInitialState is unimplemented, " +
                            "serviceName=%s epoch=%d",
                    name, epoch);
            throw new RuntimeException(exceptionMessage);
        }

        @Override
        public Integer getEpoch(String name) {
            // TODO: store epoch for each service
            return 0;
        }

        //----------------------------------------------------------------------------------------||
        //                    End implementation methods for Replicable interface                 ||
        //----------------------------------------------------------------------------------------||

    }

}
