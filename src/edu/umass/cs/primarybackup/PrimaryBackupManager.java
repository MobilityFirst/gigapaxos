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
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

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

    // requests forwarded to the primary
    private final Map<Long, RequestAndCallback> forwardedRequests;

    public PrimaryBackupManager(NodeIDType nodeID,
                                Replicable replicableApp,
                                BackupableApplication backupableApp,
                                Stringifiable<NodeIDType> unstringer,
                                Messenger<NodeIDType, JSONObject> messenger) {

        // Replicable and BackupableApplication interface must be implemented by the same
        // Application because captureStateDiff(.) in the BackupableApplication is invoked after
        // execute(.) in the Replicable.
        assert replicableApp.getClass().getSimpleName().
                equals(backupableApp.getClass().getSimpleName());

        this.myNodeID = nodeID;
        this.currentPrimaryEpoch = new HashMap<>();
        this.currentRole = new HashMap<>();
        this.currentPrimary = new HashMap<>();

        this.replicableApp = replicableApp;
        this.backupableApp = backupableApp;
        this.paxosMiddlewareApp = new PaxosMiddlewareApp(
                nodeID.toString(), this.backupableApp, this.currentPrimary,
                this.currentPrimaryEpoch);

        // TODO: confirm whether paxosLogFolder==null is fine here
        this.paxosManager = new PaxosManager<>(
                this.myNodeID,
                unstringer,
                messenger,
                this.paxosMiddlewareApp,
                null,
                true)
                .initClientMessenger(new InetSocketAddress(
                                messenger.getNodeConfig().getNodeAddress(this.myNodeID),
                                messenger.getNodeConfig().getNodePort(this.myNodeID)),
                        messenger);

        this.messenger = messenger;
        this.outstandingRequests = new LinkedList<>();
        this.forwardedRequests = new HashMap<>();

        System.out.printf(">> %s PrimaryBackupManager is initialized.", myNodeID);
    }

    public static Set<IntegerPacketType> getAllPrimaryBackupPacketTypes() {
        return new HashSet<>(List.of(PrimaryBackupPacketType.values()));
    }

    // TODO: handle different packet here
    public boolean handlePrimaryBackupPacket(PrimaryBackupPacket packet, ExecutedCallback callback) {
        System.out.printf(">> PBManager-%s: handling packet %s\n", myNodeID, packet.getRequestType());

        if (packet instanceof RequestPacket requestPacket) {
            return handleRequestPacket(requestPacket, callback);
        }

        if (packet instanceof ForwardedRequestPacket forwardedRequestPacket) {
            return handleForwardedRequestPacket(forwardedRequestPacket, callback);
        }

        if (packet instanceof ResponsePacket responsePacket) {
            return handleResponsePacket(responsePacket, callback);
        }

        if (packet instanceof ChangePrimaryPacket changePrimaryPacket) {
            throw new RuntimeException("unimplemented");
        }

        if (packet instanceof ApplyStateDiffPacket) {
            throw new RuntimeException("ApplyStateDiffPacket can only be handled in " +
                    "PaxosMiddlewareApp via Paxos' execute(.), after agreement");
        }

        if (packet instanceof StartEpochPacket) {
            throw new RuntimeException("StartEpochPacket can only be handled in PaxosMiddlewareApp "
                    + "via Paxos' execute(.), after agreement");
        }

        return true;
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
        assert currentServiceRole == Role.PRIMARY;

        // RequestPacket -> AppRequest -> execute() -> AppResponse -> RequestPacket (with response)
        try {
            // parse the encapsulated application request
            String encodedServiceRequest = new String(packet.getEncodedServiceRequest(),
                    StandardCharsets.ISO_8859_1);
            Request appRequest = replicableApp.getRequest(encodedServiceRequest);

            // execute the app request, put response if request is ClientRequest
            replicableApp.execute(appRequest);
            if (appRequest instanceof ClientRequest) {
                ClientRequest responsePacket = ((ClientRequest) appRequest).getResponse();
                packet.setResponse(responsePacket);
                System.out.printf(">> PBManager-%s: set response to %s\n",
                        myNodeID, responsePacket.toString());
            }

            // capture stateDiff, then propose the stateDiff
            String stateDiff = backupableApp.captureStatediff(serviceName);
            PrimaryEpoch currentEpoch = this.currentPrimaryEpoch.get(serviceName);
            if (currentEpoch == null) {
                throw new RuntimeException("Unknown current primary epoch for " + serviceName);
            }
            this.paxosManager.propose(
                    serviceName,
                    new ApplyStateDiffPacket(serviceName, currentEpoch, stateDiff),
                    (stateDiffPacket, handled) -> {
                        callback.executed(packet, handled);
                    });

            return true;
        } catch (RequestParseException e) {
            throw new RuntimeException(e);
        }
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
            this.messenger.send(m);
        } catch (IOException | JSONException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    private boolean askClientToContactPrimary(RequestPacket packet, ExecutedCallback callback) {
        throw new RuntimeException("unimplemented");
    }

    private boolean handleForwardedRequestPacket(ForwardedRequestPacket forwardedRequestPacket,
                                                 ExecutedCallback callback) {
        System.out.printf(">> PBManager-%s: handling forwarded request %s\n",
                myNodeID, forwardedRequestPacket.toString());

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

    private boolean handleResponsePacket(ResponsePacket responsePacket, ExecutedCallback callback) {
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
                        System.out.printf(">> %s I'M THE PRIMARY NOW FOR %s!!",
                                myNodeID, groupName);
                        currentRole.put(groupName, Role.PRIMARY);
                        currentPrimary.put(groupName, paxosCoordinatorID.toString());
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
        private final BackupableApplication backupableApplicationPtr;

        public PaxosMiddlewareApp(String myNodeID,
                                  BackupableApplication backupableApplicationPtr,
                                  Map<String, String> currentPrimaryPtr,
                                  Map<String, PrimaryEpoch> currentEpochPtr) {
            this.myNodeID = myNodeID;
            this.backupableApplicationPtr = backupableApplicationPtr;
            this.currentPrimaryPtr = currentPrimaryPtr;
            this.currentEpochPtr = currentEpochPtr;

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

            throw new RuntimeException("unable to parse request " + stringified);
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

            System.out.printf(">>> %s PaxosMiddlewareApp execute request=%s\n",
                    myNodeID, request.getClass().getSimpleName());
            String serviceName = request.getServiceName();

            if (request instanceof StartEpochPacket startEpochPacket) {
                PrimaryEpoch primaryEpoch = startEpochPacket.getStartingEpoch();
                currentPrimaryPtr.put(serviceName, primaryEpoch.nodeID);
                System.out.printf(">> %s putting current primary for %s as %s\n",
                        myNodeID, serviceName, primaryEpoch.nodeID);
                return true;
            }

            if (request instanceof ApplyStateDiffPacket stateDiffPacket) {
                PrimaryEpoch primaryEpoch = stateDiffPacket.getPrimaryEpoch();
                PrimaryEpoch currentEpoch = currentEpochPtr.get(serviceName);

                if (primaryEpoch.nodeID.equals(myNodeID)) {
                    System.out.printf(">> PBManager-%s: ignoring stateDiff from myself\n",
                            myNodeID);
                    return true;
                }

                if (currentEpoch == null || currentEpoch.compareTo(primaryEpoch) <= 0) {
                    currentEpochPtr.put(serviceName, primaryEpoch);
                    currentPrimaryPtr.put(serviceName, primaryEpoch.nodeID);
                    this.backupableApplicationPtr.applyStatediff(
                            serviceName, stateDiffPacket.getStateDiff());
                    return true;
                }
                if (currentEpoch.compareTo(primaryEpoch) >= 0) {
                    // ignore stale stateDiff
                    System.out.printf(">> PBManager-%s: ignoring stale stateDiff with epoch %s " +
                                    "(currentEpoch:%s)\n",
                            myNodeID, primaryEpoch.toString(), currentEpoch.toString());
                    return true;
                }
            }

            return true;
        }

        @Override
        public String checkpoint(String name) {
            return null;
        }

        @Override
        public boolean restore(String name, String state) {
            System.out.printf(">>> %s PaxosMiddlewareApp restore name=%s state=%s\n",
                    myNodeID, name, state);
            return true;
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
