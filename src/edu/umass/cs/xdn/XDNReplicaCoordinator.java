package edu.umass.cs.xdn;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.primarybackup.PrimaryBackupManager;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;
import edu.umass.cs.reconfiguration.PrimaryBackupReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.request.XDNRequestType;
import edu.umass.cs.xdn.service.ServiceProperty;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * XDNReplicaCoordinator is a wrapper of multiple replica coordinators supported by XDN.
 *
 * @param <NodeIDType>
 */
public class XDNReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private final String myNodeID;

    // list of all coordination managers supported in XDN
    private final AbstractReplicaCoordinator<NodeIDType> primaryBackupCoordinator;
    private final AbstractReplicaCoordinator<NodeIDType> paxosCoordinator;
    private final AbstractReplicaCoordinator<NodeIDType> chainReplicationCoordinator;
    // TODO: implement these manager below
    //  - causal manager
    //  - eventual
    //  - read your write
    //  - eventual
    //  - read your write
    //  - write follow read
    //  - monotonic reads
    //  - monotonic writes

    // mapping between service name to the service's coordination manager
    private final Map<String, AbstractReplicaCoordinator<NodeIDType>> serviceCoordinator;

    private final Set<IntegerPacketType> requestTypes;

    public XDNReplicaCoordinator(Replicable app,
                                 NodeIDType myID,
                                 Stringifiable<NodeIDType> unstringer,
                                 Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);

        System.out.printf(">> XDNReplicaCoordinator - init at node %s\n",
                myID);

        assert app.getClass().getSimpleName().equals(XDNGigapaxosApp.class.getSimpleName()) :
                "XDNReplicaCoordinator must be used with XDNGigapaxosApp";
        assert myID.getClass().getSimpleName().equals(String.class.getSimpleName()) :
                "XDNReplicaCoordinator must use String as the NodeIDType";

        this.myNodeID = myID.toString();

        // initialize all the wrapped coordinators
        PrimaryBackupManager.setupPaxosConfiguration();
        PaxosReplicaCoordinator<NodeIDType> paxosReplicaCoordinator =
                new PaxosReplicaCoordinator<>(app, myID, unstringer, messenger);
        this.paxosCoordinator = paxosReplicaCoordinator;
        this.primaryBackupCoordinator =
                new PrimaryBackupReplicaCoordinator<>(app, myID, unstringer, messenger,
                        paxosReplicaCoordinator.getPaxosManager(), true);
        this.chainReplicationCoordinator = null;

        // initialize empty service -> coordinator mapping
        this.serviceCoordinator = new ConcurrentHashMap<>();

        // registering all request types handled by XDN,
        // including all request types of each coordination managers.
        Set<IntegerPacketType> types = new HashSet<>();
        types.add(XDNRequestType.XDN_SERVICE_HTTP_REQUEST);
        types.addAll(PrimaryBackupManager.getAllPrimaryBackupPacketTypes());
        this.requestTypes = types;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        System.out.printf(">> %s:XDNReplicaCoordinator - getRequestType\n", myNodeID);
        return requestTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        System.out.printf(">> %s:XDNReplicaCoordinator - coordinateRequest request=%s payload=%s\n",
                myNodeID, request.getClass().getSimpleName(), request.toString());

        var serviceName = request.getServiceName();
        var coordinator = this.serviceCoordinator.get(serviceName);
        if (coordinator == null) {
            throw new RuntimeException("unknown coordinator for " + serviceName);
        }
        ReplicableClientRequest gpRequest = ReplicableClientRequest.wrap(request);
        gpRequest.setClientAddress(messenger.getListeningSocketAddress());
        return coordinator.coordinateRequest(gpRequest, callback);
    }

    @Override
    public boolean createReplicaGroup(String serviceName,
                                      int epoch,
                                      String state,
                                      Set<NodeIDType> nodes) {
        System.out.printf(">> %s:XDNReplicaCoordinator - createReplicaGroup name=%s, epoch=%d, state=%s, nodes=%s\n",
                myNodeID, serviceName, epoch, state, nodes);

        if (serviceName.equals(PaxosConfig.getDefaultServiceName()) ||
                serviceName.equals(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString()) ||
                serviceName.equals(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString())) {
            boolean isSuccess = this.paxosCoordinator.createReplicaGroup(serviceName, epoch, state, nodes);
            assert isSuccess : "failed to create default services";
            this.serviceCoordinator.put(serviceName, this.paxosCoordinator);
            return true;
        }

        if (epoch == 0) {
            return this.initializeReplicaGroup(serviceName, state, nodes);
        }

        throw new RuntimeException("reconfiguration with epoch > 0 is unimplemented");
    }

    private boolean initializeReplicaGroup(String serviceName,
                                           String initialState,
                                           Set<NodeIDType> nodes) {
        System.out.printf(">> %s:XDNReplicaCoordinator - initializeReplicaGroup name=%s, state=%s, nodes=%s\n",
                myNodeID, serviceName, initialState, nodes);

        String validInitialStatePrefix = "xdn:init:";
        assert initialState.startsWith(validInitialStatePrefix) : "incorrect initial state prefix";
        String serviceProperties = initialState.substring(validInitialStatePrefix.length());
        var coordinator = inferCoordinatorByProperties(serviceProperties);
        assert coordinator != null :
                "XDN does not know what coordinator to be used for the specified service";

        boolean isSuccess = coordinator.createReplicaGroup(serviceName, 0, initialState, nodes);
        assert isSuccess : "failed to initialize service";
        this.serviceCoordinator.put(serviceName, coordinator);
        return true;
    }

    private AbstractReplicaCoordinator<NodeIDType> inferCoordinatorByProperties(String prop) {
        ServiceProperty sp = null;
        try {
            sp = ServiceProperty.createFromJSONString(prop);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }

        // for non-deterministic service we always use primary-backup
        if (!sp.isDeterministic()) {
            return this.primaryBackupCoordinator;
        }

        // for deterministic service, we have more options based on the consistency model
        // but for now we just use paxos for all consistency model
        // TODO: introduce new coordinator for different consistency models.
        else {
            switch (sp.getConsistencyModel()) {
                case LINEARIZABILITY,
                        SEQUENTIAL,
                        EVENTUAL,
                        READ_YOUR_WRITES,
                        WRITES_FOLLOW_READS,
                        MONOTONIC_READS,
                        MONOTONIC_WRITES -> {
                    return this.paxosCoordinator;
                }
                default -> {
                    return null;
                }
            }
        }
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        System.out.printf(">> %s:XDNReplicaCoordinator - getReplicaGroup name=%s\n",
                myNodeID, serviceName);
        var coordinator = this.serviceCoordinator.get(serviceName);
        if (coordinator == null) {
            return null;
        }
        return coordinator.getReplicaGroup(serviceName);
    }

}
