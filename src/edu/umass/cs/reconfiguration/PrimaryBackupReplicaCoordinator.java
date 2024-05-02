package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.primarybackup.PrimaryBackupManager;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.primarybackup.packets.PrimaryBackupPacket;
import edu.umass.cs.primarybackup.packets.RequestPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * PrimaryBackupReplicaCoordinator handles replica groups that use primary-backup, which
 * only allow request execution in the primary, leaving other replicas as backups.
 * Generally, primary-backup is suitable to provide fault-tolerance for non-deterministic
 * services.
 * <p>
 * PrimaryBackupReplicaCoordinator uses PaxosManager to ensure all replicas agree on the
 * order of stateDiffs, generated from each request execution.
 * <p>
 *
 * @param <NodeIDType>
 */
public class PrimaryBackupReplicaCoordinator<NodeIDType>
        extends AbstractReplicaCoordinator<NodeIDType> {

    private final PrimaryBackupManager<NodeIDType> pbManager;
    private final Set<IntegerPacketType> requestTypes;

    public PrimaryBackupReplicaCoordinator(Replicable app,
                                           NodeIDType myID,
                                           Stringifiable<NodeIDType> unstringer,
                                           Messenger<NodeIDType, JSONObject> messenger,
                                           boolean omitClientMessengerInitialization) {
        super(app, messenger);

        // the Replicable application used for PrimaryBackupCoordinator must also implement
        // BackupableApplication interface.
        assert app instanceof BackupableApplication;

        // initialize the PrimaryBackupManager, including the PaxosManager inside it.
        this.pbManager = new PrimaryBackupManager<>(
                myID,
                app,
                (BackupableApplication) app,
                unstringer,
                messenger,
                omitClientMessengerInitialization
        );

        // initialize all the request types handled
        Set<IntegerPacketType> types = new HashSet<>(app.getRequestTypes());
        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        types.addAll(PrimaryBackupManager.getAllPrimaryBackupPacketTypes());
        this.requestTypes = types;

        // update the coordinator request parser
        this.setGetRequestImpl(this.pbManager);
    }

    public PrimaryBackupReplicaCoordinator(Replicable app,
                                           NodeIDType myID,
                                           Stringifiable<NodeIDType> unstringer,
                                           Messenger<NodeIDType, JSONObject> messenger) {
        this(app, myID, unstringer, messenger, false);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return requestTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        ExecutedCallback chainedCallback = callback;

        // if packet comes from client (i.e., ReplicableClientRequest), wrap the
        // containing request with RequestPacket, and re-chain the callback.
        // Nvm, ReplicableClientRequest can contain other PrimaryBackupPacket :(
        if (request instanceof ReplicableClientRequest rcr) {
            boolean isEndUserRequest = (rcr.getRequest() instanceof ClientRequest);

            if (isEndUserRequest) {
                ClientRequest appRequest = (ClientRequest) rcr.getRequest();
                request = new RequestPacket(
                        rcr.getServiceName(),
                        appRequest.toString().getBytes(StandardCharsets.ISO_8859_1));
                chainedCallback = (executedRequestPacket, handled) -> {
                    assert executedRequestPacket instanceof RequestPacket;
                    RequestPacket response = (RequestPacket) executedRequestPacket;
                    callback.executed(response.getResponse(), handled);
                };
            }

            if (!isEndUserRequest) {
                request = rcr.getRequest();
            }
        }

        if (request instanceof PrimaryBackupPacket packet) {
            return this.pbManager.handlePrimaryBackupPacket(packet, chainedCallback);
        }

        // printout a helpful exception message by showing the possible acceptable packets
        StringBuilder requestTypeString = new StringBuilder();
        for (IntegerPacketType p : this.app.getRequestTypes()) {
            requestTypeString.append(p.toString()).append(" ");
        }
        throw new RuntimeException(String.format(
                "Unknown request of class '%s' for Primary Backup Coordinator. " +
                        "Request must use either %s, %s, or one of the app request types: %s.",
                request.getClass().getSimpleName(),
                ReplicableClientRequest.class.getSimpleName(),
                PrimaryBackupPacket.class.getSimpleName(),
                requestTypeString.toString()));
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes) {
        assert serviceName != null && !serviceName.isEmpty();
        assert epoch >= 0;
        assert !nodes.isEmpty();
        return this.pbManager.createPrimaryBackupInstance(serviceName, epoch, state, nodes);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        return this.pbManager.deletePrimaryBackupInstance(serviceName, epoch);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return this.pbManager.getReplicaGroup(serviceName);
    }

    public AppRequestParser getRequestParser() {
        return this.pbManager;
    }

    public PrimaryBackupManager<NodeIDType> getPrimaryBackupManager() {
        return this.pbManager;
    }

    public final void close() {
        this.stop();
        this.pbManager.stop();
    }

}
