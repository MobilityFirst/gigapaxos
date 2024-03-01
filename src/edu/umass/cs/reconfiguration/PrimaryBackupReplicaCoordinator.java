package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceDestructionException;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

public class PrimaryBackupReplicaCoordinator<NodeIDType>
        extends AbstractReplicaCoordinator<NodeIDType> {

    private final PaxosManager<NodeIDType> paxosManager;
    private final Set<IntegerPacketType> requestTypes;

    public PrimaryBackupReplicaCoordinator(
            Replicable app,
            NodeIDType myID,
            Stringifiable<NodeIDType> unstringer,
            Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);

        System.out.println(">> PrimaryBackupReplicaCoordinator - initialization");

        // initialize the Paxos Manager for this Node
        this.paxosManager = new PaxosManager<NodeIDType>(myID, unstringer, messenger, this)
                .initClientMessenger(
                        new InetSocketAddress(
                                messenger.getNodeConfig().getNodeAddress(myID),
                                messenger.getNodeConfig().getNodePort(myID)),
                        messenger);

        // initialize all the request types handled
        Set<IntegerPacketType> types = new HashSet<>(app.getRequestTypes());
        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        this.requestTypes = types;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        System.out.println(">> PrimaryBackupReplicaCoordinator - getRequestTypes");
        return requestTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        System.out.println(">> coordinateRequest");
        System.out.println(">> myID:" + getMyID() + " isActive? " +
                this.paxosManager.isActive(request.getServiceName()));

        boolean isActiveCoordinator = this.paxosManager.isActive(request.getServiceName());
        if (isActiveCoordinator) {
            return executeRequestCoordinateStatediff(request, callback);
        } else {
            // TODO: forward request to active OR send error to client
        }

        return true;
    }

    private boolean executeRequestCoordinateStatediff(Request request, ExecutedCallback callback) {
        this.app.execute(request);
        // TODO: capture the statediff, coordinate the statediff.
        this.paxosManager.propose(request.getServiceName(), request, callback);
        return true;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes) {
        System.out.println(">> PrimaryBackupCoor - createReplicaGroup | serviceName:" + serviceName
                + " epoch:" + epoch
                + " state:" + state
                + " nodes:" + nodes);

        boolean created = this.paxosManager.createPaxosInstanceForcibly(serviceName,
                epoch, nodes, this, state, 0);
        boolean alreadyExist = this.paxosManager.equalOrHigherVersionExists(serviceName, epoch);

        if (!created && !alreadyExist) {
            throw new PaxosInstanceCreationException((this
                    + " failed to create " + serviceName + ":" + epoch
                    + " with state [" + state + "]") + "; existing_version=" +
                    this.paxosManager.getVersion(serviceName));
        }

        return true;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        System.out.println(">> deleteReplicaGroup - " + serviceName);
        return this.paxosManager.deleteStoppedPaxosInstance(serviceName, epoch);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        System.out.println(">> getReplicaGroup - " + serviceName);
        return this.paxosManager.getReplicaGroup(serviceName);
    }
}
