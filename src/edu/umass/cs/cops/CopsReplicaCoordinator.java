package edu.umass.cs.cops;

import edu.umass.cs.cops.packets.CopsPacket;
import edu.umass.cs.cops.packets.CopsPacketType;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CopsReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private final NodeIDType myNodeID;
    private final CopsManager<NodeIDType> copsManager;
    private final Set<IntegerPacketType> requestTypes;

    public CopsReplicaCoordinator(Replicable app,
                                  NodeIDType myID,
                                  Stringifiable<NodeIDType> nodeIDStringifier,
                                  Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);
        this.myNodeID = myID;
        this.copsManager = new CopsManager<NodeIDType>(app, myID, nodeIDStringifier, messenger);

        // initialize all the request types handled
        Set<IntegerPacketType> types = new HashSet<>(app.getRequestTypes());
        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        types.add(CopsPacketType.COPS_PACKET);
        this.requestTypes = types;

        System.out.printf(">> CopsReplicaCoordinator - initialization at node %s\n", this.myNodeID);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return this.requestTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        if (!(request instanceof ReplicableClientRequest) && !(request instanceof CopsPacket)) {
            throw new RuntimeException("Unknown request handled by CopsReplicaCoordinator");
        }

        if (request instanceof ReplicableClientRequest r) {
            return this.copsManager.handleCopsPacket(new CopsPacket(r), callback);
        }

        CopsPacket cp = (CopsPacket) request;
        return this.copsManager.handleCopsPacket(cp, callback);
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes) {
        return this.copsManager.createCopsInstance(serviceName, epoch, state, nodes);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        return this.copsManager.deleteCopsInstance(serviceName, epoch);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return this.copsManager.getCopsInstance(serviceName);
    }
}
