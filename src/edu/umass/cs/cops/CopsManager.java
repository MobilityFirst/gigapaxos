package edu.umass.cs.cops;

import edu.umass.cs.cops.examples.KeyValueAppRequest;
import edu.umass.cs.cops.packets.CopsPacket;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

public class CopsManager<NodeIDType> {

    private record CopsInstance<NodeIDType>(String serviceName, int currentEpoch,
                                            String initStateSnapshot, Set<NodeIDType> nodes) {
    }

    private final NodeIDType myNodeID;
    private final Replicable app;
    private final Stringifiable<NodeIDType> nodeIDStringifier;
    private final Messenger<NodeIDType, JSONObject> messenger;

    private final Map<String, CopsInstance<NodeIDType>> currentInstances = new HashMap<>();
    private LamportTimestamp currentTimestamp;

    private List<CopsPacket> toBeSynchronizedRequests = new ArrayList<>();
    private List<CopsPacket> toBeExecutedRequests = new ArrayList<>();

    public CopsManager(Replicable app, NodeIDType myID, Stringifiable<NodeIDType> nodeIDStringifier,
                       Messenger<NodeIDType, JSONObject> messenger) {
        this.app = app;
        this.myNodeID = myID;
        this.nodeIDStringifier = nodeIDStringifier;
        this.messenger = messenger;
        this.currentTimestamp = new LamportTimestamp(this.myNodeID.hashCode());
        System.out.println("CopsManager - initialization");
    }

    public boolean handleCopsPacket(CopsPacket packet, ExecutedCallback executedCallback) {
        assert packet != null : "unknown packet";

        System.out.println(">> " + myNodeID + " handling " + packet.isClientRequest() + " " + packet.toString());

        // Case 1: client's request.
        //  - execute all request locally
        //  - buffer blind-write requests to async queue.
        if (packet.isClientRequest()) {
            Request clientRequest = packet.getClientRequest().getRequest();

            // TODO: need to ensure all dependencies are executed
            app.execute(clientRequest);
            executedCallback.executed(clientRequest, true);

            // TODO: send put_after, if a put request
            if (clientRequest instanceof KeyValueAppRequest appRequest &&
                    appRequest.isWriteRequest()) {
                Set<NodeIDType> nodes = this.getCopsInstance(packet.getServiceName());
                nodes.remove(myNodeID);
                CopsPacket putAfterPacket =
                        CopsPacket.createPutAfterPacket(packet.getServiceName());
                GenericMessagingTask<NodeIDType, CopsPacket> m =
                        new GenericMessagingTask<>(nodes.toArray(), putAfterPacket);
                try {
                    messenger.send(m);
                } catch (IOException | JSONException e) {
                    throw new RuntimeException(e);
                }
            }

            return true;
        }

        // how the dependency is specified in COPS?
        // Case 2: put_after packet
        //  -

        return true;
    }

    public boolean createCopsInstance(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes) {
        System.out.println(">> " + myNodeID + " createCopsInstance " + serviceName + " " + nodes);
        CopsInstance<NodeIDType> copsInstance =
                new CopsInstance<>(serviceName, epoch, state, nodes);
        this.currentInstances.put(serviceName, copsInstance);
        return true;
    }

    public boolean deleteCopsInstance(String serviceName, int epoch) {
        if (!this.currentInstances.containsKey(serviceName)) return true;
        CopsInstance<NodeIDType> copsInstance = this.currentInstances.get(serviceName);
        if (copsInstance.currentEpoch != epoch) return true;
        this.currentInstances.remove(serviceName);
        return true;
    }

    public Set<NodeIDType> getCopsInstance(String serviceName) {
        CopsInstance<NodeIDType> copsInstance = this.currentInstances.get(serviceName);
        return copsInstance != null ? copsInstance.nodes() : null;
    }
}
