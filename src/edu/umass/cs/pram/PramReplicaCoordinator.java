package edu.umass.cs.pram;

import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.pram.packets.*;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import edu.umass.cs.xdn.interfaces.behavior.ReadOnlyRequest;
import edu.umass.cs.xdn.interfaces.behavior.WriteOnlyRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * PramReplicaCoordinator is a generic class to handle replica node of type NodeIDType to replicate
 * application using the PRAM protocol, offering PRAM/FIFO consistency model. The protocol is
 * implemented based on
 * <a href="https://www.cs.princeton.edu/techreports/1988/180.pdf">this paper</a>.
 *
 * @param <NodeIDType>
 */
public class PramReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private final NodeIDType myNodeID;
    private final Stringifiable<NodeIDType> nodeIdStringer;
    private final Set<IntegerPacketType> requestTypes;

    private final Messenger<NodeIDType, JSONObject> messenger;

    private record PramInstance<NodeIDType>(String serviceName, int currentEpoch,
                                            String initStateSnapshot, Set<NodeIDType> nodes,
                                            ConcurrentMap<NodeIDType, ConcurrentLinkedQueue<PramWriteAfterPacket>> replicaQueue) {
    }

    private final ConcurrentMap<String, PramInstance<NodeIDType>> currentInstances;

    public PramReplicaCoordinator(Replicable app,
                                  NodeIDType myID,
                                  Stringifiable<NodeIDType> nodeIdStringer,
                                  Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);
        assert nodeIdStringer != null : "nodeIdStringer cannot be null";
        this.myNodeID = myID;
        this.nodeIdStringer = nodeIdStringer;
        this.messenger = messenger;
        this.currentInstances = new ConcurrentHashMap<>();

        // initialize all the supported request type
        Set<IntegerPacketType> types = new HashSet<>(app.getRequestTypes());
        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        types.add(PramPacketType.PRAM_PACKET);
        types.addAll(List.of(PramPacketType.values()));
        this.requestTypes = types;

        // prepare parser for PramPacket
        this.setGetRequestImpl(new AppRequestParser() {
            @Override
            public Request getRequest(String stringified) throws RequestParseException {
                return PramPacket.createFromString(stringified, (AppRequestParser) app);
            }
            @Override
            public Set<IntegerPacketType> getRequestTypes() {
                return types;
            }
        });

        System.out.printf(">> PramReplicaCoordinator - initialization at node %s\n", this.myNodeID);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return requestTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        System.out.println(">> " + myNodeID + " PramReplicaCoordinator -- receiving request " +
                request.getClass().getSimpleName());
        if (!(request instanceof ReplicableClientRequest) && !(request instanceof PramPacket)) {
            throw new RuntimeException("Unknown request/packet handled by PramReplicaCoordinator");
        }

        // Convert the incoming Request into PramPacket
        PramPacket packet;
        if (request instanceof ReplicableClientRequest rcr &&
                rcr.getRequest() instanceof ClientRequest clientRequest) {
            boolean isWriteOnly = (clientRequest instanceof BehavioralRequest br && br.isWriteOnlyRequest());
            boolean isReadOnly = (clientRequest instanceof BehavioralRequest br && br.isReadOnlyRequest());
            if (isReadOnly) {
                packet = new PramReadPacket(clientRequest);
            } else if (isWriteOnly) {
                packet = new PramWritePacket(clientRequest);
            } else {
                throw new RuntimeException("PramReplicaCoordinator can only handle " +
                        "WriteOnlyRequest or ReadOnlyRequest");
            }
        } else if (request instanceof ReplicableClientRequest rcr &&
                rcr.getRequest() instanceof PramPacket pp){
            packet = pp;
        } else {
            assert request instanceof PramPacket :
                    "The received request must be ReplicableClientRequest or PramPacket";
            packet = (PramPacket) request;
        }

        return handlePramPacket(packet, callback);
    }

    private boolean handlePramPacket(PramPacket packet, ExecutedCallback callback) {
        if (packet instanceof PramReadPacket p) {
            System.out.println(">> handling read request ...");
            ClientRequest readRequest = p.getClientReadRequest();
            boolean isExecSuccess = app.execute(readRequest);
            if (isExecSuccess) {
                callback.executed(readRequest, true);
            }
            return isExecSuccess;
        }

        if (packet instanceof PramWritePacket p) {
            System.out.println(">> handling write request ...");
            ClientRequest writeRequest = p.getClientWriteRequest();
            boolean isExecSuccess = app.execute(writeRequest);
            if (isExecSuccess) {
                callback.executed(writeRequest, true);
            }

            // Send WRITE_AFTER packets to all replicas but myself.
            String serviceName = writeRequest.getServiceName();
            assert this.currentInstances.containsKey(serviceName) :
                    "Unknown service name " + serviceName;
            Set<NodeIDType> nodes = this.currentInstances.get(serviceName).nodes();
            nodes.remove(myNodeID);
            PramPacket writeAfterPacket = new PramWriteAfterPacket(myNodeID.toString(), writeRequest);
            GenericMessagingTask<NodeIDType, PramPacket> m =
                    new GenericMessagingTask<>(nodes.toArray(), writeAfterPacket);
            try {
                System.out.println("Sending WRITE_AFTER packet ...");
                messenger.send(m);
            } catch (JSONException | IOException e) {
                throw new RuntimeException(e);
            }

            return isExecSuccess;
        }

        if (packet instanceof PramWriteAfterPacket p) {
            System.out.println("handling write after packet ... " + p);

            // get the sender ID
            final String senderIdString = p.getSenderID();
            final NodeIDType senderID = nodeIdStringer.valueOf(senderIdString);
            assert senderID != null : "Failed to convert string into NodeIDType";

            // find the service using its name
            String serviceName = p.getServiceName();
            assert this.currentInstances.containsKey(serviceName) :
                    "Unknown service name " + serviceName;
            PramInstance<NodeIDType> instance = this.currentInstances.get(serviceName);

            // find the sender's queue
            instance.replicaQueue.putIfAbsent(senderID, new ConcurrentLinkedQueue<>());
            Queue<PramWriteAfterPacket> replicaQueue = instance.replicaQueue.get(senderID);
            boolean isAdded = replicaQueue.add(p);
            assert isAdded :  "Failed to enqueue the write request from " + senderIdString;

            // spawn a thread to execute all the write requests in the FIFO queue
            Thread writeExecutor = new Thread(() -> {
                synchronized (replicaQueue) {
                    while (!replicaQueue.isEmpty()) {
                        PramWriteAfterPacket writePacket = replicaQueue.remove();
                        ClientRequest appRequest = writePacket.getClientWriteRequest();
                        boolean isExecuted = app.execute(appRequest);
                        assert isExecuted :
                                "failed to execute write request from " + senderIdString;
                    }
                }
            });
            writeExecutor.start();

            return true;
        }

        throw new IllegalStateException("Unexpected PramPacket: " + packet.getRequestType());
    }

    @Override
    public boolean createReplicaGroup(String serviceName,
                                      int epoch,
                                      String state,
                                      Set<NodeIDType> nodes) {
        System.out.println(">> " + myNodeID + " createReplicaGroup " + serviceName + " " + nodes);
        PramInstance<NodeIDType> pramInstance =
                new PramInstance<>(serviceName, epoch, state, nodes, new ConcurrentHashMap<>());
        this.currentInstances.put(serviceName, pramInstance);
        return true;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        if (!this.currentInstances.containsKey(serviceName)) return true;
        PramInstance<NodeIDType> pramInstance = this.currentInstances.get(serviceName);
        if (pramInstance.currentEpoch != epoch) return true;
        this.currentInstances.remove(serviceName);
        return true;
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        PramInstance<NodeIDType> pramInstance = this.currentInstances.get(serviceName);
        return pramInstance != null ? pramInstance.nodes() : null;
    }
}
