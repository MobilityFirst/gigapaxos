/* Copyright (c) 2015 University of Massachusetts
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * Initial developer(s): Z. Gao */

package edu.umass.cs.chainreplication;

import edu.umass.cs.chainreplication.chainpackets.ChainPacket;
import edu.umass.cs.chainreplication.chainpackets.ChainRequestPacket;
import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;

import edu.umass.cs.gigapaxos.AbstractPaxosLogger;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.*;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.InterfaceNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Z Gao
 *
 * <p>
 *     ChainManager is the primary class to manage and use chain replication.
 *
 *     ChainManager manages all chains at a node. There is supposed to be one
 *     chain manager per machine.
 *
 *     ChainManager does not support hot swap as described in GigaPaxos paper.
 *
 * </p>
 */
public class ChainManager<NodeIDType> {

    // final
    private final PaxosMessenger<NodeIDType> messenger; // messaging
    private final int myID;
    private final Replicable myApp; // default app for all chainIDs

    private final HashMap<String, ReplicatedChainStateMachine> replicatedChains;
    // a map of NodeIDType objects to integers
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();

    private final ChainOutstading outstanding;

    // private final FailureDetection<NodeIDType> FD; // failure detection

    private static final Level level = Level.INFO;

    // checkpoint
    private final LargeCheckpointer largeCheckpointer;
    private final boolean nullCheckpointsEnabled;

    private final AbstractPaxosLogger logger; // logging

    private boolean closed = false;

    private final Stringifiable<NodeIDType> unstringer;

    private static final Logger log = Logger.getLogger(ReconfigurationConfig.class
            .getName());

    private synchronized boolean isClosed(){
        return closed;
    }

    public ChainManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
                        InterfaceNIOTransport<NodeIDType, JSONObject> niot, Replicable ci,
                        String logFolder, boolean enableNullCheckpoints){

        this.myID = this.integerMap.put(id);
        this.unstringer = unstringer;

        this.largeCheckpointer = new LargeCheckpointer(logFolder,
                id.toString());
        this.myApp = LargeCheckpointer.wrap(ci, largeCheckpointer);

        // TODO: Failure detector
        // this.FD = new FailureDetection<NodeIDType>(id, niot, logFolder);

        this.replicatedChains = new HashMap<>();

        // though the class is called PaxosMessenger, as stated in its document
        // it is just a JSONMessenger which has nothing paxos-specific
        this.messenger = (new PaxosMessenger<NodeIDType>(niot, this.integerMap));

        // TODO: implement a new logger for chain replication
        this.logger = null;

        this.nullCheckpointsEnabled = enableNullCheckpoints;
        
        this.outstanding = new ChainOutstading();

        niot.precedePacketDemultiplexer(new ChainDemultiplexer(this));
    }

    /**
     * @return NodeIDType of this.
     */
    public NodeIDType getNodeID() {
        return this.messenger.getMyID();
    }

    /**
     * The Integer return value as opposed to int is convenient to say that
     * there is no epoch.
     *
     * @param chainID
     * @return Integer version of paxos instance named {@code chainID}.
     */
    public int getVersion(String chainID) {

        ReplicatedChainStateMachine crsm = this.getInstance(chainID);
        if ( crsm != null)
            return (int) crsm.getVersion();

        // FIXME: get from logger
        // return this.paxosLogger.getEpochFinalCheckpointVersion(paxosID);
        return -1;
    }


    /**
     * handleChainPacket method abandoned the design in {@link PaxosManager} handlePaxosPacket,
     * which creates MessageTasks to process the current Request packet.
     * We
     * @param cp
     * @param rcsm
     */
    private void handleChainPacket(ChainPacket cp, ReplicatedChainStateMachine rcsm){
        // no need to check whether this manager is closed as it has already been checked

        ChainPacket.ChainPacketType packetType = cp != null ? cp.getType()
                : ChainPacket.ChainPacketType.NO_TYPE;

        // forward to the next node in the chain
        log.log(level, "ChainManager.handleChainPacket: members: {0}, + myID(NodeIDType):{1}, myID:{2}, request:{3}",
                new Object[]{this.integerMap.getIntArrayAsNodeSet(rcsm.getMembers()),
                        this.myID,
                        this.integerMap.get(this.myID),
                        (ChainRequestPacket) cp
        });

        switch(packetType) {
            case REQUEST:
                // node -> head
                handleChainRequest(cp, rcsm);
                break;
            case FORWARD:
                // node -> next
                handleForwardRequest(cp, rcsm);
                break;
            case ACK:
                // node -> prev
                handleAckRequest(cp, rcsm);
                break;

            case RESPONSE:
                // entry -> client
                handleResponse(cp, rcsm);
                break;

            case READ:
                // node -> tail (primary)
                handleRead(cp, rcsm);
                break;
            default:
                break;
        }

    }

    /**
     * Check whether myID is the same as the head node ID.
     * If so, call handleForward to forward the replication request.
     * Otherwise, forward the request to the head node.
     */
    private void handleChainRequest(
            ChainPacket cp, ReplicatedChainStateMachine rcsm){
        if ( ((ChainRequestPacket) cp).getEntryReplica() == IntegerMap.NULL_INT_NODE )
            ((ChainRequestPacket) cp).setEntryReplica(this.myID);

        log.log(level, "ChainManager.handleChainRequest handles " +
                "ChainPacket {0} for state machine {1}", new Object[]{cp, rcsm});

        if (rcsm.getChainHead() == this.myID) {
            ((ChainRequestPacket) cp).setPacketType(ChainPacket.ChainPacketType.FORWARD);
            this.handleForwardRequest(cp, rcsm);
        } else {
            // otherwise, forward the request to head
            this.sendRequest(cp, rcsm.getChainHead());
        }
    }

    /**
     * The request coordination must start from the head of the chain.
     *
     * @param cp
     * @param rcsm
     */
    private void handleForwardRequest(
            ChainPacket cp, ReplicatedChainStateMachine rcsm) {

        log.log(level, "ChainManager.handleForward handles " +
                "ChainPacket {0} for state machine {1}",
                new Object[]{cp, rcsm});

        // TODO: log decision
        //PaxosPacket pp = convertToPaxosPacketForLogging(cp);
        //logger.log(pp);

        // Execute request before forward or ack
        Request request = getInterfaceRequest(this.myApp, ((ChainRequestPacket) cp).requestValue );

        // System.out.println("About to execute request:" + request);
        this.myApp.execute(request, false);

        if(rcsm.getChainTail() == this.myID){
            // this is the tail, send back ACK to the head
            ((ChainRequestPacket ) cp).setPacketType(ChainPacket.ChainPacketType.ACK);
            this.sendRequest(cp, rcsm.getChainHead());
        } else {
            this.sendRequest(cp, rcsm.getNext());
        }

    }

    private void handleAckRequest(ChainPacket cp,
                                  ReplicatedChainStateMachine rcsm) {

        ((ChainRequestPacket ) cp).setPacketType(ChainPacket.ChainPacketType.RESPONSE);

        if(rcsm.getChainHead() == this.myID){
            // this is the head, ACK received
            log.log(level, "ChainManager.handleAck received an ACK request {0}," +
                            " for state machine {1}",
                    new Object[]{cp, rcsm});

            ChainRequestPacket crp = (ChainRequestPacket) cp;

            if (crp.getEntryReplica() == this.myID){
                // the head is the entry node, send back response to client here
                // System.out.println("Head is entry, to respond!");
                this.handleResponse(cp, rcsm);
            } else {
                // otherwise, send to entry replica
                // System.out.println("Send to entry to respond!");

                this.sendRequest(cp, crp.getEntryReplica());
            }

        } else {
            // this node is not supposed to receive this request
            log.log(level, "ChainManager.handleAck received an ACK request {0}" +
                    " for state machine {1}",
                    new Object[]{cp, rcsm});
            this.handleResponse(cp, rcsm);
        }

    }

    private void handleResponse(ChainPacket cp,
                                ReplicatedChainStateMachine rcsm){

        // About to send response back to client
        long requestID = ((ChainRequestPacket) cp).requestID;
        ChainRequestAndCallback requestAndCallback =
                outstanding.dequeue((ChainRequestPacket) cp);
        log.log(level,
                "ChainManager.handleResponse: find ChainRequestAndCallback {0} for request ID {1}",
                new Object[]{requestAndCallback, requestID});

        if (requestAndCallback != null && requestAndCallback.callback != null) {

            Request request = getInterfaceRequest(this.myApp, requestAndCallback.chainRequestPacket.requestValue);

            // System.out.println("About to execute request:" + request);
            // this.myApp.execute(request, false);

            // System.out.println("About to respond:" + request);

            // Send response back to client
            requestAndCallback.callback.executed(request
                    , true);

        } else {
            // can't find the request being queued in outstanding
            log.log(Level.WARNING, "ChainManager.handleResponse received " +
                    "an ACK request {0} for RSM {1} that does not match any enqueued request.",
                    new Object[]{cp, rcsm});
        }
    }

    /**
     * Read should be sent to the tail directly as uncoordinated request.
     *
     * @param cp
     * @param rcsm
     */
    private void handleRead(ChainPacket cp,
                            ReplicatedChainStateMachine rcsm){

        ChainRequestPacket crp = (ChainRequestPacket) cp;

        if ( crp.getEntryReplica() == IntegerMap.NULL_INT_NODE ) {
            crp.setEntryReplica(this.myID);
            crp.setPacketType(ChainPacket.ChainPacketType.READ);
        }

        if(rcsm.getChainTail() == this.myID && crp.getEntryReplica() == this.myID){
            // About to send back request to the client
            ChainRequestAndCallback requestAndCallback =
                    outstanding.dequeue(crp);
            //
            if (requestAndCallback != null && requestAndCallback.callback != null) {

                Request request = getInterfaceRequest(this.myApp, requestAndCallback.chainRequestPacket.requestValue);

                // execute "READ" request on the tail
                this.myApp.execute(request, false);
                
                // Send response back to client
                requestAndCallback.callback.executed(request
                        , true);
            }
        } else if (rcsm.getChainTail() == this.myID) {

            Request request = getInterfaceRequest(this.myApp, crp.requestValue);
            this.myApp.execute(request, false);

            ((ChainRequestPacket) cp).setPacketType(ChainPacket.ChainPacketType.RESPONSE);

            this.sendRequest(cp, crp.getEntryReplica());
        } else {
            // otherwise, forward the request to the tail
            this.sendRequest(cp, rcsm.getChainTail());
        }

    }

    private static final Request getInterfaceRequest(Replicable app, String value) {
        try {
            return app.getRequest(value);
        } catch (RequestParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Once a failure is detected, we can send a request through this method.
     * TODO: need a new demand profile for chain replication reconfiguration
     */
    private void sendDemandProfileToReconfigurator(AbstractDemandProfile demand)
            throws IOException, JSONException {
        String chainID = demand.getName();
        NodeIDType reportee = selectReconfigurator(chainID);
        assert (reportee != null);
        Integer epoch = ((AbstractReplicaCoordinator) myApp).getEpoch(chainID);
        GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, Object>(
                reportee, (new DemandReport<NodeIDType>(this.messenger.getMyID(),
                demand.getName(), (epoch == null ? 0 : epoch),
                demand)));
        this.messenger.send(mtask);

    }

    private NodeIDType selectReconfigurator(String chainID) {
        Set<NodeIDType> reconfigurators = this.getReconfigurators();
        return (NodeIDType) Util.selectRandom(reconfigurators);
    }

    private Set<NodeIDType> getReconfigurators(){
        return ((DefaultNodeConfig<NodeIDType>) this.unstringer).getReconfigurators();
    }
    
    /**
     * Used to send ChainRequest to
     * @param cp
     * @param nodeID
     */
    private void sendRequest(ChainPacket cp,
                             int nodeID){
        GenericMessagingTask<NodeIDType,?> gTask = null;
        try {
            // forward to nodeID
            gTask = new GenericMessagingTask(this.integerMap.get(nodeID),
                    cp);
            this.messenger.send(gTask);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public String propose(String chainID, Request request,
                           ExecutedCallback callback) {

        ChainRequestPacket chainRequestPacket = this.getChainRequestPacket(request, false);

        log.log(level, "ChainManager.propsoe request " +
                "{0} for service name {1}",
                new Object[]{chainRequestPacket, chainID});

        // 1 Check whether ChainManager is closed, if yes, return null
        if(this.isClosed())
            return null;

        boolean matched = false;

        ReplicatedChainStateMachine rcsm = this.getInstance(chainID);

        log.log(level, "ChainManager.propsoe retrieves ReplicatedChainStateMachine {0}",
                new Object[]{rcsm});
        if (rcsm != null) {
            matched = true;
            // put chainID and version into request
            chainRequestPacket.putChainIDAndVersion(chainID, rcsm.getVersion());
            this.outstanding.enqueue(
                    new ChainRequestAndCallback(chainRequestPacket, callback)
            );

            log.log(level, "ChainManager.propsoe starts handling ChainRequestPacket:{0}",
                    new Object[]{chainRequestPacket});

            this.handleChainPacket(chainRequestPacket, rcsm);
        } else {
            log.log(Level.INFO, "{0} could not find paxos instance {1} for " +
                    "request {2} with body {3}; last known version was [{4}]",
                    new Object[] {
                            this,
                            chainID,
                            chainRequestPacket.getSummary(),
                            Util.truncate(chainRequestPacket.getRequestValues()[0],
                                    64), this.getVersion(chainID)
                    });

        }


        return matched ? rcsm.getChainIDVersion() : null;
    }

    public boolean createReplicatedChainForcibly(String chainID, int version,
                                                 Set<NodeIDType> nodes, Replicable app,
                                                 String state){
        // long timeout = Config.getGlobalInt(PaxosConfig.PC.CAN_CREATE_TIMEOUT);
        return this.createReplicatedChainFinal(chainID, version, nodes, app, state) != null;
    }

    public boolean deleteReplicatedChain(String chainID, int epoch){
        ReplicatedChainStateMachine rcsm = this.getInstance(chainID);
        if(rcsm == null)
            return true;
        if(rcsm.getVersion() > epoch) {
            // exist a higher version, can't delete the state machine
            return false;
        }
        return this.removeInstance(chainID);
    }

    private synchronized ReplicatedChainStateMachine createReplicatedChainFinal(
            String chainID, int version, Set<NodeIDType> nodes,
            Replicable app, String initialState) {
        if (this.isClosed())
            return null;

        if (!nodes.contains(this.getNodeID()))
            throw new ReplicatedChainException(this.getNodeID()
                    + " can not create a replicated chain for group " + nodes
                    + " to which it does not belong");

        // TODO: recover and version check

        ReplicatedChainStateMachine rcsm = this.getInstance(chainID);
        if (rcsm != null)
            return rcsm;


        try {
            // else try to create (could still run into exception)
            rcsm = new ReplicatedChainStateMachine(chainID, version, myID,
                    this.integerMap.put(nodes), app != null ? app : this.myApp,
                    initialState, this);
            log.log(level, "Create replicated chain state machine for " +
                            "chainID {0}: {1} on {2}:{3}",
                    new Object[] {chainID, rcsm, myID, this.integerMap.get(myID)});
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReplicatedChainException(e.getMessage());
        }

        this.putInstance(chainID, rcsm);
        this.integerMap.put(nodes);

        return rcsm;
    }

    private ChainRequestPacket getChainRequestPacket(Request request, boolean stop){

        if (request instanceof ChainRequestPacket
                && request.getRequestType().getInt() == ChainPacket.ChainPacketType.CHAIN_PACKET.getInt()){
            return (stop == ((ChainRequestPacket) request).stop) ? (ChainRequestPacket) request :
                    new ChainRequestPacket(((ChainRequestPacket) request).requestID,
                            ((ChainRequestPacket) request).requestValue, stop,
                            (ChainRequestPacket) request);
        } else if (request instanceof ClientRequest){
            return new ChainRequestPacket(
                    ((ClientRequest) request).getRequestID(),
                    ((ClientRequest) request).toString(), stop,
                    ((ClientRequest) request).getClientAddress()
            );
        }
        return new ChainRequestPacket(this.outstanding.generateUnusedID(),request.toString(), stop);
    }


    protected Replicable getApp() {
        return this.myApp;
    }

    private ReplicatedChainStateMachine getInstance(String chainID) {
        return replicatedChains.get(chainID);
    }

    private void putInstance(String chainID, ReplicatedChainStateMachine rcsm){
        this.replicatedChains.put(chainID, rcsm);
    }

    private boolean removeInstance(String chainID) {
        return this.replicatedChains.remove(chainID) != null;
    }

    public Set<NodeIDType> getReplicaGroup(String chainID) {
        ReplicatedChainStateMachine crsm = this.getInstance(chainID);
        if (crsm == null)
            return null;
        return this.integerMap.getIntArrayAsNodeSet(crsm.getMembers());
    }

    public static class ChainRequestAndCallback {
        // protected  ChainRequestPacket chainRequestPacket;
        protected ChainRequestPacket chainRequestPacket;
        final ExecutedCallback callback;

        ChainRequestAndCallback(ChainRequestPacket chainRequestPacket, ExecutedCallback callback){
            this.chainRequestPacket = chainRequestPacket;
            this.callback = callback;
        }

        @Override
        public String toString(){
            return this.chainRequestPacket +" ["+ callback+"]";
        }
    }

    /**
     * The class is used to keep track of outstanding requests that have not been
     * acknowledged.
     */
    private class ChainOutstading {
        int totalRequestSize = 0;
        ConcurrentHashMap<Long, ChainRequestAndCallback> requests = new ConcurrentHashMap<>();

        private void enqueue(ChainRequestAndCallback requestAndCallback) {
            assert (requestAndCallback.chainRequestPacket != null);
            ChainRequestAndCallback prev = null;
            synchronized (this.requests) {
                prev = this.requests.putIfAbsent(
                        requestAndCallback.chainRequestPacket.getRequestID(), requestAndCallback);
                if (prev != null){
                    // same request ID exists in outstanding already
                    log.log(Level.FINE,
                            "ChainManager: the same request ID {0} " +
                                    "already exists in outstanding for request: {1}",
                            new Object[] {requestAndCallback.chainRequestPacket.getRequestID(), requestAndCallback.chainRequestPacket} );
                }
            }
        }

        private ChainRequestAndCallback dequeue(ChainRequestPacket chainRequestPacket) {
            // ChainRequestAndCallback queued =
            return this.requests.remove(chainRequestPacket.getRequestID());
        }


        private long generateUnusedID() {
            Long requestID = null;
            do {
                requestID = (long) (Math.random() * Long.MAX_VALUE);
            } while (this.requests.containsKey(requestID));
            return requestID;
        }
    }

    /**
     * ChainPacket demultiplexier, the order can be found in AbstractPacketDemultiplexer
     * processHeader: convert a byte[] to ChainPacket
     * getPacketType: get packet type of a ChainPacket
     *
     */
    class ChainDemultiplexer extends AbstractPacketDemultiplexer<Object> {

        final ChainManager manager;

        public ChainDemultiplexer(int numThreads,
                                  ChainManager manager){
            super(numThreads);
            this.register(ChainPacket.ChainPacketType.CHAIN_PACKET);
            this.manager = manager;
        }

        public ChainDemultiplexer(ChainManager manager){
            this(Config.getGlobalInt(PaxosConfig.PC.PACKET_DEMULTIPLEXER_THREADS), manager);
        }

        @Override
        protected Integer getPacketType(Object message) {
            if (message instanceof net.minidev.json.JSONObject)
                return (Integer) ((net.minidev.json.JSONObject) message).get(JSONPacket.PACKET_TYPE);

            assert (message instanceof ChainPacket || message instanceof byte[]) : message;

            return (message instanceof byte[]) ? ByteBuffer.wrap((byte[]) message,
                    0, 4).getInt()
                    : ChainPacket.ChainPacketType.CHAIN_PACKET.getInt();
        }

        @Override
        protected Object processHeader(byte[] bytes, NIOHeader header) {

            if(!JSONPacket.couldBeJSON(bytes)) return bytes;

            ByteBuffer bbuf = ByteBuffer.wrap(bytes);
            ChainRequestPacket packet = null;
            try {
                packet = new ChainRequestPacket(bbuf);
            } catch (UnsupportedEncodingException | UnknownHostException e) {
                e.printStackTrace();
            }
            return packet;
        }


        private boolean isByteable(byte[] bytes) {
            ByteBuffer bbuf = ByteBuffer.wrap(bytes, 0, 0);
            return (bbuf.getInt()) == ChainPacket.ChainPacketType.CHAIN_PACKET.getInt()
                    && (bbuf.getInt() == ChainPacket.ChainPacketType.REQUEST.getInt()
                    || bbuf.getInt() == ChainPacket.ChainPacketType.FORWARD.getInt()
                    || bbuf.getInt() == ChainPacket.ChainPacketType.ACK.getInt());
        }

        /**
         * @param message
         * @return
         */
        @Override
        protected boolean matchesType(Object message) {
            return message instanceof net.minidev.json.JSONObject;
        }

        @Override
        public boolean handleMessage(Object message, NIOHeader header) {

            ChainManager.log.log(level, "ChainDemultiplexer.handleMessage: receive message {0}" +
                    " with NIOHeader {1}", new Object[]{message.toString(), header});

            assert (message != null);
            if (message instanceof net.minidev.json.JSONObject){
                // TODO:
                // ChainPacket cp = null;
                // ChainManager.this.handleChainPacket(cp, ChainManager.this.getInstance());
                return true;
            }

            if(message instanceof byte[]){
                ByteBuffer bbuf = ByteBuffer.wrap((byte[]) message);
                ChainRequestPacket packet = null;
                try {
                    packet = new ChainRequestPacket(bbuf);
                    this.manager.handleChainPacket(packet,
                            this.manager.getInstance(packet.getChainID()));
                    return true;
                } catch (UnsupportedEncodingException | UnknownHostException e) {
                    e.printStackTrace();
                }
            }

            try {
                // convert to ChainRequestPacket
                ChainRequestPacket packet = (ChainRequestPacket) message;

                this.manager.handleChainPacket(packet,
                        this.manager.getInstance(packet.getChainID()));
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }

            return false;
        }
    }

}
