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

import edu.umass.cs.chainreplication.chainpackets.ChainRequestPacket;
import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
import edu.umass.cs.gigapaxos.*;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.*;
import edu.umass.cs.nio.interfaces.InterfaceNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.utils.Config;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

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

    // checkpoint
    private final LargeCheckpointer largeCheckpointer;
    private final boolean nullCheckpointsEnabled;

    private final AbstractPaxosLogger logger; // logging

    private boolean closed = false;

    private synchronized boolean isClosed(){
        return closed;
    }

    public ChainManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
                        InterfaceNIOTransport<NodeIDType, JSONObject> niot, Replicable ci,
                        String logFolder, boolean enableNullCheckpoints){

        this.myID = this.integerMap.put(id);

        this.largeCheckpointer = new LargeCheckpointer(logFolder,
                id.toString());
        this.myApp = LargeCheckpointer.wrap(ci, largeCheckpointer);

        this.replicatedChains = new HashMap<>();

        // though the class is called PaxosMessenger, as stated in its document
        // it is just a JSONMessenger which has nothing paxos-specific
        this.messenger = (new PaxosMessenger<NodeIDType>(niot, this.integerMap));

        this.logger = new SQLPaxosLogger(this.myID, id.toString(),
                logFolder, this.messenger);

        this.nullCheckpointsEnabled = enableNullCheckpoints;


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

    public String propose(String chainID, Request request,
                          ExecutedCallback callback) {
        return this.propose(chainID, (ChainRequestPacket) request, callback);
    }

    private String propose(String chainID, ChainRequestPacket request,
                           ExecutedCallback callback) {
        // TODO: coordinate request based on the chain information
        System.out.println("Coordinate: "+request);
        return request.toString();
    }

    public boolean createReplicatedChainForcibly(String chainID, int version,
                                                 Set<NodeIDType> nodes, Replicable app,
                                                 String state){
        // long timeout = Config.getGlobalInt(PaxosConfig.PC.CAN_CREATE_TIMEOUT);
        return this.createReplicatedChainFinal(chainID, version, nodes, app, state) != null;
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
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReplicatedChainException(e.getMessage());
        }

        replicatedChains.put(chainID, rcsm);

        // this.integerMap.put(nodes);

        return rcsm;
    }


    private ReplicatedChainStateMachine getInstance(String chainID) {
        return replicatedChains.get(chainID);
    }

    public Set<NodeIDType> getReplicaGroup(String chainID) {
        ReplicatedChainStateMachine crsm = this.getInstance(chainID);
        return this.integerMap.getIntArrayAsNodeSet(crsm.getMembers());
    }
}
