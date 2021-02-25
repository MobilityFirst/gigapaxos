package edu.umass.cs.chainreplication;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.utils.Util;

import java.util.Arrays;
import java.util.Set;

/**
 * This class keeps the state of a replicated chain used by
 * chain replication algorithm. The information kept by
 * this class is as follows:
 *  (1) members of the chain (from head to tail, tail is the primary)
 *  (2) id of the chain
 *  (3) current version of the chain
 *  (4) a pointer to the ChainManager of this node
 *
 * A typical process to coordinate a request is as this:
 * A coordinated request is first sent ot forwarded to the head,
 * which then passes the update along the chain to the tail (primary).
 * All read requests are sent to the tail (primary) of the chain as
 * in normal primary-backup systems.
 *
 * TODO: reconfiguration, fault-tolerance
 *
 * @author Z Gao
 */
public class ReplicatedChainStateMachine {
    private final int[] chainMembers; // from head to tail
    private final Object chainID;
    private final int version;
    private final ChainManager<?> chainManager;

    public ReplicatedChainStateMachine(String chainID, int version, int id,
                                       Set<Integer> chainMembers, Replicable app, String initialState,
                                       ChainManager<?> cm){
        Arrays.sort(this.chainMembers = Util.setToIntArray(chainMembers));
        this.chainID = chainID;
        this.version = version;
        this.chainManager = cm;

    }

    protected int getVersion() {
        return this.version;
    }

    protected int[] getMembers() {
        return this.chainMembers;
    }

    /*
    protected String getNodeID() {
        return this.chainManager != null ? this.chainManager.intToString(this
                .getMyID()) : "" + getMyID();
    }
    */

    protected String getChainID() {
        return (chainID instanceof String ? (String) chainID : new String(
                (byte[]) chainID));
    }

    protected int getChainHead() {
        //TODO
        return 0;
    }

    protected int getChainTail() {
        //TODO
        return 0;
    }

    protected int getNextNode() {
        //TODO
        return 0;
    }

    // for fault-tolerance when a node in the middle of the chain fails
    protected int getNextNext() {
        // TODO
        return 0;
    }


}
