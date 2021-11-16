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
 * A write request is first sent to the head,
 * which then passes the update along the chain to the tail (primary).
 * All read requests are sent to the tail (primary).
 *
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

    private final int next;
    // private final int nextNext;
    private final int head;
    private final int tail;

    public ReplicatedChainStateMachine(String chainID, int version, int id,
                                       Set<Integer> chainMembers, Replicable app, String initialState,
                                       ChainManager<?> cm){
        Arrays.sort(this.chainMembers = Util.setToIntArray(chainMembers));
        this.chainID = chainID;
        this.version = version;
        this.chainManager = cm;

        boolean found = false;

        // there must be at least one node in the chain
        assert(this.chainMembers.length > 0);

        this.head = this.chainMembers[0];
        this.tail = this.chainMembers[this.chainMembers.length-1];

        int idx = 0;
        while (idx<this.chainMembers.length){
            if (this.chainMembers[idx] == id) {
                found = true;
                break;
            }
            ++idx;
        }

        if (found && idx < this.chainMembers.length-1)
            this.next = this.chainMembers[idx + 1];
        else {
            // unfound, no next, I'm the tail
            this.next = -1;
        }

        restore(initialState);
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

    protected String getChainIDVersion(){
        return this.chainID + ","+this.version;
    }

    protected Integer getChainHead() {
        return this.head;
    }

    protected Integer getChainTail() {
        return this.tail;
    }

    protected Integer getNext() {
        return this.next;
    }

    // for fault-tolerance when a node in the middle of the chain fails
//    protected int getNextNext() {
//        // TODO
//        return 0;
//    }

    @Override
    public String toString(){
        StringBuilder members = new StringBuilder("[");
        for (int chainMember : this.chainMembers) {
            members.append(chainMember).append(",");
        }
        members.append("]");

        return "("+this.chainID+","+this.version+","+members.toString()+",next:"
                +this.next+",chain="+this.head+"-->"+this.tail+")";
    }


    private boolean restore(String state){
        return this.chainManager.getApp().restore(getChainID(), state);
    }


}
