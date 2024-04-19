package edu.umass.cs.primarybackup;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;

public class PrimaryEpoch {

    public final String nodeID;
    public final int counter;

    public PrimaryEpoch(String primaryNodeID, int counter) {
        this.nodeID = primaryNodeID;
        this.counter = counter;
    }

    public PrimaryEpoch(String epochString) {
        String[] raw = epochString.split(":");
        assert raw.length == 2;

        this.nodeID = raw[0];
        this.counter = Integer.parseInt(raw[1]);
    }

    @Override
    public String toString() {
        return String.format("%s:%d", this.nodeID, this.counter);
    }

    public int compareTo(PrimaryEpoch that) {
        if (this.nodeID.equals(that.nodeID)) {
            return Integer.compare(this.counter, that.counter);
        }
        return this.nodeID.compareTo(that.nodeID);
    }
}
