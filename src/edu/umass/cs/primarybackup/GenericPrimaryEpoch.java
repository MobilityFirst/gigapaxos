package edu.umass.cs.primarybackup;

import edu.umass.cs.nio.interfaces.Stringifiable;

public class GenericPrimaryEpoch<NodeIDType> {

    public final String nodeID;
    public final int counter;

    public GenericPrimaryEpoch(NodeIDType primaryNodeID, int counter) {
        this.nodeID = primaryNodeID.toString();
        this.counter = counter;
    }

    public GenericPrimaryEpoch(String epochString) {
        String[] raw = epochString.split(":");
        assert raw.length == 2 : "invalid format of epochString";

        this.nodeID = raw[0];
        this.counter = Integer.parseInt(raw[1]);
    }

    public int compareTo(GenericPrimaryEpoch<?> that) {
        if (this.counter == that.counter) {
            return this.nodeID.compareTo(that.nodeID);
        }
        if (this.counter < that.counter) {
            return -1;
        }
        return 1;
    }

    @Override
    public String toString() {
        return String.format("%s:%d", this.nodeID.toString(), this.counter);
    }


}
