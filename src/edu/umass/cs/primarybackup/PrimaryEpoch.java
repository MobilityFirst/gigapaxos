package edu.umass.cs.primarybackup;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Objects;

@RunWith(Enclosed.class)
public class PrimaryEpoch<NodeIDType> {

    public final String nodeID;
    public final int counter;

    public PrimaryEpoch(NodeIDType primaryNodeID, int counter) {
        this.nodeID = primaryNodeID.toString();
        this.counter = counter;
    }

    public PrimaryEpoch(String primaryNodeIDStr, int counter) {
        this.nodeID = primaryNodeIDStr;
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

    public int compareTo(PrimaryEpoch<NodeIDType> that) {
        if (this.counter == that.counter) {
            return this.nodeID.compareTo(that.nodeID);
        }
        if (this.counter < that.counter) {
            return -1;
        }
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrimaryEpoch<?> that = (PrimaryEpoch<?>) o;
        return counter == that.counter && Objects.equals(nodeID, that.nodeID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeID, counter);
    }

    public static class TestPrimaryEpoch {

        @Test
        public void TestEquals() {
            PrimaryEpoch<String> e1 = new PrimaryEpoch<>("node1:0");
            PrimaryEpoch<String> e2 = new PrimaryEpoch<>("node1:0");
            assert e1.equals(e2);
            assert e1.compareTo(e2) == 0;
        }

        @Test
        public void TestCompare() {
            PrimaryEpoch<String> e1 = new PrimaryEpoch<>("node1:0");
            PrimaryEpoch<String> e2 = new PrimaryEpoch<>("node1:1");
            assert e1.compareTo(e2) < 0;
        }

        @Test
        public void TestCompare2() {
            PrimaryEpoch<String> e1 = new PrimaryEpoch<>("node2:0");
            PrimaryEpoch<String> e2 = new PrimaryEpoch<>("node1:1");
            assert e1.compareTo(e2) < 0;
        }

    }

}
