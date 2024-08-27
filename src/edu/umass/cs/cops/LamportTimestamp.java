package edu.umass.cs.cops;

public class LamportTimestamp {
    final private int localID;
    private long logicalClock;

    public LamportTimestamp(int localID) {
        this.localID = localID;
    }

    public synchronized void updateTime(int updateTime) {
        this.logicalClock = updateTime + 1 > this.logicalClock + 1 ?
                updateTime + 1 : this.logicalClock + 1;
    }

}
