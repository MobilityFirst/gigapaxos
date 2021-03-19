package edu.umass.cs.chainreplication.chainpackets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;

public class ChainReplicationPacket {

    public static enum Keys {
        /**
         * JSON key for packet type field in JSON representation.
         */
        PT,
        /**
         * JSON key for chainID field.
         */
        ID,
        /**
         * JSON key for paxos version (or epoch number) field.
         */
        V,
        /**
         * slot
         */
        S,
    }

    protected String chainID = null;
    protected int version = -1;

    public enum ChainPacketType implements IntegerPacketType{
        
    }
}
