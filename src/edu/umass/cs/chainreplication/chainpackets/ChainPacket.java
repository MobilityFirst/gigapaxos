package edu.umass.cs.chainreplication.chainpackets;

import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;


import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * ChainPacket is the universal packet used in all
 */
public abstract class ChainPacket extends JSONPacket {

    protected String chainID = null;
    protected int version = -1;
    protected ChainPacketType packetType;
    protected int slot;

    protected ChainPacket(ChainPacket pkt){
        super(ChainPacket.ChainPacketType.CHAIN_PACKET);

        if (pkt !=null){
            this.packetType = pkt.packetType;
            this.chainID = pkt.chainID;
            this.version = pkt.version;
            this.slot = pkt.slot;
        }
    }

    protected ChainPacket(ChainPacketType packetType, String chainID, int version, int slot){
        super(ChainPacket.ChainPacketType.CHAIN_PACKET);
        this.packetType = packetType;
        this.chainID = chainID;
        this.version = version;
        this.slot = slot;
    }

    protected ChainPacket(JSONObject json) throws JSONException {
        super(json);
        if (json != null) {
            if (json.has(ChainPacket.Keys.PT.toString()))
                this.packetType = ChainPacketType.getChainPacketType
                        (json.getInt(ChainPacket.Keys.PT.toString()));
            if (json.has(ChainPacket.Keys.ID.toString()))
                this.chainID = json.getString(ChainPacket.Keys.ID.toString());
            if (json.has(ChainPacket.Keys.V.toString()))
                this.version = json.getInt(ChainPacket.Keys.V.toString());
            if (json.has(ChainPacket.Keys.S.toString()))
                this.slot = json.getInt(ChainPacket.Keys.S.toString());
        }
    }

    protected final static String CHARSET = "ISO-8859-1";

    protected static final int SIZEOF_CHAINPACKET_FIXED = Integer.BYTES*4 + 1;

    protected ChainPacket(ByteBuffer bbuf) throws UnsupportedEncodingException {
        super(ChainPacketType.CHAIN_PACKET);

        bbuf.getInt(); // ignore chain packet type
        this.packetType = ChainPacketType.getChainPacketType(bbuf.getInt());
        this.version = bbuf.getInt();
        this.slot = bbuf.getInt();

        byte chainIDLength = bbuf.get();
        byte[] chainIDBytes = new byte[chainIDLength];
        bbuf.get(chainIDBytes);
        this.chainID = chainIDBytes.length > 0?
                new String(chainIDBytes, CHARSET) : null;
        int exactLength = SIZEOF_CHAINPACKET_FIXED + chainIDLength;
        assert (bbuf.position() == exactLength);
    }

    protected ByteBuffer toBytes(ByteBuffer bbuf)
            throws UnsupportedEncodingException {
        bbuf.putInt(ChainPacketType.CHAIN_PACKET.getInt());
        bbuf.putInt(this.packetType.getInt());
        bbuf.putInt(this.version);
        bbuf.putInt(this.slot);

        byte[] chainIDBytes = this.chainID != null ?
                this.chainID.getBytes(CHARSET) : new byte[0];
        bbuf.put((byte) chainIDBytes.length);
        bbuf.put(chainIDBytes);
        assert (bbuf.position() == SIZEOF_CHAINPACKET_FIXED + chainIDBytes.length);
        return bbuf;
    }

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
         * JSON key for chain version (or epoch number) field.
         */
        V,
        /**
         * slot
         */
        S,
    }



    /**
     * To avoid potential conflict with existing {@link PaxosPacket} PaxosPacketType,
     * ChainPacketType starts from 1000 to 1100
     */
    public enum ChainPacketType implements IntegerPacketType{

        /**
         *
         */
        REQUEST("CHAIN_REQ", 1001),
        /**
         *
         */
        FORWARD("CHAIN_FWD", 1002),
        /**
         *
         */
        ACK("CHAIN_ACK", 1003),
        /**
         *
         */
        RESPONSE("RESPONSE", 1004),
        /**
         *
         */
        READ("CHAIN_READ", 1005),
        /**
         *
         */
        CHAIN_PACKET("CHAIN_PACKET",1099),
        /**
         *
         */
        NO_TYPE("CHAIN_NO_TYPE", 1999);
        ;

        private static HashMap<String, ChainPacketType> labels = new HashMap<String, ChainPacketType>();
        private static HashMap<Integer, ChainPacketType> numbers = new HashMap<Integer, ChainPacketType>();

        private final String label;
        private final int number;

        ChainPacketType(String s, int t) {
            this.label = s;
            this.number = t;
        }

        /************** BEGIN static code block to ensure correct initialization ***************/
        static {
            for (ChainPacketType type: ChainPacketType.values()) {
                if (!ChainPacketType.labels.containsKey(type.label)
                && !ChainPacketType.numbers.containsKey(type.number)) {
                    ChainPacketType.labels.put(type.label, type);
                    ChainPacketType.numbers.put(type.number, type);
                } else {
                    assert(false): "Duplicate or inconsistent enum type for ChainPacketType";
                }
            }
        }

        /************** END static code block to ensure correct initialization ***************/

        @Override
        public int getInt() {
            return this.number;
        }

        /**
         * @return String label
         */
        public String getLabel() {
            return label;
        }

        public String toString() {
            return getLabel();
        }

        public static ChainPacketType getChainPacketType(int type){
            return ChainPacketType.numbers.get(type);
        }

        public static ChainPacketType getChainPacketType(String type){
            return ChainPacketType.labels.get(type);
        }
    }

    public String getChainID() {
        return this.chainID;
    }

    public int getVersion() {
        return this.version;
    }

    public ChainPacketType getType() {
        return this.packetType;
    }

    public Object getSummary() {
        return new Object() {
            public String toString() {
                return getChainID() + ":" + getVersion()+ ":" + getType();
                        // + ":[" + getSummaryString() + "]";
            }
        };
    }


    protected net.minidev.json.JSONObject toJSONSmart() throws JSONException {
        net.minidev.json.JSONObject json = new net.minidev.json.JSONObject();

        // the specific type of ChainPacket
        json.put(ChainPacket.Keys.PT.toString(), this.packetType.getInt());
        json.put(ChainPacket.Keys.ID.toString(), this.chainID);
        json.put(ChainPacket.Keys.V.toString(), this.version);
        json.put(ChainPacket.Keys.S.toString(), this.slot);

        // copy over child fields
        net.minidev.json.JSONObject child = new net.minidev.json.JSONObject();
        for (String name : (child).keySet())
            json.put(name, child.get(name));

        return json;
    }

    protected abstract JSONObject toJSONObjectImpl() throws JSONException;

}
