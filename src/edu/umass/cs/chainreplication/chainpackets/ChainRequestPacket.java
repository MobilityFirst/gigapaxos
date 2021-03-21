package edu.umass.cs.chainreplication.chainpackets;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class ChainRequestPacket extends ChainPacket
        implements Request, ClientRequest, Byteable {

    public static enum Keys {
        /**
         * True if stop request.
         */
        STOP,
        /**
         * Most recent forwarder.
         */
        FWDR,
        /**
         * Request ID.
         */
        QID,
        /**
         * Request value.
         */
        QV,
        /**
         * Response value.
         */
        RV,
        /**
         * Client socket address: a client use this socket to send request to this node
         */
        CA,
        /**
         * Listen socket address: this node uses this socket to listen for client's request
         */
        LA,
    }

    public final long requestID;
    /**
     * Whether this request is a stop request.
     */
    public final boolean stop;
    /**
     * The actual request body. The client will get back this string if that is
     * what it sent to paxos. If it issued a RequestPacket, then it will get
     * back the whole RequestPacket back.
     */
    public final String requestValue;

    // response to be returned to client
    private String responseValue = null;

    // the replica that first received this request
    private int entryReplica = IntegerMap.NULL_INT_NODE;

    /**
     * The socket address that client used to send {@REPLICABLE_CLIENT_REQUEST}
     */
    private InetSocketAddress clientSocketAddress;
    /**
     * The socket address that the ChainManager used to receive REPLICABLE_CLIENT_REQUEST
     */
    private InetSocketAddress listenSocketAddress;

    public ChainRequestPacket(long reqID, String value, boolean stop,
                              ChainRequestPacket req){

        super(req);
        this.packetType = ChainPacketType.REQUEST;

        this.requestID = reqID;
        this.requestValue = value;
        this.stop = stop;

        if(req == null)
            return;

        this.entryReplica = req.entryReplica;
        this.responseValue = req.responseValue;

        this.clientSocketAddress = req.clientSocketAddress;
        this.listenSocketAddress = req.listenSocketAddress;
    }

    public ChainRequestPacket(long reqID, String value, boolean stop){
        this(reqID, value, stop, (ChainRequestPacket) null);
    }

    public ChainRequestPacket(long reqID, String value, boolean stop,
                              InetSocketAddress csa) {
        this(reqID, value, stop, (ChainRequestPacket) null);
        this.clientSocketAddress = csa;
    }


    public ChainRequestPacket(ByteBuffer bbuf) throws UnsupportedEncodingException, UnknownHostException {
        super(bbuf);
        // int exactLength = bbuf.position();

        this.requestID = bbuf.getLong();
        this.stop = bbuf.get() == (byte) 1;
        this.entryReplica = bbuf.getInt();

        byte[] clientAddressBytes = new byte[4];
        bbuf.get(clientAddressBytes);
        int cport = (int) bbuf.getShort();
        cport = cport >= 0 ? cport : cport + 2 * (Short.MAX_VALUE + 1);
        this.clientSocketAddress = cport != 0 ? new InetSocketAddress(
                InetAddress.getByAddress(clientAddressBytes), cport): null;

        byte[] listenAddressBytes = new byte[4];
        bbuf.get(listenAddressBytes);
        int lport = (int) bbuf.getShort();
        lport = lport >= 0 ? lport : lport + 2* (Short.MAX_VALUE + 1);
        this.listenSocketAddress = lport != 0 ? new InetSocketAddress(
                InetAddress.getByAddress(listenAddressBytes), lport) : null;

        // exactLength += (Long.BYTES+1+Integer.BYTES);
        int valLength = bbuf.getInt();
        byte[] valBytes = new byte[valLength];
        bbuf.get(valBytes);
        this.requestValue = new String(valBytes, CHARSET);

        int respLength = bbuf.getInt();
        if (respLength>0) {
            byte[] respBytes = new byte[respLength];
            bbuf.get(respBytes);
            this.responseValue = new String(respBytes, CHARSET);
        }
    }

    @Override
    public byte[] toBytes() {

        // System.out.println("######## ChainRequestPacket.toBytes is called");

        byte[] array = new byte[this.lengthEstimate()];
        ByteBuffer bbuf = ByteBuffer.wrap(array);
        assert (bbuf.position() == 0);

        try {
            super.toBytes(bbuf);

            int chainPacketPosition = bbuf.position(); // for assertion
            int exactLength = bbuf.position();

            bbuf.putLong(this.requestID);
            bbuf.put(this.stop? (byte) 1: (byte) 0);
            bbuf.putInt(this.entryReplica);

            exactLength += (Long.BYTES+1+ Integer.BYTES);

            // addresses
            /* Note: 0 is ambiguous with wildcard address, but that's okay
             * because an incoming packet will never come with a wildcard
             * address. */
            bbuf.put(this.clientSocketAddress != null ? this.clientSocketAddress
                    .getAddress().getAddress() : new byte[4]);
            // 0 (not -1) means invalid port
            bbuf.putShort(this.clientSocketAddress != null ? (short) this.clientSocketAddress
                    .getPort() : 0);
            /* Note: 0 is an ambiguous wildcard address that could also be a
             * legitimate value of the listening socket address. If the request
             * happens to have no listening address, we will end up assuming it
             * was received on the wildcard address. At worst, the matching for
             * the corresponding response back to the client can fail. */
            bbuf.put(this.listenSocketAddress != null ? this.listenSocketAddress
                    .getAddress().getAddress() : new byte[4]);
            // 0 (not -1) means invalid port
            bbuf.putShort(this.listenSocketAddress != null ? (short) this.listenSocketAddress
                    .getPort() : 0);
            exactLength += 2 * (Integer.BYTES + Short.BYTES);

            // request value
            byte[] reqValBytes = this.requestValue != null?
                    this.requestValue.getBytes(CHARSET) : new byte[0];
            bbuf.putInt(reqValBytes.length);
            bbuf.put(reqValBytes);

            exactLength += (Integer.BYTES + reqValBytes.length);

            //response value
            byte[] respValBytes = this.responseValue != null?
                    this.responseValue.getBytes(CHARSET): new byte[0];
            bbuf.putInt(respValBytes.length);
            bbuf.put(respValBytes);
            exactLength += (Integer.BYTES + respValBytes.length);

            // bbuf.array() was a generous allocation
            byte[] exactBytes = new byte[exactLength];
            bbuf.flip();
            assert (bbuf.remaining() == exactLength) : bbuf.remaining()
                    + " != " + exactLength;
            bbuf.get(exactBytes);

            return exactBytes;

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return null;
    }

    private final static int SIZE_ESTIMATE = estimateSize();
    private static ChainRequestPacket samplePacket = null;

    private final static int EXPECTED_LENGTH_WITH_FIXED_FIELD = Long.BYTES // request ID: long
            + 1 // stop: boolean
            + Integer.BYTES // entryReplica: int
            ;

//    private static ChainRequestPacket getSamplePacket(String chainID, int version,
//                                                      int slot, boolean stop) {
//
//    }

    private static int estimateSize() {
        int length = 0;
        try {
            if (samplePacket == null){
                samplePacket = new ChainRequestPacket(0, "", false);
                samplePacket.putChainIDAndVersion("", 0);
                samplePacket.setClientSocketAddress(new InetSocketAddress("0.0.0.0", 0));
                samplePacket.setListenSocketAddress(new InetSocketAddress("0.0.0.0", 0));
            }
            length = samplePacket.toJSONObjectImpl().toString().length();

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return (int) (length * 1.5);
    }

    private int lengthEstimate() {
        return this.requestValue.length() + SIZE_ESTIMATE;
    }


    /************************** Getters ****************************/
    @Override
    public ClientRequest getResponse() {
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! getResponse is called!!!!!!!!!!!!");

        ChainRequestPacket reply = new ChainRequestPacket(this.requestID,
                    RequestPacket.ResponseCodes.ACK.toString(), this.stop, this);
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Response value is "+responseValue+"!!!!!!!!!!!!");
            reply.responseValue = this.responseValue;


        return reply;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return this.getType();
    }

    @Override
    public String getServiceName() {
        return this.chainID;
    }

    @Override
    public Object getSummary() {
        return super.getSummary();
    }

    @Override
    public long getRequestID() {
        return this.requestID;
    }

    @Override
    public InetSocketAddress getClientAddress(){
        return this.clientSocketAddress;
    }

    public boolean getStop() {
        return this.stop;
    }

    public int getEntryReplica() {
        return this.entryReplica;
    }

    public String[] getRequestValues() {
        String[] reqValues = new String[1];
        reqValues[0] = toString();
        return reqValues;
    }

    /************************** Setters ****************************/
    public void setPacketType(ChainPacketType packetType){
        this.packetType = packetType;
    }

    public void putChainIDAndVersion(String chainID, int version) {
        this.chainID = chainID;
        this.version = version;
    }

    public void setClientSocketAddress(InetSocketAddress socketAddress){
        this.clientSocketAddress = socketAddress;
    }

    public void setListenSocketAddress(InetSocketAddress socketAddress){
        this.listenSocketAddress = socketAddress;
    }

    public void setEntryReplica(int entryReplica) {
        this.entryReplica = entryReplica;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject json = new JSONObject();
        json.put(ChainPacket.Keys.ID.toString(), this.chainID);
        json.put(ChainPacket.Keys.PT.toString(), this.packetType.getInt());
        json.put(ChainPacket.Keys.V.toString(), this.requestValue);
        json.put(ChainPacket.Keys.S.toString(), this.slot);

        json.put(Keys.RV.toString(), this.requestValue);
        json.put(Keys.QID.toString(), this.requestID);
        json.put(Keys.STOP.toString(), this.stop);
        json.putOpt(Keys.RV.toString(), this.responseValue);
        json.put(Keys.FWDR.toString(), this.entryReplica);
        if(this.clientSocketAddress != null)
            json.put(Keys.CA.toString(), this.clientSocketAddress);
        if(this.listenSocketAddress != null)
            json.put(Keys.LA.toString(), this.listenSocketAddress);

        return json;
    }

    @Override
    public String toString(){
        try {
            return this.toJSONSmart().toString();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected net.minidev.json.JSONObject toJSONSmart() throws JSONException {
        net.minidev.json.JSONObject json = super.toJSONSmart();
        json.put(Keys.QID.toString(), this.requestID);
        json.put(Keys.RV.toString(), this.requestValue);
        json.put(Keys.FWDR.toString(), this.entryReplica);
        json.put(Keys.STOP.toString(), this.stop);
        json.put(Keys.CA.toString(), this.clientSocketAddress.toString());

        return json;
    }
}
