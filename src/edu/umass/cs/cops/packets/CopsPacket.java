package edu.umass.cs.cops.packets;

import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import org.json.JSONException;
import org.json.JSONObject;

public class CopsPacket extends JSONPacket implements ReplicableRequest {

    // general fields for all packet
    private final String serviceName;
    private final long requestID;

    // fields for client request packet
    private final boolean isClientRequest;
    private ReplicableClientRequest clientRequest;

    // TODO: fields for put_after packet

    public CopsPacket(ReplicableClientRequest clientRequest) {
        super(CopsPacketType.COPS_PACKET);
        this.clientRequest = clientRequest;
        this.isClientRequest = true;
        this.serviceName = clientRequest.getServiceName();
        this.requestID = clientRequest.getRequestID();
    }

    private CopsPacket(String serviceName) {
        super(CopsPacketType.COPS_PACKET);
        this.isClientRequest = false;
        this.serviceName = serviceName;
        this.requestID = System.currentTimeMillis();
    }

    private CopsPacket(String serviceName, long requestID) {
        super(CopsPacketType.COPS_PACKET);
        this.isClientRequest = false;
        this.serviceName = serviceName;
        this.requestID = requestID;
    }

    public static CopsPacket createPutAfterPacket(String serviceName) {
        return new CopsPacket(serviceName);
    }

    public boolean isClientRequest() {
        return isClientRequest;
    }

    public ReplicableClientRequest getClientRequest() {
        return clientRequest;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("serviceName", this.serviceName);
        object.put("requestID", this.requestID);
        object.put("clientRequest",
                this.clientRequest != null ?
                this.clientRequest.toString() : "");

        return object;
    }

    public static CopsPacket createFromString(String encoded) {
        try {
            JSONObject json = new JSONObject(encoded);
            String serviceName = json.getString("serviceName");
            long requestID = json.getLong("requestID");

            return new CopsPacket(serviceName, requestID);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IntegerPacketType getRequestType() {
        return CopsPacketType.COPS_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.requestID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }
}
