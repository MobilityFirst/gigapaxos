package edu.umass.cs.pram.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import edu.umass.cs.xdn.interfaces.behavior.ReadOnlyRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PramReadPacket extends PramPacket  {

    private final long requestID;
    private final ClientRequest clientReadOnlyRequest;

    public PramReadPacket(ClientRequest readOnlyRequest) {
        this(System.currentTimeMillis(), readOnlyRequest);
    }

    private PramReadPacket(long requestID, ClientRequest readOnlyRequest) {
        super(PramPacketType.PRAM_READ_PACKET);
        assert readOnlyRequest != null : "The provided request cannot be null";
        assert (readOnlyRequest instanceof BehavioralRequest r && r.isReadOnlyRequest()) :
                "The provided request must be a ReadOnlyRequest";
        this.clientReadOnlyRequest = readOnlyRequest;
        this.requestID = requestID;
    }

    public ClientRequest getClientReadRequest() {
        return clientReadOnlyRequest;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PramPacketType.PRAM_READ_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.clientReadOnlyRequest.getServiceName();
    }

    @Override
    public long getRequestID() {
        return this.requestID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("id", this.requestID);
        object.put("req", this.clientReadOnlyRequest.toString());
        return object;
    }

    static PramReadPacket fromJsonObject(JSONObject jsonObject, AppRequestParser appRequestParser) {
        assert jsonObject != null : "the provided jsonObject can not be null";
        assert appRequestParser != null : "the provided appRequestParser can not be null";
        assert jsonObject.has("id") : "unknown id from the encoded packet";
        assert jsonObject.has("req") : "unknown user request from the encoded packet";
        try {
            long requestID = jsonObject.getLong("id");
            String encodedClientRequest = jsonObject.getString("req");
            Request clientRequest = appRequestParser.getRequest(encodedClientRequest);
            assert (clientRequest instanceof ClientRequest) :
                    "The request inside PramPacket must implement ClientRequest interface";
            assert (clientRequest instanceof BehavioralRequest r && r.isReadOnlyRequest()) :
                    "The client request inside PramReadPacket must be ReadOnlyRequest";
            return new PramReadPacket(requestID, (ClientRequest) clientRequest);
        } catch (JSONException | RequestParseException e) {
            Logger.getGlobal().log(Level.SEVERE, "receiving an invalid encoded pram packet");
            return null;
        }
    }
}
