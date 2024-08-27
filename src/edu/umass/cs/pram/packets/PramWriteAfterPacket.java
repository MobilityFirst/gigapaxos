package edu.umass.cs.pram.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import edu.umass.cs.xdn.interfaces.behavior.ReadOnlyRequest;
import edu.umass.cs.xdn.interfaces.behavior.WriteOnlyRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PramWriteAfterPacket extends PramPacket {

    private final ClientRequest clientWriteOnlyRequest;
    private final long requestID;
    private final String senderID;

    public PramWriteAfterPacket(String senderID, ClientRequest clientWriteOnlyRequest) {
        this(System.currentTimeMillis(), senderID, clientWriteOnlyRequest);
    }

    private PramWriteAfterPacket(long requestID, String senderID, ClientRequest clientWriteOnlyRequest) {
        super(PramPacketType.PRAM_WRITE_AFTER_PACKET);
        assert clientWriteOnlyRequest != null : "The provided request cannot be null";
        assert (clientWriteOnlyRequest instanceof BehavioralRequest br && br.isWriteOnlyRequest()) :
                "The provided request must be a WriteOnlyRequest";
        this.clientWriteOnlyRequest = clientWriteOnlyRequest;
        this.senderID = senderID;
        this.requestID = requestID;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PramPacketType.PRAM_WRITE_AFTER_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.clientWriteOnlyRequest.getServiceName();
    }

    @Override
    public long getRequestID() {
        return requestID;
    }

    public String getSenderID() {
        return senderID;
    }

    public ClientRequest getClientWriteRequest() {
        return clientWriteOnlyRequest;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("id", this.requestID);
        object.put("req", this.clientWriteOnlyRequest.toString());
        object.put("sid", this.senderID);
        return object;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public static PramWriteAfterPacket fromJsonObject(JSONObject jsonObject, AppRequestParser appRequestParser) {
        assert jsonObject != null : "The provided json object can not be null";
        assert appRequestParser != null : "The provided appRequestParser can not be null";
        assert jsonObject.has("id") : "Unknown ID from the encoded packet";
        assert jsonObject.has("req") : "Unknown user request from the encoded packet";
        assert jsonObject.has("sid") : "Unknown sender ID from the encoded packet";
        try {
            long requestID = jsonObject.getLong("id");
            String senderID = jsonObject.getString("sid");
            String encodedClientRequest = jsonObject.getString("req");
            Request clientRequest = appRequestParser.getRequest(encodedClientRequest);
            assert (clientRequest instanceof ClientRequest) :
                    "The request inside PramPacket must implement ClientRequest interface";
            assert (clientRequest instanceof BehavioralRequest br && br.isWriteOnlyRequest()) :
                    "The client request inside PramReadPacket must be WriteOnlyRequest";
            return new PramWriteAfterPacket(requestID, senderID, (ClientRequest) clientRequest);
        } catch (JSONException | RequestParseException e) {
            Logger.getGlobal().log(Level.SEVERE, "receiving an invalid encoded pram packet");
            return null;
        }
    }
}
