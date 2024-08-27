package edu.umass.cs.pram.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class PramPacket extends JSONPacket implements ReplicableRequest {

    protected PramPacket(IntegerPacketType t) {
        super(t);
    }

    public static Request createFromString(String stringified, AppRequestParser appRequestParser) {
        JSONObject object;
        Integer packetType;
        try {
            object = new JSONObject(stringified);
            packetType = JSONPacket.getPacketType(object);
        } catch (JSONException e) {
            return null;
        }

        if (packetType == null) {
            Logger.getGlobal().log(Level.WARNING,
                    "Ignoring PramPacket as the type is null");
            return null;
        }

        PramPacketType pramPacketType = PramPacketType.intToType.get(packetType);
        switch (pramPacketType) {
            case PramPacketType.PRAM_READ_PACKET -> {
                return PramReadPacket.fromJsonObject(object, appRequestParser);
            }
            case PramPacketType.PRAM_WRITE_PACKET -> {
                throw new RuntimeException("unimplemented :(");
            }
            case PramPacketType.PRAM_WRITE_AFTER_PACKET -> {
                return PramWriteAfterPacket.fromJsonObject(object, appRequestParser);
            }
            default -> {
                Logger.getGlobal().log(Level.SEVERE,
                        "Handling unknown PramPacketType " + pramPacketType);
            }
        }

        return null;
    }


//    public static PramPacket createWriteAfterPacket(String serviceName,
//                                                    Request clientWriteRequest) {
//        return new PramPacket(PramPacketType.PRAM_WRITE_AFTER_PACKET, 0,
//                serviceName, clientWriteRequest);
//    }
//
//    private PramPacket(PramPacketType pramPacketType,
//                       long requestID,
//                       String serviceName,
//                       Request clientRequest) {
//        super(pramPacketType);
//        this.packetType = pramPacketType;
//        this.serviceName = serviceName;
//        this.requestID = requestID == 0 ? System.currentTimeMillis() : requestID;
//        this.clientRequest = clientRequest;
//    }
//
//    public PramPacket(ReplicableClientRequest request, boolean isWrite) {
//        super(isWrite ? PramPacketType.PRAM_WRITE_PACKET : PramPacketType.PRAM_READ_PACKET);
//        this.packetType = isWrite ? PramPacketType.PRAM_WRITE_PACKET : PramPacketType.PRAM_READ_PACKET;
//        this.serviceName = request.getServiceName();
//        this.requestID = request.getRequestID();
//        this.clientRequest = request.getRequest();
//        System.out.println(">>>> request type = " + this.getRequestType());
//    }
//
//    public Request getClientRequest() {
//        return clientRequest;
//    }
//
//    @Override
//    public IntegerPacketType getRequestType() {
//        return this.packetType;
//    }
//
//    @Override
//    public String getServiceName() {
//        return this.serviceName;
//    }
//
//    @Override
//    public long getRequestID() {
//        return this.requestID;
//    }
//
//    @Override
//    protected JSONObject toJSONObjectImpl() throws JSONException {
//        JSONObject object = new JSONObject();
//        object.put("serviceName", this.serviceName);
//        object.put("requestID", this.requestID);
//        if (this.clientRequest != null) {
//            object.put("request", this.clientRequest.toString());
//        }
//        return object;
//    }
//
//    public static PramPacket createFromString(String encoded, AppRequestParser appRequestParser) {
//        try {
//            JSONObject object = new JSONObject(encoded);
//            String serviceName = object.getString("serviceName");
//            long requestID = object.getLong("requestID");
//            PramPacketType packetType = PramPacketType.PRAM_PACKET;
//            int packetTypeInt = object.getInt("type");
//            if (packetTypeInt == PramPacketType.PRAM_WRITE_AFTER_PACKET.getInt()) {
//                packetType = PramPacketType.PRAM_WRITE_AFTER_PACKET;
//            }
//            Request appRequest = null;
//            if (object.has("request") && appRequestParser != null) {
//                String appRequestEncoded = object.getString("request");
//                appRequest = appRequestParser.getRequest(appRequestEncoded);
//            }
//
//            return new PramPacket(packetType, requestID, serviceName, appRequest);
//        } catch (JSONException | RequestParseException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    public boolean needsCoordination() {
//        return true;
//    }
}
