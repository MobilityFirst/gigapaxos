package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

@RunWith(Enclosed.class)
public class ForwardedRequestPacket extends PrimaryBackupPacket {

    public static final String SERIALIZED_PREFIX = "pb:fwd:";
    private final String serviceName;
    private final String entryNodeID;
    private final byte[] forwardedRequest;
    private final long packetID;

    public ForwardedRequestPacket(String serviceName, String entryNodeID, byte[] forwardedRequest) {
        this.serviceName = serviceName;
        this.entryNodeID = entryNodeID;
        this.forwardedRequest = forwardedRequest;
        this.packetID = System.currentTimeMillis();
    }

    private ForwardedRequestPacket(String serviceName, String entryNodeID, byte[] forwardedRequest,
                                   long packetID) {
        this.serviceName = serviceName;
        this.entryNodeID = entryNodeID;
        this.forwardedRequest = forwardedRequest;
        this.packetID = packetID;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_FORWARDED_REQUEST_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.packetID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public byte[] getForwardedRequest() {
        return forwardedRequest;
    }

    public String getEntryNodeID() {
        return entryNodeID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForwardedRequestPacket that = (ForwardedRequestPacket) o;
        return packetID == that.packetID &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(entryNodeID, that.entryNodeID) &&
                Arrays.equals(forwardedRequest, that.forwardedRequest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serviceName, packetID);
        result = 31 * result + Arrays.hashCode(forwardedRequest);
        return result;
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("serviceName", this.serviceName);
            json.put("entryID", this.entryNodeID);
            json.put("forwardedRequest", new String(
                    this.forwardedRequest,
                    StandardCharsets.ISO_8859_1)
            );
            json.put("id", this.packetID);
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static ForwardedRequestPacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("serviceName");
            String entryNodeID = json.getString("entryID");
            String forwardedReqStr = json.getString("forwardedRequest");
            long packetID = json.getLong("id");

            return new ForwardedRequestPacket(
                    serviceName,
                    entryNodeID,
                    forwardedReqStr.getBytes(StandardCharsets.ISO_8859_1),
                    packetID
            );
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TestForwardedRequestPacket {
        @Test
        public void TestForwardedRequestPacketSerializationDeserialization() {
            String serviceName = "dummy-service-name";
            String entryNodeID = "ar0";
            byte[] forwardedReq = "raw-request".getBytes(StandardCharsets.ISO_8859_1);
            ForwardedRequestPacket p1 = new ForwardedRequestPacket(
                    serviceName, entryNodeID, forwardedReq);
            ForwardedRequestPacket p2 = ForwardedRequestPacket.createFromString(p1.toString());

            assert p2 != null;
            assert Objects.equals(p1, p2);
        }
    }

}
