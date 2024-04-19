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
public class ResponsePacket extends PrimaryBackupPacket {

    public static final String SERIALIZED_PREFIX = "pb:res:";
    private final String serviceName;
    private final byte[] response;
    private final long requestID; // ID of request whose response contained in this packet

    public ResponsePacket(String serviceName, long requestID, byte[] response) {
        this.serviceName = serviceName;
        this.response = response;
        this.requestID = requestID;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_RESPONSE_PACKET;
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

    public byte[] getEncodedResponse() {
        return response;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResponsePacket that = (ResponsePacket) o;
        return requestID == that.requestID &&
                Objects.equals(serviceName, that.serviceName) &&
                Arrays.equals(response, that.response);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serviceName, requestID);
        result = 31 * result + Arrays.hashCode(response);
        return result;
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("serviceName", this.serviceName);
            json.put("response", new String(
                    this.response,
                    StandardCharsets.ISO_8859_1)
            );
            json.put("id", this.requestID);
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static ResponsePacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("serviceName");
            String responseStr = json.getString("response");
            long packetID = json.getLong("id");

            return new ResponsePacket(
                    serviceName,
                    packetID,
                    responseStr.getBytes(StandardCharsets.ISO_8859_1)
            );
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TestResponsePacket {
        @Test
        public void TestResponsePacketSerializationDeserialization() {
            String serviceName = "dummy-service-name";
            byte[] request = "raw-response".getBytes(StandardCharsets.ISO_8859_1);
            long requestID = 1000;
            ResponsePacket p1 = new ResponsePacket(serviceName, requestID, request);
            ResponsePacket p2 = ResponsePacket.createFromString(p1.toString());

            assert p2 != null;
            assert Objects.equals(p1, p2);
        }
    }

}
