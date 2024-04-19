package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
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
public class RequestPacket extends PrimaryBackupPacket implements ClientRequest {

    public static final String SERIALIZED_PREFIX = "pb:req:";
    private final String serviceName;
    private final byte[] encodedServiceRequest;
    private final long packetID;

    private ClientRequest response;

    public RequestPacket(String serviceName, byte[] encodedServiceRequest) {
        this.serviceName = serviceName;
        this.encodedServiceRequest = encodedServiceRequest;
        this.packetID = System.currentTimeMillis();
    }

    private RequestPacket(String serviceName, byte[] encodedServiceRequest, long packetID) {
        this.serviceName = serviceName;
        this.encodedServiceRequest = encodedServiceRequest;
        this.packetID = packetID;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_REQUEST_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.packetID;
    }

    public byte[] getEncodedServiceRequest() {
        return encodedServiceRequest;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public void setResponse(ClientRequest response) {
        this.response = response;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestPacket that = (RequestPacket) o;
        return packetID == that.packetID &&
                Objects.equals(serviceName, that.serviceName) &&
                Arrays.equals(encodedServiceRequest, that.encodedServiceRequest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serviceName, packetID);
        result = 31 * result + Arrays.hashCode(encodedServiceRequest);
        return result;
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("serviceName", this.serviceName);
            json.put("request", new String(
                    this.encodedServiceRequest,
                    StandardCharsets.ISO_8859_1)
            );
            json.put("id", this.packetID);
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static RequestPacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("serviceName");
            String requestStr = json.getString("request");
            long packetID = json.getLong("id");

            return new RequestPacket(
                    serviceName,
                    requestStr.getBytes(StandardCharsets.ISO_8859_1),
                    packetID
            );
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClientRequest getResponse() {
        return this.response;
    }

    public static class TestRequestPacket {
        @Test
        public void TestRequestPacketSerializationDeserialization() {
            String serviceName = "dummy-service-name";
            byte[] request = "raw-request".getBytes(StandardCharsets.ISO_8859_1);
            RequestPacket p1 = new RequestPacket(serviceName, request);
            RequestPacket p2 = RequestPacket.createFromString(p1.toString());

            assert p2 != null;
            assert Objects.equals(p1, p2);
        }
    }

}
