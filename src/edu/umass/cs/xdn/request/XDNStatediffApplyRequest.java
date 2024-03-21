package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Random;

public class XDNStatediffApplyRequest implements ReplicableRequest {

    /**
     * All the serialized XDNStatediffApplyRequest starts with "xdn:31302:"
     */
    public static final String SERIALIZED_PREFIX = String.format("%s%d:",
            XDNRequest.SERIALIZED_PREFIX, XDNRequestType.XDN_STATEDIFF_APPLY_REQUEST.getInt());

    private final String serviceName;
    private final String statediff;
    private long requestID;

    public XDNStatediffApplyRequest(String serviceName, String statediff) {
        this.serviceName = serviceName;
        this.statediff = statediff;
        this.requestID = System.currentTimeMillis();
    }

    @Override
    public IntegerPacketType getRequestType() {
        return XDNRequestType.XDN_STATEDIFF_APPLY_REQUEST;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.requestID;
    }

    public void setRequestID(long requestID) {
        this.requestID = requestID;
    }

    public String getStatediff() {
        return statediff;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("sn", this.serviceName);
            json.put("sd", this.statediff);
            json.put("id", this.requestID);
            return String.format("%s%s",
                    SERIALIZED_PREFIX,
                    json.toString());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.serviceName, this.statediff, this.requestID);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XDNStatediffApplyRequest that = (XDNStatediffApplyRequest) o;
        return this.serviceName.equals(that.serviceName) &&
                this.statediff.equals(that.statediff) &&
                this.requestID == that.requestID;
    }

    public static XDNStatediffApplyRequest createFromString(String encodedRequest) {
        if (encodedRequest == null || !encodedRequest.startsWith(SERIALIZED_PREFIX)) {
            return null;
        }
        encodedRequest = encodedRequest.substring(SERIALIZED_PREFIX.length());
        try {
            JSONObject json = new JSONObject(encodedRequest);

            // prepare the deserialized variables
            String serviceName = json.getString("sn");
            String statediffStr = json.getString("sd");
            long requestID = json.getLong("id");

            XDNStatediffApplyRequest request = new XDNStatediffApplyRequest(
                    serviceName,
                    statediffStr);
            request.setRequestID(requestID);
            return request;
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static class TestXDNStatediffApplyRequest {
        @Test
        public void TestXDNStatediffApplyRequestSerializationDeserialization() {
            byte[] statediff = new byte[10240];
            new Random().nextBytes(statediff);

            String serviceName = "dummyServiceName";
            String statediffString = new String(statediff, StandardCharsets.ISO_8859_1);
            XDNStatediffApplyRequest request = new XDNStatediffApplyRequest(
                    serviceName,
                    statediffString);

            String serialized = request.toString();
            XDNStatediffApplyRequest deserializedRequest = XDNStatediffApplyRequest.
                    createFromString(serialized);

            System.out.println(request);
            System.out.println(deserializedRequest);
            assert deserializedRequest != null : "deserialized XDNStatediffApplyRequest is null";

            assert request.equals(deserializedRequest);
            assert serialized.equals(deserializedRequest.toString());
        }
    }

}
