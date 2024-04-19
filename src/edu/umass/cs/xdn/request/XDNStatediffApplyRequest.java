package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.PrimaryEpoch;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Objects;

public class XDNStatediffApplyRequest extends XDNRequest {

    /**
     * All the serialized XDNStatediffApplyRequest starts with "xdn:31303:"
     */
    public static final String SERIALIZED_PREFIX = String.format("%s%d:",
            XDNRequest.SERIALIZED_PREFIX, XDNRequestType.XDN_STATEDIFF_APPLY_REQUEST.getInt());

    private final String serviceName;
    private final String statediff;
    private final PrimaryEpoch epoch;
    private long requestID;

    public XDNStatediffApplyRequest(String serviceName, PrimaryEpoch epoch, String statediff) {
        assert serviceName != null;
        assert epoch != null;
        assert statediff != null;

        this.serviceName = serviceName;
        this.epoch = epoch;
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

    public PrimaryEpoch getEpoch() {
        return epoch;
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
            json.put("ep", this.epoch.toString());
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
        return Objects.hash(this.serviceName, this.epoch, this.statediff, this.requestID);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XDNStatediffApplyRequest that = (XDNStatediffApplyRequest) o;
        return this.serviceName.equals(that.serviceName) &&
                this.epoch.equals(that.epoch) &&
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
            String epochStr = json.getString("ep");
            String statediffStr = json.getString("sd");
            long requestID = json.getLong("id");

            XDNStatediffApplyRequest request = new XDNStatediffApplyRequest(
                    serviceName,
                    new PrimaryEpoch(epochStr),
                    statediffStr);
            request.setRequestID(requestID);
            return request;
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
    }

}
