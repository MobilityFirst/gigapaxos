package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import io.netty.handler.codec.http.HttpResponse;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Objects;

public class XDNHttpForwardRequest extends XDNRequest {

    /**
     * All the serialized XDNHttpRequest starts with "xdn:31301:"
     */
    public static final String SERIALIZED_PREFIX = String.format("%s%d:",
            XDNRequest.SERIALIZED_PREFIX, XDNRequestType.XDN_HTTP_FORWARD_REQUEST.getInt());

    private final XDNHttpRequest request;
    private final String entryNodeID;

    public XDNHttpForwardRequest(XDNHttpRequest request, String entryNodeID) {
        this.request = request;
        this.entryNodeID = entryNodeID;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return XDNRequestType.XDN_HTTP_FORWARD_REQUEST;
    }

    @Override
    public String getServiceName() {
        return request.getServiceName();
    }

    @Override
    public long getRequestID() {
        return 0;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public XDNHttpRequest getRequest() {
        return request;
    }

    public String getEntryNodeID() {
        return entryNodeID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XDNHttpForwardRequest that = (XDNHttpForwardRequest) o;
        return Objects.equals(request, that.request);
    }

    @Override
    public int hashCode() {
        return Objects.hash(request);
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("r", this.request.toString());
            json.put("e", this.entryNodeID);
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static XDNHttpForwardRequest createFromString(String stringified) {
        if (stringified == null || !stringified.startsWith(SERIALIZED_PREFIX)) {
            return null;
        }
        stringified = stringified.substring(SERIALIZED_PREFIX.length());
        try {
            JSONObject json = new JSONObject(stringified);
            XDNHttpRequest httpRequest = XDNHttpRequest.createFromString(json.getString("r"));
            String entryNodeID = json.getString("e");
            return new XDNHttpForwardRequest(httpRequest, entryNodeID);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

}
