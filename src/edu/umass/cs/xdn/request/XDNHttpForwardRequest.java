package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

public class XDNHttpForwardRequest extends XDNRequest {

    /**
     * All the serialized XDNHttpRequest starts with "xdn:31301:"
     */
    public static final String SERIALIZED_PREFIX = String.format("%s%d:",
            XDNRequest.SERIALIZED_PREFIX, XDNRequestType.XDN_HTTP_FORWARD_REQUEST.getInt());

    private final XDNHttpRequest request;

    public XDNHttpForwardRequest(XDNHttpRequest request) {
        this.request = request;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return XDNRequestType.XDN_HTTP_FORWARD_REQUEST;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public long getRequestID() {
        return 0;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }
}
