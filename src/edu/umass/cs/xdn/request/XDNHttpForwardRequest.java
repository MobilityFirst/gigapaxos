package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

public class XDNHttpForwardRequest implements ReplicableRequest {

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
