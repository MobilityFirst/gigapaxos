package edu.umass.cs.chainreplication.chainpackets;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.net.InetSocketAddress;

public class ChainRequestPacket
        implements Request, ClientRequest, Byteable {


    @Override
    public ClientRequest getResponse() {
        return null;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return null;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public Object getSummary() {
        return null;
    }

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }

    @Override
    public long getRequestID() {
        return 0;
    }
}
