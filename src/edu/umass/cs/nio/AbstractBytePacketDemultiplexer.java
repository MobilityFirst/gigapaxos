package edu.umass.cs.nio;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;

public abstract class AbstractBytePacketDemultiplexer extends
        AbstractPacketDemultiplexer<byte[]> {

    protected AbstractBytePacketDemultiplexer() {
        this.register(new IntegerPacketType() {
            @Override
            public int getInt() {
                return 0;
            }
        });
    }

    @Override
    protected Integer getPacketType(byte[] message) {
        return 0;
    }

    @Override
    protected byte[] processHeader(byte[] message, NIOHeader header) {
        return message;
    }

    @Override
    protected boolean matchesType(Object message) {
        return message instanceof byte[];
    }
}
