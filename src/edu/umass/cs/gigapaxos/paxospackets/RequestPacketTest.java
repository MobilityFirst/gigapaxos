package edu.umass.cs.gigapaxos.paxospackets;

import edu.umass.cs.utils.DefaultTest;
import org.junit.Test;

/**
 * Created by kanantharamu on 2/20/17.
 */
public class RequestPacketTest extends DefaultTest {
    /**
     * 
     */
    public RequestPacketTest() {
    }

    /**
     * 
     */
    @Test
    public void testCheckFields() {
        RequestPacket.doubleCheckFields();
    }
}
