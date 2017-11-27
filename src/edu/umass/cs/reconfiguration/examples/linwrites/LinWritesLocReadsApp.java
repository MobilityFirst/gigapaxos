package edu.umass.cs.reconfiguration.examples.linwrites;

import edu.umass.cs.gigapaxos.examples.adder.StatefulAdderApp;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.noop.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This class extends the example {@link StatefulAdderApp} so that
 * only writes are totally ordered by the replicated state machine
 * but reads are served by each replica locally.
 *
 * @author arun
 *
 */
public class LinWritesLocReadsApp extends StatefulAdderApp {

    @Override
    public boolean execute(Request request) {

        // coordinated
        if (request instanceof SimpleAppRequest && ((SimpleAppRequest)
                request).getRequestType().equals(SimpleAppRequest.PacketType.COORDINATED_WRITE)) {
            this.total += Integer.valueOf(((SimpleAppRequest) request)
                    .getValue());
            ((SimpleAppRequest) request).setResponse("total=" + this.total);
        }
        // uncoordinated
        else if (request instanceof SimpleAppRequest && ((SimpleAppRequest)
                request).getRequestType().equals(SimpleAppRequest.PacketType.LOCAL_READ)) {
            ((SimpleAppRequest) request).setResponse("total="+this.total);
        }

        return true;
    }

    /**
     * Needed only if app uses request types other than RequestPacket. Refer
     * {@link NoopApp} for a more detailed example.
     */
    @Override
    public Request getRequest(String stringified)
            throws RequestParseException {
        try {
            return new SimpleAppRequest(new JSONObject(stringified));
        } catch(JSONException je) {
            throw new RequestParseException(je);
        }
    }

    /**
     * Needed only if app uses request types other than RequestPacket. Refer
     * {@link NoopApp} for a more detailed example.
     */
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(SimpleAppRequest
                .PacketType.values()));
    }
}
