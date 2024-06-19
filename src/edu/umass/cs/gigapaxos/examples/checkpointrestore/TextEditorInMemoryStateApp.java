package edu.umass.cs.gigapaxos.examples.checkpointrestore;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

public class TextEditorInMemoryStateApp implements Replicable {

    protected String text;

    public TextEditorInMemoryStateApp() {
        super();
        this.text = "";
        // TODO: setup connection to the data store and keyspace
        //throw new RuntimeException("Not yet implemented");
    }

    @Override
    public boolean execute(Request request) {
        if (request instanceof RequestPacket) {
            String requestValue = ((RequestPacket) request).requestValue;
            try {
                JSONObject jsonObject = new JSONObject(requestValue);
                String typeOfReq = jsonObject.getString("type");
                switch (typeOfReq) {
                    case "type":
                        this.text += jsonObject.getString("value");
                        break;
                    case "backspace":
                        this.text = this.text.substring(0, this.text.length() - 1);
                        break;
                    case "newline":
                        this.text += System.lineSeparator();
                        break;
                    case "cleartext":
                        this.text = "";
                        break;
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
            ((RequestPacket) request).setResponse("Current text = " + this.text);
        } else System.err.println("Unknown request type: " + request.getRequestType());
        return true;
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return this.execute(request);
    }

    @Override
    public String checkpoint(String name) {
        return this.text;
    }

    @Override
    public boolean restore(String name, String state) {
        if (state == null || state.equals(Config.getGlobalString(PaxosConfig.PC
                .DEFAULT_NAME_INITIAL_STATE))) {
            this.text = "";
        } else {
            this.text = state;
        }
        return true;
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return null;
    }
}
