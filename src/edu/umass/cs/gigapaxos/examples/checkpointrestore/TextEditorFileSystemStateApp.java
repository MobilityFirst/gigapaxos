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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;

public class TextEditorFileSystemStateApp implements Replicable {

    protected final String CURRENT_STATE_DIR = "current_state/";
    protected String currentStateFilename = "";

    public TextEditorFileSystemStateApp(String[] args) throws IOException {
        super();
        Path currStateDir = Paths.get(CURRENT_STATE_DIR);
        if (!currentStateFilename.isEmpty()) {
            return;
        }
        if (Files.notExists(currStateDir)) {
            Files.createDirectory(currStateDir);
        }
        this.currentStateFilename = CURRENT_STATE_DIR + args[0] + ".txt";
        // TODO: setup connection to the data store and keyspace
        //throw new RuntimeException("Not yet implemented");
    }

    @Override
    public boolean execute(Request request) {
        try {
            if (request instanceof RequestPacket) {
                String requestValue = ((RequestPacket) request).requestValue;
                Path statePath = Paths.get(currentStateFilename);
                if (!Files.exists(statePath)) {
                    Files.createFile(statePath);
                }
                try {
                    JSONObject jsonObject = new JSONObject(requestValue);
                    String typeOfReq = jsonObject.getString("type");
                    switch (typeOfReq) {
                        case "type":
                            Files.write(statePath, jsonObject.getString("value").getBytes(), StandardOpenOption.APPEND);
                            break;
                        case "backspace":
                            String currText = Files.readString(statePath);
                            Files.write(statePath, currText.substring(0, currText.length() - 1).getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
                            break;
                        case "newline":
                            Files.write(statePath, System.lineSeparator().getBytes(), StandardOpenOption.APPEND);
                            break;
                        case "cleartext":
                            Files.write(statePath, "".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                ((RequestPacket) request).setResponse("Current text = " + Files.readString(statePath));
            } else
                System.err.println("Unknown request type: " + request.getRequestType());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return this.execute(request);
    }

    @Override
    public String checkpoint(String name) {
        Path statePath = Paths.get(currentStateFilename);
        String checkpointStr = "";
        try {
            if (!Files.exists(statePath)) {
                return "{}";
            }
            checkpointStr = Files.readString(statePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new JSONObject(Map.of("fileName", currentStateFilename, "data", checkpointStr)).toString();
    }

    @Override
    public boolean restore(String name, String state) {
        if (state == null || state.equals(Config.getGlobalString(PaxosConfig.PC
                .DEFAULT_NAME_INITIAL_STATE)))
            return true;
        try {
            JSONObject jsonObject = new JSONObject(state);
            currentStateFilename = jsonObject.getString("fileName");
            Path statePath = Paths.get(currentStateFilename);
            Files.write(statePath, jsonObject.getString("data").getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e) {
            e.printStackTrace();
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
