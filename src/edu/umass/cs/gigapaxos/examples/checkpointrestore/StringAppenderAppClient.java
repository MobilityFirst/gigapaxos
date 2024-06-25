package edu.umass.cs.gigapaxos.examples.checkpointrestore;

import edu.umass.cs.gigapaxos.PaxosClientAsync;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StringAppenderAppClient extends PaxosClientAsync {

    public StringAppenderAppClient() throws IOException {
        super();
    }

    private static List<String> typeSampleText() {
        return PaxosConfig.getAsProperties().getProperty("TEXT_TO_BE_ADDED").chars().mapToObj(c -> (char) c)
                .map(c -> new JSONObject(Map.of("type", "type", "value", String.valueOf(c))).toString())
                .toList();
    }

    private static String readCommand() {
        return new JSONObject(Map.of("type", "type", "value", "")).toString();
    }

    private static void sendRequestWithCommand(StringAppenderAppClient client, String command, String lineNo, boolean printResponse)
            throws JSONException, IOException, InterruptedException {
        client.sendRequest(PaxosConfig.getDefaultServiceName(), command, new RequestCallback() {
            final long createTime = System.currentTimeMillis();

            @Override
            public void handleResponse(Request response) {
                if (printResponse) {
                    System.out
                            .println(lineNo + ": Response for request ["
                                    + command
                                    + "] = "
                                    + ((RequestPacket) response).getResponseValue()
                                    + " received in "
                                    + (System.currentTimeMillis() - createTime)
                                    + "ms");
                } else {
                    System.out.println("Response received for request #" + lineNo);
                }
            }
        });
        Thread.sleep(100);
    }

    public static void main(String[] args) throws IOException, JSONException, InterruptedException {
        StringAppenderAppClient client = new StringAppenderAppClient();
        int i = 1;
        for (String command : typeSampleText()) {
            sendRequestWithCommand(client, command, i + "", false);
            i = i + 1;
            Thread.sleep(100);
        }
        sendRequestWithCommand(client, readCommand(), i + "", true);
        Thread.sleep(100);
        client.close();
    }
}
