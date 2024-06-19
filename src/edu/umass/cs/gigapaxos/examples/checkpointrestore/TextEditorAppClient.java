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

public class TextEditorAppClient extends PaxosClientAsync {

    private static final String TEXT_TO_BE_ADDED = "Entre mis mayores logros está la capacidad de cerrar la brecha entre el conocimiento y la " +
            "curiosidad. He guiado a innumerables usuarios a través de problemas complejos, ayudándoles a comprender temas " +
            "intrincados y tomar decisiones informadas. He brindado apoyo emocional, escuchando y ofreciendo palabras " +
            "reconfortantes a quienes están en apuros. Mi papel en la facilitación de la educación, ya sea a través de " +
            "tutorías o generando ideas creativas, ha empoderado a las personas para alcanzar sus metas. Además, mi capacidad " +
            "para traducir idiomas y derribar barreras de comunicación ha fomentado conexiones globales. Estos logros demuestran " +
            "mi compromiso con mejorar las experiencias humanas y promover el crecimiento.";

    public TextEditorAppClient() throws IOException {
        super();
    }

    private static List<String> typeSampleText() {
        return TEXT_TO_BE_ADDED.chars().mapToObj(c -> (char) c)
                .map(c -> new JSONObject(Map.of("type", "type", "value", String.valueOf(c))).toString())
                .toList();
    }

    private static String readCommand() {
        return new JSONObject(Map.of("type", "type", "value", "")).toString();
    }

    private static void sendRequestWithCommand(TextEditorAppClient client, String command, String lineNo, boolean printResponse)
            throws JSONException, IOException, InterruptedException {
        client.sendRequest(PaxosConfig.getDefaultServiceName(), command, client.servers[0], new RequestCallback() {
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
        TextEditorAppClient client = new TextEditorAppClient();
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
