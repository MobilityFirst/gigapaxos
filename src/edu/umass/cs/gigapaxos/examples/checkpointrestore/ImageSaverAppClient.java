package edu.umass.cs.gigapaxos.examples.checkpointrestore;

import edu.umass.cs.gigapaxos.PaxosClientAsync;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import org.json.JSONException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.random.RandomGenerator;

import static edu.umass.cs.gigapaxos.PaxosConfig.PC.CHECKPOINT_INTERVAL;

public class ImageSaverAppClient extends PaxosClientAsync {

    private static final String IMAGE_POOL_DIR = PaxosConfig.getAsProperties().getProperty("imagePoolDir");

    public ImageSaverAppClient() throws IOException {
        super();
    }

    public static void main(String[] args) throws IOException, JSONException, InterruptedException {
        ImageSaverAppClient client = new ImageSaverAppClient();
        Path imagePoolDir = Paths.get(IMAGE_POOL_DIR);
        List<Path> imageFiles = Files.list(imagePoolDir).toList();

        for (int i = 0; i < ((int)CHECKPOINT_INTERVAL.getDefaultValue()); i++) {
            byte[] image = Files.readAllBytes(imageFiles.get(RandomGenerator.getDefault().nextInt(imageFiles.size())));
            System.out.print("Request #" + i);
            client.sendRequest(PaxosConfig.getDefaultServiceName(), Base64.getEncoder().encodeToString(image), new RequestCallback() {
                final long createTime = System.currentTimeMillis();

                @Override
                public void handleResponse(Request response) {
                    System.out
                            .println(": Response for request ["
                                    //+ command
                                    + "] = "
                                    + ((RequestPacket) response).getResponseValue()
                                    + " received in "
                                    + (System.currentTimeMillis() - createTime)
                                    + "ms");

                }
            });
            Thread.sleep(100);
        }
        client.close();
    }
}
