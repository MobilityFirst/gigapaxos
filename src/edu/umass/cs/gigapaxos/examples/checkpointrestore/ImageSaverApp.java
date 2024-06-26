package edu.umass.cs.gigapaxos.examples.checkpointrestore;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Set;

public class ImageSaverApp implements Replicable {

    private final Path fileName;

    public ImageSaverApp(String[] args) {
        super();
        this.fileName = Paths.get(PaxosConfig.getAsProperties().getProperty("savedPool") + "image_" + args[0] + ".jpg");
    }

    @Override
    public boolean execute(Request request) {
        if (request instanceof RequestPacket) {
            try {
                String requestValue = ((RequestPacket) request).requestValue;
                byte[] image = Base64.getDecoder().decode(requestValue);
                Files.write(this.fileName, image);
                ((RequestPacket) request).setResponse("Saved Successfully");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else
            System.err.println("Unknown request type: " + request.getRequestType());


        return true;
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return this.execute(request);
    }

    @Override
    public String checkpoint(String name) {

        try {
            byte[] image = Files.readAllBytes(this.fileName);
            System.out.println("Checkpoint created");
            return new String(image, StandardCharsets.ISO_8859_1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean restore(String name, String state) {
        if (state == null || state.equals(Config.getGlobalString(PaxosConfig.PC
                .DEFAULT_NAME_INITIAL_STATE))) {
            return true;
        }
        try {
            System.out.println("Restoring using the last saved checkpoint");
            byte[] image = state.getBytes(StandardCharsets.ISO_8859_1);
            Files.write(this.fileName, image);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return Set.of();
    }
}
