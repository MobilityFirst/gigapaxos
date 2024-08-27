package edu.umass.cs.cops.examples;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KeyValueAppClient extends ReconfigurableAppClientAsync<KeyValueAppRequest> {

    public static void main(String[] args) throws IOException, InterruptedException {
        String serviceName = "key-value-service-" + Util.getRandomNumberBetween(0, 9999);
        String initialState = "";

        KeyValueAppClient client = new KeyValueAppClient();
        Thread.sleep(1000);

        Set<InetSocketAddress> servers = client.getDefaultServers();
        System.out.println("List of servers:");
        for (InetSocketAddress s : servers) {
            System.out.printf(">> %s %d\n", s.getAddress().toString(), s.getPort());
        }

        // deploy the replica group
        try {
            System.out.println(">> deploying " + serviceName + " ...");
            RequestFuture<ClientReconfigurationPacket> future = client.sendRequest(
                    new CreateServiceName(serviceName, initialState));
            ClientReconfigurationPacket responsePacket = future.get();
            CreateServiceName response = (CreateServiceName) responsePacket;
            System.out.println(">> response: " + response.toString());
        } catch (ReconfigurationException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        // prepare a set of key and value
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            keys.add("key-" + i%2);
            values.add("value-" + i);
        }

        // send request to one of the replicated service
        long myClientID = 120312;
        List<String> currentDeps = new ArrayList<>();
        InetSocketAddress replicaAddress = new InetSocketAddress(2000);
        for (int i = 0; i < 10; i++) {
            System.out.println(">>>> Sending request ...");
            long requestID = System.currentTimeMillis();
            // send PUT request
            KeyValueAppWriteRequest appWriteRequest =
                    new KeyValueAppWriteRequest(serviceName, myClientID, requestID, keys.get(i), values.get(i));
            appWriteRequest.dependencies = currentDeps;
            Request response = client.sendRequest(
                    ReplicableClientRequest.wrap(appWriteRequest), replicaAddress);
            System.out.printf(">> PUT %s response=%s version=%d deps=%s\n",
                    appWriteRequest.getKey(),
                    ((KeyValueAppRequest) response).getResponseValue(),
                    ((KeyValueAppRequest) response).responseVersion,
                    ((KeyValueAppRequest) response).dependencies);
            currentDeps = ((KeyValueAppRequest) response).dependencies;

            // send GET request
            KeyValueAppRequest appRequest = new KeyValueAppRequest(serviceName, myClientID, requestID, keys.get(i), null);
            appRequest.dependencies = currentDeps;
            response = client.sendRequest(ReplicableClientRequest.wrap(appRequest), replicaAddress);
            System.out.printf(">> GET %s response=%s version=%d deps=%s\n",
                    appRequest.getKey(),
                    ((KeyValueAppRequest) response).getResponseValue(),
                    ((KeyValueAppRequest) response).responseVersion,
                    ((KeyValueAppRequest) response).dependencies);
            currentDeps = ((KeyValueAppRequest) response).dependencies;
        }

        Thread.sleep(1000);
        client.close();

    }

    public KeyValueAppClient() throws IOException {
        super();
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        assert stringified != null && stringified.startsWith(KeyValueAppRequest.SERIALIZED_PREFIX) :
            "KeyValueAppClient: Invalid encoded request";
        return KeyValueAppRequest.createFromString(stringified);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> types = new HashSet<>();
        types.add(KeyValueAppRequest.KeyValueAppRequestType);
        return types;
    }

}
