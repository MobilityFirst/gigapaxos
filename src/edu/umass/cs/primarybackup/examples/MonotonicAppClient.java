package edu.umass.cs.primarybackup.examples;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class MonotonicAppClient extends ReconfigurableAppClientAsync<MonotonicAppRequest> {

    public MonotonicAppClient() throws IOException {
        super();
        // TODO: confirm the weird value of the default configuration
        //  super(new HashSet<InetSocketAddress>(List.of(new InetSocketAddress(3000))));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String serviceName = "monotonic-service-" + Util.getRandomNumberBetween(0, 9999);
        String initialState = "";

        MonotonicAppClient client = new MonotonicAppClient();
        Thread.sleep(1000);

        Set<InetSocketAddress> servers = client.getDefaultServers();
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

        // send request to one of the replicated service
        InetSocketAddress replicaAddress = new InetSocketAddress(2100);
        for (int i = 0; i < 10; i++) {
            Request response = client.sendRequest(ReplicableClientRequest.wrap(
                    new MonotonicAppRequest(
                            serviceName,
                            MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND)
            ), replicaAddress);
            System.out.printf(">> %s\n", ((MonotonicAppRequest) response).getResponseValue());

            response = client.sendRequest(ReplicableClientRequest.wrap(
                    new MonotonicAppRequest(
                            serviceName,
                            MonotonicAppRequest.MONOTONIC_APP_GET_SEQUENCE_COMMAND)
            ), replicaAddress);
            System.out.printf(">> %s\n", ((MonotonicAppRequest) response).getResponseValue());
        }

        Thread.sleep(1000);
        client.close();
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        assert stringified != null && stringified.startsWith(MonotonicAppRequest.SERIALIZED_PREFIX);
        return MonotonicAppRequest.createFromString(stringified);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> types = new HashSet<>();
        types.add(MonotonicAppRequest.MonotonicAppRequestType);
        return types;
    }
}
