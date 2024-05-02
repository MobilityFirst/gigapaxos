package edu.umass.cs.primarybackup.tests;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.primarybackup.PrimaryBackupManager;
import edu.umass.cs.reconfiguration.PrimaryBackupReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class TestPrimaryBackup {

    protected static final String CONTROL_PLANE_ID = "RC0";
    protected static final String NODE_1_ID = "node1";
    protected static final String NODE_2_ID = "node2";
    protected static final String NODE_3_ID = "node3";
    protected static final String SERVICE_NAME = "monotonic-app-test-001";

    protected record CoordinatorNode(PrimaryBackupReplicaCoordinator<String> coordinator,
                                     PrimaryBackupManager manager,
                                     MonotonicTestApp app,
                                     JSONMessenger<String> messenger) {
    }

    protected static Map<String, CoordinatorNode> startThreeNodesWithMonotonicApp(
            ReconfigurableNodeConfig<String> config
    ) {
        Map<String, CoordinatorNode> servers = new HashMap<>();

        // initializing app in node-1
        String node1ID = NODE_1_ID;
        MonotonicTestApp appAtNode1 = new MonotonicTestApp();
        ReconfigurationPacketDemultiplexer pdAtNode1 = TestPrimaryBackup.
                createDefaultPacketDemultiplexer(
                        node1ID, config);
        JSONMessenger<String> messengerAtNode1 = TestPrimaryBackup.createNodeDefaultMessenger(
                node1ID, config, pdAtNode1);
        PrimaryBackupReplicaCoordinator<String> node1 =
                new PrimaryBackupReplicaCoordinator<>(
                        appAtNode1,
                        node1ID,
                        TestPrimaryBackup.getDefaultStringifier(),
                        messengerAtNode1
                );
        TestPrimaryBackup.registerHandlerForDemultiplexer(pdAtNode1, node1);
        servers.put(node1ID, new CoordinatorNode(
                node1,
                node1.getPrimaryBackupManager(),
                appAtNode1,
                messengerAtNode1)
        );

        // initializing app in node-2
        String node2ID = NODE_2_ID;
        MonotonicTestApp appAtNode2 = new MonotonicTestApp();
        ReconfigurationPacketDemultiplexer pdAtNode2 = TestPrimaryBackup.
                createDefaultPacketDemultiplexer(node2ID, config);
        JSONMessenger<String> messengerAtNode2 = TestPrimaryBackup.createNodeDefaultMessenger(
                node2ID, config, pdAtNode2);
        PrimaryBackupReplicaCoordinator<String> node2 =
                new PrimaryBackupReplicaCoordinator<>(
                        appAtNode2,
                        node2ID,
                        TestPrimaryBackup.getDefaultStringifier(),
                        messengerAtNode2
                );
        TestPrimaryBackup.registerHandlerForDemultiplexer(pdAtNode2, node2);
        servers.put(node2ID, new CoordinatorNode(
                node2,
                node2.getPrimaryBackupManager(),
                appAtNode2,
                messengerAtNode2)
        );

        // initializing app in node-3
        String node3ID = NODE_3_ID;
        MonotonicTestApp appAtNode3 = new MonotonicTestApp();
        ReconfigurationPacketDemultiplexer pdAtNode3 = TestPrimaryBackup.
                createDefaultPacketDemultiplexer(
                        node3ID, config);
        JSONMessenger<String> messengerAtNode3 = TestPrimaryBackup.createNodeDefaultMessenger(
                node3ID, config, pdAtNode3);
        PrimaryBackupReplicaCoordinator<String> node3 =
                new PrimaryBackupReplicaCoordinator<>(
                        appAtNode3,
                        node3ID,
                        TestPrimaryBackup.getDefaultStringifier(),
                        messengerAtNode3
                );
        TestPrimaryBackup.registerHandlerForDemultiplexer(pdAtNode3, node3);
        servers.put(node3ID, new CoordinatorNode(
                node3,
                node3.getPrimaryBackupManager(),
                appAtNode3,
                messengerAtNode3)
        );

        return servers;
    }

    /**
     * cleanPreviousState removes all Gigapaxos state stored in the /tmp/gigapaxos directory.
     * This ensures that our tests run without any previous state.
     */
    protected static void cleanPreviousState() {
        try {
            String command = "rm -rf /tmp/gigapaxos/";
            ProcessBuilder pb = new ProcessBuilder();
            pb.command(command.split("\\s+"));
            Process process = pb.start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("failed to remove previous state");
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void killServers(Map<String, CoordinatorNode> nodes)
            throws InterruptedException {
        for (CoordinatorNode n : nodes.values()) {
            n.coordinator.close();
        }
        Thread.sleep(3000);
    }

    protected static void printServers(ReconfigurableNodeConfig<String> config) {
        System.out.print(">> List of active replicas:\n");
        for (Map.Entry<String, InetSocketAddress> ar : config.getActiveReplicasReadOnly().
                entrySet()) {
            System.out.printf(" - %s %s\n", ar.getKey(), ar.getValue().toString());
        }
        System.out.print(">> List of reconfigurators:\n");
        for (Map.Entry<String, InetSocketAddress> rc : config.getReconfiguratorsReadOnly().
                entrySet()) {
            System.out.printf(" - %s %s\n", rc.getKey(), rc.getValue().toString());
        }
        System.out.print("\n\n\n");
    }

    protected static Map<String, InetSocketAddress> getDefaultActiveReplicas() {
        Map<String, InetSocketAddress> actives = new HashMap<>();
        actives.put(NODE_1_ID, new InetSocketAddress("localhost", 2000));
        actives.put(NODE_2_ID, new InetSocketAddress("localhost", 2001));
        actives.put(NODE_3_ID, new InetSocketAddress("localhost", 2002));
        return actives;
    }

    protected static Map<String, InetSocketAddress> getDefaultReconfigurators() {
        Map<String, InetSocketAddress> rc = new HashMap<>();
        rc.put(CONTROL_PLANE_ID, new InetSocketAddress("localhost", 3000));
        return rc;
    }

    private static Stringifiable<String> getDefaultStringifier() {
        return strValue -> strValue;
    }

    private static ReconfigurationPacketDemultiplexer createDefaultPacketDemultiplexer(
            String nodeID,
            NodeConfig<String> config) {
        return new ReconfigurationPacketDemultiplexer(
                config).setThreadName(nodeID);
    }

    private static void registerHandlerForDemultiplexer(ReconfigurationPacketDemultiplexer pd,
                                                        PrimaryBackupReplicaCoordinator<String>
                                                                coordinator) {
        pd.setAppRequestParser(coordinator.getRequestParser());
        pd.register(
                PrimaryBackupManager.getAllPrimaryBackupPacketTypes(),
                (parsedMessage, header) -> {
                    try {
                        coordinator.coordinateRequest(parsedMessage, null);
                    } catch (IOException | RequestParseException e) {
                        throw new RuntimeException(e);
                    }
                    return true;
                });
    }

    private static JSONMessenger<String>
    createNodeDefaultMessenger(String nodeID,
                               NodeConfig<String> config,
                               AbstractPacketDemultiplexer<Request> pd) {
        try {
            MessageNIOTransport<String, JSONObject> nioTransport = new MessageNIOTransport<>(
                    nodeID,
                    config,
                    pd,
                    true,
                    SSLDataProcessingWorker.SSL_MODES.CLEAR
            );
            System.out.printf(">> initializing messenger for %s at %s\n",
                    nodeID, nioTransport.getListeningSocketAddress());
            assert (nioTransport.getListeningSocketAddress().equals(
                    new InetSocketAddress(
                            config.getNodeAddress(nodeID),
                            config.getNodePort(nodeID)))) :
                    "Unable to start NIOTransport at socket address";
            return new JSONMessenger<>(nioTransport);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
