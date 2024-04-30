package edu.umass.cs.primarybackup.tests;

import edu.umass.cs.primarybackup.examples.MonotonicAppRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static edu.umass.cs.primarybackup.tests.TestPrimaryBackup.*;

public class TestSendRequestToAllNodes {

    @Test
    public void Test2_TestSendRequestToAllNodes() throws
            InterruptedException, IOException, RequestParseException {

        String scenario = """
                
                Scenario for TestSendRequestToAllNodes:
                  1. Create and initialize 3 active replicas: AR0, AR1, AR2.
                  2. Create and initialize replica group using PrimaryBackupReplicaCoordinator with
                     MonotonicApp as the BackupableApplication (extended in the MonotonicTestApp).
                  3. AR1 is elected as Paxos' coordinator and as the primary given the option:
                     ENABLE_STARTUP_LEADER_ELECTION=false.
                  4. Send client requests to AR0, AR1, and AR2, respectively.
                  5. Assert that all the replicas have the same state at the end.
                  
                Expectation:
                  - Sending client requests to non-primary nodes should work. The entry node
                    will forward the requests to the primary.
                  - At the end, all the replicas must have the same state, even for non-
                    deterministic application, such as MonotonicApp.
                    
                    
                """;
        System.out.println(scenario);

        TestPrimaryBackup.cleanPreviousState();

        System.out.print("\n\n ===== Step-0: Preparing config ... \n\n");
        ReconfigurableNodeConfig<String> config = new DefaultNodeConfig<>(
                TestPrimaryBackup.getDefaultActiveReplicas(),
                TestPrimaryBackup.getDefaultReconfigurators()
        );
        TestPrimaryBackup.printServers(config);
        Thread.sleep(1000);


        System.out.print("\n\n ===== Step-1: Initializing PrimaryBackup in 3 nodes ... \n\n");
        var servers = TestPrimaryBackup.startThreeNodesWithMonotonicApp(config);
        var node1 = servers.get(NODE_1_ID).coordinator();
        var node2 = servers.get(NODE_2_ID).coordinator();
        var node3 = servers.get(NODE_3_ID).coordinator();
        var appAtNode1 = servers.get(NODE_1_ID).app();
        var appAtNode2 = servers.get(NODE_2_ID).app();
        var appAtNode3 = servers.get(NODE_3_ID).app();
        System.out.print("\n\n ===== PrimaryBackupManagers are initialized ===== \n\n");
        Thread.sleep(3000);

        System.out.print("\n\n ===== Step-2: Initializing Applications in 3 nodes ... \n\n");
        int zeroPlacementEpoch = 0;
        String initialState = null;
        String serviceName = SERVICE_NAME;
        Set<String> nodes = new HashSet<>(List.of(new String[]{NODE_1_ID, NODE_2_ID, NODE_3_ID}));
        node1.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
        node3.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
        Thread.sleep(500);
        node2.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
        System.out.printf(" replica-group: %s \n", node1.getReplicaGroup(serviceName));

        System.out.print("\n\n ===== Applications are initialized ===== \n\n\n\n");
        Thread.sleep(3000);


        System.out.print("\n\n ===== Step-3: Sending app requests to 3 nodes ... \n\n");
        for (int i = 0; i < 3; i++) {
            // preparing the request packet
            MonotonicAppRequest appRequest = new MonotonicAppRequest(
                    serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);

            // handling the request packet
            node1.coordinateRequest(
                    ReplicableClientRequest.wrap(appRequest),
                    (executedRequest, handler) -> {
                        System.out.printf("\n\nrequest is executed :) %s \n\n", executedRequest);
                    });
             Thread.sleep(500);
        }
        for (int i = 0; i < 3; i++) {
            // preparing the request packet
            MonotonicAppRequest appRequest = new MonotonicAppRequest(
                    serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);

            // handling the request packet
            node2.coordinateRequest(
                    ReplicableClientRequest.wrap(appRequest),
                    (executedRequest, handler) -> {
                        System.out.printf("\n\nrequest is executed :) %s \n\n", executedRequest);
                    });
             Thread.sleep(500);
        }
        for (int i = 0; i < 3; i++) {
            // preparing the request packet
            MonotonicAppRequest appRequest = new MonotonicAppRequest(
                    serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);

            // handling the request packet
            node3.coordinateRequest(
                    ReplicableClientRequest.wrap(appRequest),
                    (executedRequest, handler) -> {
                        System.out.printf("\n\nrequest is executed :) %s \n\n", executedRequest);
                    });
             Thread.sleep(500);
        }

        Thread.sleep(3000);
        System.out.print("\n\n\n ========== Final result at the end of execution:\n");
        System.out.printf("++ monotonic-sequence at node 1: %s\n",
                appAtNode1.test_GetSequenceAsString());
        System.out.printf("++ monotonic-sequence at node 2: %s\n",
                appAtNode2.test_GetSequenceAsString());
        System.out.printf("++ monotonic-sequence at node 3: %s\n",
                appAtNode2.test_GetSequenceAsString());

        appAtNode1.test_AssertMonotonicallyIncreasingNumbers();
        appAtNode2.test_AssertMonotonicallyIncreasingNumbers();
        appAtNode3.test_AssertMonotonicallyIncreasingNumbers();
        assert appAtNode1.test_GetSequenceAsString() != null :
                "The end state must not be null as we sent several app requests";
        assert !appAtNode1.test_GetSequenceAsString().isEmpty() :
                "The end state must not be empty as we sent several app requests";
        assert Objects.equals(
                appAtNode1.test_GetSequenceAsString(),
                appAtNode2.test_GetSequenceAsString()) :
                "State in node1 != node2. At the end all replicas must have the same state";
        assert Objects.equals(
                appAtNode2.test_GetSequenceAsString(),
                appAtNode3.test_GetSequenceAsString()) :
                "State in node2 != node3. At the end all replicas must have the same state";

        killServers(servers);
    }

}
