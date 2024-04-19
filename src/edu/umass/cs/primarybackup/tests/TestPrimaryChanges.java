package edu.umass.cs.primarybackup.tests;

import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Set;

public class TestPrimaryChanges {

    // TODO: implement these test scenario
    //  1. create and initialize 3 active replicas: AR0, AR1, AR2
    //  2. create and initialize replica group using PrimaryBackupReplicaCoordinator
    //     with MonotonicApp as the BackupableApplication
    //  3. identify the primary node
    //  4. send a bunch of request to primary node
    //  5. abruptly change the primary
    //  6. send a bunch of request to the new primary
    //  7. assert that the application satisfies monotonicity

    @Test
    public void TestPrimaryChangesWithThreeNodes() throws IOException, InterruptedException {
        ReconfigurableNode controlPlaneNode = new ReconfigurableNode.DefaultReconfigurableNode(
                "RC0",
                getDefaultReconfigurableConfig(),
                null,
                true
        );

        Thread.sleep(5000);
    }

    private ReconfigurableNodeConfig<String> getDefaultReconfigurableConfig() {
        return new ReconfigurableNodeConfig<>() {
            @Override
            public Set<String> getActiveReplicas() {
                return null;
            }

            @Override
            public Set<String> getReconfigurators() {
                return null;
            }

            @Override
            public boolean nodeExists(String id) {
                return false;
            }

            @Override
            public InetAddress getNodeAddress(String id) {
                return null;
            }

            @Override
            public InetAddress getBindAddress(String id) {
                return null;
            }

            @Override
            public int getNodePort(String id) {
                return 0;
            }

            @Override
            public Set<String> getNodeIDs() {
                return null;
            }

            @Override
            public String valueOf(String strValue) {
                return null;
            }
        };
    }

}
