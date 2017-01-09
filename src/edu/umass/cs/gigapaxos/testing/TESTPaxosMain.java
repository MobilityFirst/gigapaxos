/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.testing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.utils.Config;

/**
 * @author V. Arun
 * 
 *         The main class for testing gigapaxos on a single machine with virtual
 *         nodes.
 */
@SuppressWarnings("javadoc")
public class TESTPaxosMain {
	private HashMap<Integer, TESTPaxosNode> nodes = new HashMap<Integer, TESTPaxosNode>();
	ScheduledExecutorService execpool = Executors.newScheduledThreadPool(5);

	public TESTPaxosMain() throws IOException {
		// initTESTPaxosMain();
	}

	TESTPaxosMain initTESTPaxosMain() throws IOException {
		Set<Integer> nodeIDs = TESTPaxosConfig.getNodes();
		for (int id : nodeIDs) {
			TESTPaxosNode node = null;
			try {
				node = new TESTPaxosNode(id);
			} catch (Exception e) {
				e.printStackTrace();
			}
			assert (node != null) : "Failed to create node " + id;
			nodes.put(id, node);
		}
		return this;
	}

	protected void assertRSMInvariant(String paxosID) {
		String state = null, prevState = null;
		for (int id : TESTPaxosConfig.getGroup(paxosID)) {
			if (TESTPaxosConfig.isCrashed(id))
				continue;
			if (state == null)
				state = nodes.get(id).getAppState(paxosID);
			else
				assert (state.equals(prevState));
			prevState = state;
		}
	}


	static long t = System.currentTimeMillis();

	/*
	 * This method tests single-node paxos and exits gracefully at the end by
	 * closing all nodes and associated paxos managers. Calling this method
	 * again with testRecovery=true will test recovery mode.
	 */
	public void testPaxos(boolean recovery) {
		TESTPaxosConfig.setSingleNodeTest();
		try {
			/*************** Setting up servers below ***************************/

			TESTPaxosMain tpMain = this.initTESTPaxosMain();
			// creates all nodes, each with its paxos manager and app

			// no-op if recovery enabled coz we need consistent groups across
			// runs
			TESTPaxosConfig.setRandomGroups(Config
					.getGlobalInt(TC.PRE_CONFIGURED_GROUPS));

			// creates paxos groups (may not create if recovering)
			for (int id : tpMain.nodes.keySet()) {
				tpMain.nodes.get(id).createDefaultGroupInstances();
				tpMain.nodes.get(id).createNonDefaultGroupInstanes(
						Config.getGlobalInt(TC.NUM_GROUPS));
			}

			/*************** End of server setup ***************************/

			/*************** Client requests/responses below ****************/

			TESTPaxosClient[] clients = TESTPaxosClient.setupClients(null);
			TESTPaxosShutdownThread.register(clients);
			int numReqs = Config.getGlobalInt(TC.NUM_REQUESTS);

			Thread.sleep(2000);

			Config.getConfig(TC.class).put(TC.PROBE_CAPACITY.toString(), false);
			TESTPaxosClient.twoPhaseTest(numReqs, clients);

			// sleep for a bit to ensure all replicas get everything
			Thread.sleep(2000);

			for (TESTPaxosNode node : tpMain.nodes.values()) {
				node.close();
			}
			for (TESTPaxosClient client : clients)
				client.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void testPaxosAfterRecovery() {
		testPaxos(true);
	}

	private static void processArgs(String[] args) {
		PaxosManager.startWithCleanDB(TESTPaxosConfig.shouldCleanDB(args));
	}

	/**
	 * Basic test with recovery. The timeout here should be at least
	 * 4*NUM_REQUESTS/TOTAL_LOAD + a recovery time that depends upon
	 * NUM_REQUESTS*NUM_NODES*T, where T is roughly 1/10^3, so, for example, for
	 * 33K requests and 3 nodes roughly corresponds to a recovery time of
	 * 33K*3*1/10^3 ~= 100 seconds.
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	private static final long timeoutMillis = 250000;

	@Test(timeout = timeoutMillis)
	public void testWithRecovery() throws InterruptedException {
		int runtimeEstimate = (int) (
		// time to create groups at under 2ms per group
		2 / 1000 / 1000 * 2 * Config.getGlobalInt(TC.NUM_GROUPS)
				* Config.getGlobalInt(TC.NUM_NODES)
				+
				// time to send the requests for four runs
				4 * Config.getGlobalInt(TC.NUM_REQUESTS) * 1.0
				/ Config.getGlobalDouble(TC.TOTAL_LOAD) +
		// recovery time
		2 * Config.getGlobalInt(TC.NUM_REQUESTS)
				* Config.getGlobalInt(TC.NUM_NODES) * 0.3 / 1000);
		Assert.assertTrue("Increase the timeout for this test to at least "
				+ runtimeEstimate * 1000, timeoutMillis >= runtimeEstimate);
		testPaxos(false);
		Thread.sleep(1000);
		System.out
				.println("\n############### Testing with recovery ################\n");
		TESTPaxosConfig.setCleanDB(false);
		PaxosManager.startWithCleanDB(false);
		testPaxosAfterRecovery();
	}

	public static void main(String[] args) throws InterruptedException,
			IOException {
		TESTPaxosConfig.load();
		TESTPaxosConfig.setConsoleHandler();

		processArgs(args);
		System.out
				.println("\nThis is a single-node test. For distributed testing, "
						+ "use TESTPaxosNode and TESTPaxosClient with the appropriate "
						+ "configuration file.\nInitiating single-node test...\n");
		TESTPaxosConfig.setAssertRSMInvariant(true);
		//(new TESTPaxosMain()).testWithRecovery();

		Result result = JUnitCore.runClasses(TESTPaxosMain.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.toString());
			failure.getException().printStackTrace();
		}
	}
}
