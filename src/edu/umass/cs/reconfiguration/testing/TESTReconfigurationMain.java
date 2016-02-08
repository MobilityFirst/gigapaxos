package edu.umass.cs.reconfiguration.testing;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.ReconfigurableNode.DefaultReconfigurableNode;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationConfig.TRC;
import edu.umass.cs.utils.Config;

/**
 * @author arun
 * 
 *         The tests in TESTReconfigurationClient invoked herein test basic
 *         operations like creation, request active replicas, app requests, and
 *         deletions with reconfigurations enabled.
 * 
 *         TODOS:
 * 
 *         (1) Add test for reconfiguring reconfigurators like in
 *         {@link ReconfigurableClient}.
 * 
 *         (2) Add test involving crashes of reconfigurators or active replicas.
 * 
 *         (3) Add tests with different numbers of reconfigurators
 *         and actives.
 *
 */
public class TESTReconfigurationMain {

	static ReconfigurableNodeConfig<String> dnc = null;

	private static Set<ReconfigurableNode<?>> startReconfigurators(String[] args)
			throws IOException {
		Set<ReconfigurableNode<?>> createdNodes = new HashSet<ReconfigurableNode<?>>();
		System.out.print("Creating reconfigurator(s) [ ");
		for (int i = 0; i < Config.getGlobalInt(TRC.NUM_ACTIVES); i++) {
			createdNodes
					.add(new DefaultReconfigurableNode(
							Config.getGlobalString(TRC.RC_PREFIX) + i,
							// must use a different nodeConfig for each
							dnc = new DefaultNodeConfig<String>(
									TESTReconfigurationConfig.getLocalActives(),
									TESTReconfigurationConfig
											.getLocalReconfigurators()), args,
							false));
			System.out.print(Config.getGlobalString(TRC.RC_PREFIX) + i + " ");
		}
		System.out.println("]");
		return createdNodes;
	}

	private static Set<ReconfigurableNode<?>> startActives(String[] args)
			throws IOException {
		Set<ReconfigurableNode<?>> createdNodes = new HashSet<ReconfigurableNode<?>>();
		System.out.print("Creating active(s) [ ");
		for (int i = 0; i < Config.getGlobalInt(TRC.NUM_ACTIVES); i++) {
			createdNodes
					.add(new DefaultReconfigurableNode(
							Config.getGlobalString(TRC.AR_PREFIX) + i,
							// must use a different nodeConfig for each
							dnc = new DefaultNodeConfig<String>(
									TESTReconfigurationConfig.getLocalActives(),
									TESTReconfigurationConfig
											.getLocalReconfigurators()), args,
							false));
			System.out.print(Config.getGlobalString(TRC.AR_PREFIX) + i + " ");
		}
		System.out.println("]");
		return createdNodes;
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test0() throws IOException, InterruptedException {
		(new TESTReconfigurationClient(TESTReconfigurationConfig
				.getLocalReconfigurators())).allTests().close();
	}

	private static Set<ReconfigurableNode<?>> reconfigurators = null,
			actives = null;

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void startLocalServers() throws IOException,
			InterruptedException {
		String[] args = new String[0];
		dnc = new DefaultNodeConfig<String>(
				TESTReconfigurationConfig.getLocalActives(),
				TESTReconfigurationConfig.getLocalReconfigurators());
		System.out.println(dnc.getNodeIDs());
		reconfigurators = startReconfigurators(args);
		actives = startActives(args);
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@AfterClass
	public static void closeServers() throws IOException, InterruptedException {
		// all tests should be complete at this point
		for (ReconfigurableNode<?> node : reconfigurators)
			node.close();
		for (ReconfigurableNode<?> node : actives)
			node.close();
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException {
		ReconfigurationConfig.setConsoleHandler();
		TESTReconfigurationConfig.load();

		Result result = JUnitCore.runClasses(TESTReconfigurationMain.class);
		for (Failure failure : result.getFailures())
			System.out.println(failure.toString());

	}
}
