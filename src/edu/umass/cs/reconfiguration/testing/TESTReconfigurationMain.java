package edu.umass.cs.reconfiguration.testing;

import java.io.IOException;

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
 */
public class TESTReconfigurationMain {

	static ReconfigurableNodeConfig<String> dnc = null;

	private static void startReconfigurators(String[] args) throws IOException {
		System.out.print("Creating reconfigurator(s) [ ");
		for (int i = 0; i < Config.getGlobalInt(TRC.NUM_ACTIVES); i++) {
			new DefaultReconfigurableNode(
					Config.getGlobalString(TRC.RC_PREFIX) + i,
					// must use a different nodeConfig for each
					dnc = new DefaultNodeConfig<String>(
							TESTReconfigurationConfig.getLocalActives(),
							TESTReconfigurationConfig.getLocalReconfigurators()),
					args, false);
			System.out.print(Config.getGlobalString(TRC.RC_PREFIX) + i + " ");
		}
		System.out.println("]");
	}

	private static void startActives(String[] args) throws IOException {
		System.out.print("Creating active(s) [ ");
		for (int i = 0; i < Config.getGlobalInt(TRC.NUM_ACTIVES); i++) {
			new DefaultReconfigurableNode(
					Config.getGlobalString(TRC.AR_PREFIX) + i,
					// must use a different nodeConfig for each
					dnc = new DefaultNodeConfig<String>(
							TESTReconfigurationConfig.getLocalActives(),
							TESTReconfigurationConfig.getLocalReconfigurators()),
					args, false);
			System.out.print(Config.getGlobalString(TRC.AR_PREFIX) + i + " ");
		}
		System.out.println("]");
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		ReconfigurationConfig.setConsoleHandler();
		TESTReconfigurationConfig.load();
		dnc = new DefaultNodeConfig<String>(
				TESTReconfigurationConfig.getLocalActives(),
				TESTReconfigurationConfig.getLocalReconfigurators());
		System.out.println(dnc.getNodeIDs());
		startReconfigurators(args);
		startActives(args);
		(new TESTReconfigurationClient()).startTests();
	}
}
