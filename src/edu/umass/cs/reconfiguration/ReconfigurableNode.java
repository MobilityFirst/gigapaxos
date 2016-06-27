/* Copyright (c) 2015 University of Massachusetts
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
 * Initial developer(s): V. Arun */
package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONObject;

import edu.umass.cs.gigapaxos.AbstractPaxosLogger;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPacketDemultiplexer;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * 
 * @author V. Arun
 *
 * @param <NodeIDType>
 *            A generic type for representing node identifiers. It must support
 *            an explicitly overridden toString() method that converts
 *            NodeIDType to String, and the NodeConfig object supplied to this
 *            class' constructor must support a valueOf(String) method that
 *            returns back the original NodeIDType. Thus, even though NodeIDType
 *            is generic, a one-to-one mapping between NodeIDType and String is
 *            necessary.
 */
public abstract class ReconfigurableNode<NodeIDType> {

	protected final NodeIDType myID;
	protected final ReconfigurableNodeConfig<NodeIDType> nodeConfig;
	protected final JSONMessenger<NodeIDType> messenger;

	protected abstract AbstractReplicaCoordinator<NodeIDType> createAppCoordinator();

	private final Set<ActiveReplica<NodeIDType>> activeReplicas = new HashSet<ActiveReplica<NodeIDType>>();
	private final Set<Reconfigurator<NodeIDType>> reconfigurators = new HashSet<Reconfigurator<NodeIDType>>();

	/**
	 * @param id
	 * @param nodeConfig
	 * @throws IOException
	 */
	public ReconfigurableNode(NodeIDType id,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig) throws IOException {
		this(id, nodeConfig, null, false);
	}

	/**
	 * Close gracefully.
	 */
	public void close() {
		for (ActiveReplica<NodeIDType> node : this.activeReplicas) {
			node.close();
		}
		for (Reconfigurator<NodeIDType> node : this.reconfigurators) {
			node.close();
		}
		this.messenger.stop();
		Reconfigurator.getLogger().info(
				"----------" + this + " closed----->||||");
	}

	private AbstractReplicaCoordinator<NodeIDType> createApp(String[] args,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig) {
		if (ReconfigurationConfig.application != null) {
			Replicable app = ReconfigurationConfig.createApp(args);
			if (app instanceof ClientMessenger)
				((ClientMessenger) app).setClientMessenger(messenger);
			else
				Reconfigurator
						.getLogger()
						.info(app.getClass().getSimpleName()
								+ " does not implement "
								+ ClientMessenger.class.getSimpleName()
								+ ", which means the app should either rely on "
								+ ClientRequest.class.getSimpleName()
								+ " or not expect to send "
								+ " responses back to clients or rely on alternate means for messaging.");
			PaxosReplicaCoordinator<NodeIDType> prc = new PaxosReplicaCoordinator<NodeIDType>(
					app, myID, nodeConfig, messenger);
			Reconfigurator.getLogger().info(
					"Creating default group with "
							+ nodeConfig.getActiveReplicas());
			prc.createDefaultGroupNodes(app.getClass().getSimpleName() + "0",
					nodeConfig.getActiveReplicas(), nodeConfig);
			return prc;
		} else {
			AbstractReplicaCoordinator<NodeIDType> appCoordinator = this
					.createAppCoordinator();
			if (appCoordinator instanceof PaxosReplicaCoordinator)
				Reconfigurator
						.getLogger()
						.warning(
								"Using createAppCoordinator() is discouraged for "
										+ "applications simply using paxos as the coordiantion protocol. Implement "
										+ "Application.createApp(String[]) or Application.createApp() to construct "
										+ "the application instance instead.");
			return appCoordinator;
		}
	}

	/**
	 * @param id
	 *            Node ID of this ReconfigurableNode being created.
	 * @param nodeConfig
	 *            Maps node IDs of active replicas and reconfigurators to their
	 *            socket addresses.
	 * @param args
	 * @param startCleanSlate
	 *            Used to join newly added nodes.
	 * 
	 * @throws IOException
	 *             Thrown if networking functions can not be successfully
	 *             initialized. A common reason for this exception is that the
	 *             socket addresses corresponding to the supplied 'id' argument
	 *             are not local, i.e., the node with this id should not be
	 *             created on this machine in the first place, or if the id is
	 *             not present at all in the supplied 'nodeConfig' argument.
	 */
	public ReconfigurableNode(NodeIDType id,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig, String[] args,
			boolean startCleanSlate) throws IOException {
		this.myID = id;
		this.nodeConfig = nodeConfig;

		AbstractPaxosLogger.fileLock(id);

		AbstractJSONPacketDemultiplexer pd;

		String err = null;
		if (!nodeConfig.getActiveReplicas().contains(id)
				&& !nodeConfig.getReconfigurators().contains(id)) {
			Reconfigurator.getLogger().severe(
					err = "Node " + id
							+ " not present in NodeConfig argument \n  "
							+ nodeConfig.getActiveReplicas() + "\n  "
							+ nodeConfig.getReconfigurators());
			throw new IOException(err);
		}
		// else
		Reconfigurator.getLogger().info(
				this + ":" + this.myID + " listening on "
						+ nodeConfig.getNodeAddress(myID) + ":"
						+ nodeConfig.getNodePort(myID));
		MessageNIOTransport<NodeIDType, JSONObject> niot = null;
		InetSocketAddress isa = new InetSocketAddress(
				nodeConfig.getNodeAddress(myID), nodeConfig.getNodePort(myID));
		// else we have something to start
		messenger = (new JSONMessenger<NodeIDType>(
				(niot = new MessageNIOTransport<NodeIDType, JSONObject>(
						ReconfigurableNode.this.myID, nodeConfig,
						(pd = new ReconfigurationPacketDemultiplexer()
								.setThreadName(ReconfigurableNode.this.myID
										.toString())), true,
						ReconfigurationConfig.getServerSSLMode()))));
		if (!niot.getListeningSocketAddress().equals(isa)
				&& Config.getGlobalBoolean(PC.STRICT_ADDRESS_CHECKS)) {
			Reconfigurator
					.getLogger()
					.severe(err = this
							+ " unable to start ReconfigurableNode at socket address "
							+ isa);
			throw new IOException(err);
		}
		// else created messenger, may still fail to create client messenger

		if (nodeConfig.getActiveReplicas().contains(id)) {
			// create active
			ActiveReplica<NodeIDType> activeReplica = new ActiveReplica<NodeIDType>(
			// createAppCoordinator(),
					createApp(args, nodeConfig), nodeConfig, messenger);
			this.activeReplicas.add(activeReplica);
			// getPacketTypes includes app's packets
			pd.register(activeReplica.getPacketTypes(), activeReplica);
		} else if (nodeConfig.getReconfigurators().contains(id)) {
			// create reconfigurator
			Reconfigurator<NodeIDType> reconfigurator = new Reconfigurator<NodeIDType>(
					nodeConfig, messenger, startCleanSlate);
			pd.register(
					reconfigurator.getPacketTypes().toArray(
							new IntegerPacketType[0]), reconfigurator);
			this.reconfigurators.add(reconfigurator);

			// wrap reconfigurator in active to make it reconfigurable
			ActiveReplica<NodeIDType> activeReplica = reconfigurator
					.getReconfigurableReconfiguratorAsActiveReplica();
			pd.register(activeReplica.getPacketTypes(), activeReplica);
			this.activeReplicas.add(activeReplica);
		}
	}

	// because ReconfigurableNode is abstract for backwards compatibility
	/**
	 */
	public static class DefaultReconfigurableNode extends
			ReconfigurableNode<String> {

		/**
		 * @param id
		 * @param nodeConfig
		 * @param args
		 * @param startCleanSlate
		 * @throws IOException
		 */
		public DefaultReconfigurableNode(String id,
				ReconfigurableNodeConfig<String> nodeConfig, String[] args,
				boolean startCleanSlate) throws IOException {
			super(id, nodeConfig, args, startCleanSlate);
		}

		@Override
		protected AbstractReplicaCoordinator<String> createAppCoordinator() {
			return super.createApp(null, super.nodeConfig);
		}

		public String toString() {
			return super.toString();
		}
	}

	public String toString() {
		return "Node" + this.myID;
	}

	private static boolean clear = false;

	// get all nodes to be started via main
	private static Set<String> getAllNodes(String[] args) {
		Set<String> nodeIDs = new HashSet<String>();
		// search for START_ALL; only for backwards compatibility
		if (args[args.length - 1]
				.equals(ReconfigurationConfig.CommandArgs.START_ALL.toString())
		// look for "start all" or "clear all" at the end
				|| (args.length >= 2
						&& args[args.length - 1]
								.equals(ReconfigurationConfig.CommandArgs.all) && (args[args.length - 2]
						.equals(ReconfigurationConfig.CommandArgs.start) || args[args.length - 2]
						.equals(ReconfigurationConfig.CommandArgs.clear)))) {
			nodeIDs.addAll(ReconfigurationConfig.getReconfiguratorIDs());
			nodeIDs.addAll(PaxosConfig.getActives().keySet());
		} else
			for (int i = args.length - 1; i >= 0; i--)
				if ((ReconfigurationConfig.getReconfiguratorIDs().contains(
						args[i]) || PaxosConfig.getActives().keySet()
						.contains(args[i]))
						&& !args[i]
								.equals(ReconfigurationConfig.CommandArgs.start
										.toString())
						&& !args[i]
								.equals(ReconfigurationConfig.CommandArgs.clear
										.toString()))
					nodeIDs.add(args[i]);
				else {
					clear = clear
							|| args[i]
									.equals(ReconfigurationConfig.CommandArgs.clear
											.toString());
					break;
				}
		return nodeIDs;
	}

	// only for backwards compatibility
	private static String getAppArgs(String[] args) {
		String argsAsString = "";
		for (String arg : args)
			argsAsString += " " + arg;
		argsAsString = argsAsString
				.replaceAll(
						ReconfigurationConfig.CommandArgs.START_ALL.toString(),
						"")
				.replaceAll(
						" "
								+ ReconfigurationConfig.CommandArgs.start.toString()
								+ " .*", "").trim();
		return argsAsString;
	}

	/**
	 * {@code args} contains a list of app arguments followed by a list of
	 * active or reconfigurator node IDs at the end. The string "start all" is
	 * accepted as a proxy for the list of all nodes if the socket addresses of
	 * all nodes are on the local machine.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// Util.assertAssertionsEnabled();
		Config.register(args);
		ReconfigurationConfig.setConsoleHandler();

		if (args.length == 0)
			throw new RuntimeException(
					"At least one node ID must be specified as a command-line argument for starting "
							+ ReconfigurableNode.class);
		ReconfigurableNodeConfig<String> nodeConfig = new DefaultNodeConfig<String>(
				PaxosConfig.getActives(),
				ReconfigurationConfig.getReconfigurators());
		PaxosConfig.sanityCheck(nodeConfig);
		Set<String> servers = getAllNodes(args);

		if (clear) {
			for (String id : servers)
				try {
					SQLReconfiguratorDB.dropState(
							id,
							new ConsistentReconfigurableNodeConfig<String>(
									new DefaultNodeConfig<String>(PaxosConfig
											.getActives(),
											ReconfigurationConfig
													.getReconfigurators())));
				} catch (Exception e) {
					/* ignore all exceptions as they correspond to non-local
					 * nodes */
					e.printStackTrace();
				}
			return;
		}

		String sysPropAppArgsAsString = System
				.getProperty(ReconfigurationConfig.CommandArgs.appArgs
						.toString());
		// append cmdline args to system property based args
		String cmdlineAppArgs = getAppArgs(args);
		String[] appArgs = ((sysPropAppArgsAsString != null ? sysPropAppArgsAsString
				: "")
				+ " " + cmdlineAppArgs).split("\\s");
		int numServers = servers.size();
		if (numServers == 0)
			throw new RuntimeException("No valid server names supplied");
		System.out.print("Initializing gigapaxos server"
				+ (numServers > 1 ? "s" : "") + " [ ");
		for (String node : servers) {
			System.out.print(node + ":" + nodeConfig.getNodeAddress(node) + ":"
					+ nodeConfig.getNodePort(node) + " ");
			new DefaultReconfigurableNode(node,
			// must use a different nodeConfig for each
					new DefaultNodeConfig<String>(PaxosConfig.getActives(),
							ReconfigurationConfig.getReconfigurators()),
					appArgs, false);
		}
		System.out.println("]; server" + (numServers > 1 ? "s" : "") + servers
				+ " ready");
	}
}
