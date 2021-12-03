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
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.ReplicaCoordinator;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.AbstractPaxosLogger;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPolicyTest;
import edu.umass.cs.utils.Config;

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
		ReconfigurationConfig.getLogger().info(
				"----------" + this + " closed----->||||");
	}
	
	/**
	 * For testing. Only a server that has been started from within this JVM can be
	 * closed and force-cleared using this method.
	 * 
	 * @param server 
	 */
	public static void forceClear(String server) {
		if(allInstances.containsKey(server)) allInstances.get(server).close();
		SQLReconfiguratorDB.dropState(server,
				new ConsistentReconfigurableNodeConfig<String>(
						new DefaultNodeConfig<String>(PaxosConfig.getActives(),
								ReconfigurationConfig.getReconfigurators())));

	}
	
	/**
	 * @param server
	 */
	public static void stopServer(String server) {
		if(allInstances.containsKey(server)) allInstances.get(server).close();
	}

	private AbstractReplicaCoordinator<NodeIDType> createApp(String[] args,
	// private ReplicaCoordinator<NodeIDType> createApp(String[] args,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig) {
		AbstractReplicaCoordinator<NodeIDType> appCoordinator = null;
		// ReplicaCoordinator<NodeIDType> appCoordinator = null;

		if (ReconfigurationConfig.application == null)
			throw new RuntimeException("Application name can not be null");
		// else
		{
			Replicable app = ReconfigurationConfig.createApp(args);
			if (app instanceof ClientMessenger)
				((ClientMessenger) app).setClientMessenger(messenger);
			else
				ReconfigurationConfig
						.getLogger()
						.info(app.getClass().getSimpleName()
								+ " does not implement "
								+ ClientMessenger.class.getSimpleName()
								+ ", which means the app should either rely on "
								+ ClientRequest.class.getSimpleName()
								+ " or not expect to send "
								+ " responses back to clients or rely on alternate means for messaging.");
			appCoordinator = this.getAppCoordinator(
					app, myID, nodeConfig, messenger);

			// default service name created at all actives
			ReconfigurationConfig
					.getLogger()
					.log(Level.INFO,
							"{0} creating default service name {1} with replica group {2}",
							new Object[] {
									this,
									PaxosConfig
											.getDefaultServiceName(),
									nodeConfig.getActiveReplicas() });
			appCoordinator.createReplicaGroup(PaxosConfig
							.getDefaultServiceName(), 0,
					Config.getGlobalString(PaxosConfig.PC
							.DEFAULT_NAME_INITIAL_STATE),
					nodeConfig.getActiveReplicas());

			// special record at actives containing a map of all current actives
			ReconfigurationConfig.getLogger().log(
					Level.INFO,
					"{0} creating {1} with replica group {2}",
					new Object[] { this,
							AbstractReconfiguratorDB.RecordNames.AR_AR_NODES,
							nodeConfig.getActiveReplicas() });
			appCoordinator.createReplicaGroup(
					AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString(), 0,
					// state is just the stringified map of active replicas
					nodeConfig.getActiveReplicasReadOnly().toString(), nodeConfig.getActiveReplicas());

			// special record at actives containing a map of all current reconfigurators
			ReconfigurationConfig.getLogger().log(
					Level.INFO,
					"{0} creating {1} with replica group {2}",
					new Object[] { this,
							AbstractReconfiguratorDB.RecordNames.AR_RC_NODES,
							nodeConfig.getActiveReplicas() });
			appCoordinator.createReplicaGroup(
					AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString(),
					0,
					// state is just the stringified map of active replicas
					nodeConfig.getReconfiguratorsReadOnly().toString(),
					nodeConfig.getActiveReplicas());

			return appCoordinator;
		} 
	}

	/**
	 * 
	 * TODO: Extend this method to enable support for other coordinators.
	 * 
	 * @param app
	 * @param myID
	 * @param nodeConfig
	 * @param messenger
	 * @return AbstractReplicaCoordinator: currently paxos by default
	 */
	private AbstractReplicaCoordinator<NodeIDType> getAppCoordinator(
			Replicable app, NodeIDType myID,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig,
			JSONMessenger<NodeIDType> messenger) {
		String coordinatorClassName = Config.getGlobalString(
				ReconfigurationConfig.RC.REPLICA_COORDINATOR_CLASS);
		if (coordinatorClassName.equals("edu.umass.cs.reconfiguration.PaxosReplicaCoordinator"))
			return new PaxosReplicaCoordinator<NodeIDType>(app, myID, nodeConfig,
					messenger).setOutOfOrderLimit(Config
					.getGlobalInt(ReconfigurationConfig.RC.OUT_OF_ORDER_LIMIT));
		else if (coordinatorClassName.equals("edu.umass.cs.reconfiguration.ChainAndPaxosReplicaCoordinator"))
			return null;
		else if (coordinatorClassName.equals("edu.umass.cs.reconfiguration.ChainReplicaCoordinator"))
			return new ChainReplicaCoordinator<NodeIDType>(app, myID, nodeConfig,
					messenger);

		return null;

		// FIXME: instantiating {@link ReplicaCoordinator} with Generic type
		// 		  parameters is  not possible as the Generic type information
		// 		  is erased in the JVM runtime even though the code compiles.

		/*
		Class<?> c;
		ReplicaCoordinator<?> coordinator = null;
		try {
			c = Class.forName(coordinatorClassName);
			coordinator = (ReplicaCoordinator<?>) c.getDeclaredConstructor(
					Replicable.class, Object.class, Stringifiable.class, JSONMessenger.class)
					.newInstance(app, myID, nodeConfig, messenger);
		} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
			e.printStackTrace();
		}
		// typecast ReplicaCoordinator to AbstractReplicaCoordinator
		return (AbstractReplicaCoordinator) coordinator;
		*/
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

		ReconfigurationPacketDemultiplexer pd;
		
		if(Config.getGlobalBoolean(PC.ENABLE_HANDLE_MESSAGE_REPORT))
			NIOInstrumenter.monitorHandleMessage();

		String err = null;
		if (!nodeConfig.getActiveReplicas().contains(id)
				&& !nodeConfig.getReconfigurators().contains(id)) {
			ReconfigurationConfig.getLogger().severe(
					err =  id
							+ " not present in NodeConfig argument \n  "
							+ nodeConfig.getActiveReplicas() + "\n  "
							+ nodeConfig.getReconfigurators());
			throw new IOException(err);
		}
		// else
		ReconfigurationConfig.getLogger().info(
				this + ":" + this.myID + " listening on "
						+ nodeConfig.getNodeAddress(myID) + ":"
						+ nodeConfig.getNodePort(myID));
		MessageNIOTransport<NodeIDType, JSONObject> niot = null;
		InetSocketAddress isa = new InetSocketAddress(
				nodeConfig.getNodeAddress(myID), nodeConfig.getNodePort(myID));
		// else we have something to start
		messenger = (new JSONMessenger<NodeIDType>(
				(niot = new MessageNIOTransport<NodeIDType, JSONObject>(
						ReconfigurableNode.this.myID,
						nodeConfig,
						(pd = new ReconfigurationPacketDemultiplexer(nodeConfig)
								.setThreadName(ReconfigurableNode.this.myID
										.toString())), true,
						ReconfigurationConfig.getServerSSLMode()))));
		if (!niot.getListeningSocketAddress().equals(isa)
				&& Config.getGlobalBoolean(PC.STRICT_ADDRESS_CHECKS)) {
			ReconfigurationConfig
					.getLogger()
					.severe(err = this
							+ " unable to start ReconfigurableNode at socket address "
							+ isa);
			throw new IOException(err);
		}
		// else created messenger, may still fail to create client messenger

		if (nodeConfig.getActiveReplicas().contains(id)) {
			AbstractReplicaCoordinator<NodeIDType> app=null;
			// create active
			ActiveReplica<NodeIDType> activeReplica = new ActiveReplica<NodeIDType>(
			// createAppCoordinator(),
					app=createApp(args, nodeConfig), nodeConfig, messenger);
			this.activeReplicas.add(activeReplica);
			// getPacketTypes includes app's packets
			pd.setAppRequestParser(app).register(activeReplica.getPacketTypes(), activeReplica);
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
			pd.register(activeReplica.getPacketTypes(true), activeReplica);
			this.activeReplicas.add(activeReplica);
		}
		
		allInstances.putIfAbsent(this.myID.toString(), this);
	}
	
	private static ConcurrentMap<String,ReconfigurableNode<?>> allInstances = new ConcurrentHashMap<String,ReconfigurableNode<?>>();

	// because ReconfigurableNode is abstract for backwards compatibility
	/**
	 */
	public static final class DefaultReconfigurableNode extends
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
		return this.myID.toString();
	}
	/**
	 * @return String ID
	 */
	public String getMyID() {
		return this.myID.toString();
	}

	private static boolean clear = false;

	// get all nodes to be started via main
	private static final Set<String> getAllNodes(String[] args) {
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
			// first reconfigurators, then actives
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
	private static final String getAppArgs(String[] args) {
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

	public static void main(String[] args) throws IOException {
		main1(args);
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
	public static Set<ReconfigurableNode> main1(String[] args) throws IOException {
		PaxosConfig.ensureFileHandlerDirExists();
		Config.register(args);
		ReconfigurationConfig.setConsoleHandler();

		Set<ReconfigurableNode> rcNodes = new HashSet<ReconfigurableNode>();


		PaxosConfig.sanityCheck();
		if (args.length == 0)
			throw new RuntimeException(
					"At least one node ID must be specified as a command-line argument for starting "
							+ ReconfigurableNode.class);
		ReconfigurableNodeConfig<String> nodeConfig = new DefaultNodeConfig<String>(
				PaxosConfig.getActives(),
				ReconfigurationConfig.getReconfigurators());
		PaxosConfig.sanityCheck(nodeConfig);

		if (Config.getGlobalBoolean(PC.EMULATE_DELAYS))
			AbstractPacketDemultiplexer.emulateDelays();

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
			return null;
		}

		ReconfigurationPolicyTest
				.testPolicyImplementation(ReconfigurationConfig
						.getDemandProfile());

		String sysPropAppArgsAsString = System
				.getProperty(ReconfigurationConfig.CommandArgs.appArgs
						.toString());
		// append cmdline args to system property based args
		String cmdlineAppArgs = getAppArgs(args);
		String[] appArgs = ((sysPropAppArgsAsString != null ? sysPropAppArgsAsString
				: "")
				+ " " + cmdlineAppArgs).trim().split("(\\s)+");
		int numServers = servers.size();
		if (numServers == 0)
			throw new RuntimeException("No valid server names supplied");
		System.out.print("Initializing gigapaxos server"
				+ (numServers > 1 ? "s" : "") + " [ ");
		String serversStr="";
		for (String node : servers) {
			String serverStr = node + ":" + nodeConfig.getNodeAddress(node) + ":"
					+ nodeConfig.getNodePort(node) + " ";
			serversStr += serverStr;
			System.out.print(serverStr);
			rcNodes.add(
			new DefaultReconfigurableNode(node,
			// must use a different nodeConfig for each
					new DefaultNodeConfig<String>(PaxosConfig.getActives(),
							ReconfigurationConfig.getReconfigurators()),
					appArgs, false)
			);
		}
		ReconfigurationConfig.getLogger().log(Level.INFO, "{0} server{1} ready (total number of servers={2})",
				new Object[] { serversStr, (numServers > 1 ? "s" : ""), PaxosConfig.getActives().size() +  
				ReconfigurationConfig.getReconfigurators().size()});
		System.out.println("]; server" + (numServers > 1 ? "s" : "") + servers
				+ " ready");

		return rcNodes;
	}
}
