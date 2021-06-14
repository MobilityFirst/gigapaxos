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

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentHashing;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *         Reconfiguration configuration parameters. These parameters are
 *         expected to be statically set at JVM initiation time and should not
 *         be changed subsequently. A static code block can be used to read
 *         parameters from a file and set them using methods here.
 *         Reconfiguration parameters that can be dynamically changed after
 *         initiation are generally supported using get/set methods in their
 *         respective classes.
 */
public class ReconfigurationConfig {
    static final Logger log = Logger.getLogger(ReconfigurationConfig.class
            .getName());

	/**
	 * 
	 */
	public static void load() {
		/* Both gigapaxos and reconfiguration take parameters from the same
		 * properties file (default "gigapaxos.properties"). */
		PaxosConfig.load();
		PaxosConfig.load(ReconfigurationConfig.RC.class);
	}

	static {
		load();
	}

	/**
	 * The default demand profile type is DemandProfile.class. This will
	 * reconfigure once per request, so you probably want to use something else.
	 */
	private static Class<?> demandProfileType = getDemandProfile(); // DEFAULT_DEMAND_PROFILE_TYPE;

	private static Class<?> getClassSuppressExceptions(String className) {
		Class<?> clazz = null;
		try {
			if (className != null && !"null".equals(className)) {
				clazz = Class.forName(className);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return clazz;
	}

	/**
	 * 
	 */
	public static final Class<?> application = getClassSuppressExceptions(Config
			.getGlobalString(RC.APPLICATION));

	/**
	 * @return Initial state of default service name that is replicated at all
	 *         active replicas.
	 */
	public static final String getDefaultServiceNameInitialState() {
		try {
			return new JSONObject().put(PaxosConfig.getDefaultServiceName(),
					Config.getGlobalString(PC.DEFAULT_NAME_INITIAL_STATE))
					.toString();
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}

    /**
     * @return Logger used by all of the reconfiguration package.
     */
    public static final Logger getLogger() {
        return log;
    }

	private static final boolean IS_AGGREGATED_MERGE_SPLIT = true;
    
	/**
	 * Default true now for an improved merge/split implementation. Doing merges
	 * in the old way potentially violates RSM safety. True effectively disables
	 * the use of {@link RCRecordRequest.RequestTypes#RECONFIGURATION_MERGE}.
	 */
	protected static final boolean isAggregatedMergeSplit() {
		return IS_AGGREGATED_MERGE_SPLIT;
	}

	/**
	 * Reconfiguration config parameters.
	 */
	public static enum RC implements Config.ConfigurableEnum {
		/**
		 * 
		 */
		APPLICATION(
				"edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp"),
		/**
		 * Demand profile class name. iOS client requires that this name be specified as a string
		 * as opposed to using class.getName()
		 */
		DEMAND_PROFILE_TYPE("edu.umass.cs.reconfiguration.reconfigurationutils.DemandProfile"),

		/**
		 * Directory where reconfiguration DB is maintained when an embedded DB
		 * is used. Can not be changed via properties file.
		 */
		RECONFIGURATION_DB_DIR("reconfiguration_DB"),

		/**
		 * Prefix of the reconfiguration DB's name. The whole name is obtained
		 * as this prefix concatenated with the node ID.
		 */
		RECONFIGURATION_DB_PREFIX("reconfiguration_DB"),

		/**
		 * {@link edu.umass.cs.gigapaxos.paxosutil.SQL.SQLType} type. Currently,
		 * the only other alternative is "MYSQL". Note that this enum has the
		 * same name as {@link edu.umass.cs.gigapaxos.PaxosConfig.PC#SQL_TYPE},
		 * so the two are currently forced to use the same DB type.
		 */
		SQL_TYPE("EMBEDDED_DERBY"),

		/**
		 * Whether reconfigurations should be performed even though
		 * AbstractDemandProfile returned a set of active replicas that are
		 * identical to the current one. Useful for testing, but should be false
		 * in production.
		 */
		RECONFIGURE_IN_PLACE(false),
		/**
		 * Default TLS authentication mode for client-server communication.
		 * Here, "client" means an end-client, not the (more general) initiator
		 * of communication. An end-client generally also is the initiator of
		 * communication, but the converse is not true. We generally want this
		 * to either be CLEAR or SERVER_AUTH, not MUTUAL_AUTH, as it is not
		 * generally meaningful for a server to authenticate an end-client for
		 * TLS purposes (as opposed to application-level authentication).
		 */
		CLIENT_SSL_MODE(SSLDataProcessingWorker.SSL_MODES.CLEAR),
		/**
		 * Default TLS authentication mode for server-server communication. We
		 * generally want this to be MUTUAL_AUTH as both parties need to
		 * authenticate each other.
		 */
		SERVER_SSL_MODE(SSLDataProcessingWorker.SSL_MODES.CLEAR),
		/**
		 * The default offset added to the active replica or reconfigurator port
		 * number in order to get the client-facing port for client-facing
		 * request types (CREATE_SERVICE_NAME, DELETE_SERVICE_NAME,
		 * REQUEST_ACTIVE_REPLICAS at reconfigurators) and all app request types
		 * at active replicas. In general, we need the port for client-facing
		 * requests to be different because the TLS authentication mode for
		 * client-server and server-server communication may be different.
		 */
		CLIENT_PORT_OFFSET(100),

		/**
		 * 
		 */
		CLIENT_PORT_SSL_OFFSET(200),

		/**
		 * True if deletes are completed based on probing all actives.
		 * 
		 * Assumption for safety: Safe only if the set of actives does not
		 * change or is consistent across reconfigurators (not true by default).
		 * This option should only be false in production runs.
		 */
		AGGRESSIVE_DELETIONS(true),
		/**
		 * True if further reconfigurations can progress without waiting for the
		 * previous epoch final state to be dropped cleanly.
		 */
		AGGRESSIVE_RECONFIGURATIONS(true),

		/**
		 * Default retransmission timeout for coordinated requests in the
		 * reconfiguration protocol.
		 */
		COMMIT_WORKER_RESTART_PERIOD(2000),

		/**
		 * Default restart period for the stop epoch task. All other restart
		 * periods are multiples of this time.
		 */
		STOP_TASK_RESTART_PERIOD(2000),

		/**
		 * Maximum string length of a demand profile message.
		 */
		MAX_DEMAND_PROFILE_SIZE(4096),

		/**
		 * Whether most recent demand report should be combined with historic
		 * demand stats.
		 */
		COMBINE_DEMAND_STATS(false),

		/**
		 * If true, reconfiguration consists of committing an intent and then a
		 * complete both via paxos. If false, reconfiguration for non-RC-group
		 * names can proceed with just a single paxos round to commit an intent
		 * while using a simple broadcast for the complete. Avoiding the second
		 * paxos round is more efficient but has the downside that if the
		 * complete message for a name gets lost, some replicas may not be able
		 * to initiate further reconfigurations for the name. Using paxos does
		 * not guarantee liveness either, but its in-built mechanisms allowing
		 * laggard replicas to catch up combined with the CommitWorker mechanism
		 * to try to commit the complete until successful ensures that (1) the
		 * complete does indeed get eventually committed, and (2) all replicas
		 * apply *all* state changes in the same order. The latter property may
		 * not hold if TWO_PAXOS_RC is false but is not necessary for safety
		 * anyway.
		 * 
		 * We don't allow RC group name or NODE_CONFIG changes to proceed with a
		 * single paxos round because reconfigurations can get stuck if a
		 * complete arrives a replica before the creation of the new paxos
		 * group. The inefficiency of two paxos rounds hardly matters given the
		 * high inherent overhead of RC group reconfigurations.
		 */
		TWO_PAXOS_RC(true),

		/**
		 * 
		 */
		USE_DISK_MAP_RCDB(true),

		/**
		 * This parameter specifies the the number of active replicas for a name
		 * at upon creation of the name. This parameter is irrelevant if
		 * {@link #REPLICATE_ALL} is true and is irrelevant after name creation
		 * as the number of replicas thereafter is controlled by the
		 * reconfiguration policy.
		 */
		DEFAULT_NUM_REPLICAS(3),

		/**
		 * True means that a name upon creation will be replicated at all
		 * current active replicas; else it will be replicated at at most
		 * {@link #DEFAULT_NUM_REPLICAS} randomly chosen active replicas. Note
		 * that this parameter is irrelevant after name creation as the number
		 * of replicas thereafter is controlled by the reconfiguration policy.
		 */
		REPLICATE_ALL(true),
		/**
		 * 
		 */
		MAX_BATCH_SIZE(10000),

		/**
		 * Requesting actives returns a random active replica for this name.
		 */
		SPECIAL_NAME("*"),

		/**
		 * True if delay profiling is enabled at various places.
		 */
		ENABLE_INSTRUMENTATION(false),

		/**
		 * 
		 */
		STAMP_SENDER_ADDRESS_JSON(false),

		/**
		 * 
		 */
		BROADCAST_NAME("**"),

		/**
		 * If true, a client actively probes active replicas in order to
		 * determine which active replicas are closest to it.
		 */
		ORIENT_CLIENT(true),

		/**
		 * The maximum number of actives that will be sent to a client when it
		 * bootstraps to probe them in order to orient itself
		 */
		ORIENT_LIMIT(8),

		/**
		 * The closest K servers to a given client IP address.
		 */
		CLOSEST_K(3),

		/**
		 * 
		 */
		SEND_CLOSEST_TO_RECONFIGURATORS(true),

		/**
		 * A flag to stop even all reporting.
		 */
		DISABLE_RECONFIGURATION(false),
		
		/**
		 * True means clients can create or delete names. False means
		 * only active replicas can create or delete names provided
		 * MUTUAL_AUTH is used between servers.
		 */
		ALLOW_CLIENT_TO_CREATE_DELETE (true),

		/**
		 * True if app request types are allowed on the server port. False means
		 * that only app coordination request types will be allowed.
		 */
		ALLOW_APP_TYPES_ON_SERVER_PORT (true),

		/**
		 * True means that request IDs will be automatically transformed by
		 * {@link ReconfigurableAppClientAsync} if it receives two unequal 
		 * requests with identical IDs.
		 */
		ENABLE_ID_TRANSFORM (false),
		
		/**
		 * True means that the demand profile implementation is tested at bootstrap
		 * time to sanity check that its implementation meets the specification.
		 */
		TEST_DEMAND_PROFILE (true), 
		
		/**
		 * HTTP server port offset relative to reconfigurator port.
		 */
		HTTP_PORT_OFFSET(300), 
		
		/**
		 * HTTP server port offset relative to reconfigurator port.
		 */
		HTTP_PORT_SSL_OFFSET(400),
		
		/**
		 * Enable DnsReconfigurator, which needs admin privilege to bind to port 53
		 */
		ENABLE_RECONFIGURATOR_DNS (false),
		
		/**
		 * The default ttl value used by DnsReconfigurator
		 */
		DEFAULT_DNS_TTL(30),
		
		/**
		 * The default traffic policy class used by DnsReconfigurator
		 */
		DEFAULT_DNS_TRAFFIC_POLICY_CLASS("edu.umass.cs.reconfiguration.dns.NoopDnsTrafficPolicy"),
		
		/**
		 * Enable the HTTP server for reconfigurators.
		 */
		ENABLE_RECONFIGURATOR_HTTP (true),
		
		/**
		 * Enable the HTTP server for active replicas
		 */
		ENABLE_ACTIVE_REPLICA_HTTP(false),
		
		/**
		 * HTTP active replica name
		 */
		HTTP_ACTIVE_REPLICA_NAME("edu.umass.cs.reconfiguration.http.HttpActiveReplica"),
		
		/**
		 * If true, transactions are enabled; else disabled.
		 */
		ENABLE_TRANSACTIONS (false),
		
		/**
		 * Enable {@HelloRequest} for an active running behind NAT
		 * to be able to communicate to the other replicas
		 */
		ENABLE_NAT (false),
		
		/**
		 * The name of the class used to wrap the application's default
		 * coordinator.
		 */
		COORDINATOR_WRAPPER("edu.umass.cs.txn.DistTransactor"),
		
		/**
		 * 
		 */
		TX_GROUP_NAME("_TXGROUP_"),

		/**
		 * Used to set @link {@link edu.umass.cs.gigapaxos.PaxosManager#setOutOfOrderLimit(int)}.
		 */
		OUT_OF_ORDER_LIMIT(100),

		/**
		 * Default coordinator: {@link edu.umass.cs.reconfiguration.PaxosReplicaCoordinator}
		 */
		REPLICA_COORDINATOR_CLASS("edu.umass.cs.reconfiguration.PaxosReplicaCoordinator"),

		;

		final Object defaultValue;

		RC(Object defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}

		@Override
		public String getDefaultConfigFile() {
			return PaxosConfig.DEFAULT_GIGAPAXOS_CONFIG_FILE;
		}

		@Override
		public String getConfigFileKey() {
			return PaxosConfig.GIGAPAXOS_CONFIG_FILE_KEY;
		}
	}

	private static boolean reconfigureInPlace = Config
			.getGlobalBoolean(RC.RECONFIGURE_IN_PLACE);

	private static SSLDataProcessingWorker.SSL_MODES clientSSLMode = SSLDataProcessingWorker.SSL_MODES
			.valueOf(Config.getGlobal(RC.CLIENT_SSL_MODE).toString());

	private static SSLDataProcessingWorker.SSL_MODES serverSSLMode = SSLDataProcessingWorker.SSL_MODES
			.valueOf(Config.getGlobal(RC.SERVER_SSL_MODE).toString());

	private static int clientPortClearOffset = Config
			.getGlobalInt(RC.CLIENT_PORT_OFFSET);

	private static int clientPortSSLOffset = Config
			.getGlobalInt(RC.CLIENT_PORT_SSL_OFFSET);

	private static int httpPortClearOffset = Config
			.getGlobalInt(RC.HTTP_PORT_OFFSET);

	private static int httpPortSSLOffset = Config
			.getGlobalInt(RC.HTTP_PORT_SSL_OFFSET);

	private static boolean aggressiveDeletions = Config
			.getGlobalBoolean(RC.AGGRESSIVE_DELETIONS);

	private static boolean aggressiveReconfigurations = Config
			.getGlobalBoolean(RC.AGGRESSIVE_RECONFIGURATIONS);

	/**
	 * Necessary to ensure safety under name re-creations (see
	 * {@link #getDelayedDeleteWaitDuration()} below). We also use this timeout
	 * for garbage collecting remote checkpoints transferred using the file
	 * system.
	 * 
	 * @return The value of MAX_FINAL_STATE_AGE used by paxos. This is the time
	 *         after which paxos' epoch final state can be safely deleted.
	 */
	public static final long getMaxFinalStateAge() {
		return Config.getGlobalInt(PC.MAX_FINAL_STATE_AGE);
	}

	/**
	 * The time for which we must wait before finally deleting a name's
	 * reconfiguration record (i.e., all memory of that name is lost) must be at
	 * least as high as paxos' MAX_FINAL_STATE_AGE, otherwise it can cause the
	 * creation of a name or addition of a reconfigurator to stall for
	 * arbitrarily long, or worse, violate safety by using incorrect state from
	 * previous incarnations.
	 * 
	 * @return The time for which deleted records should be left waiting so that
	 *         they don't get recreated subsequently with un-garbage-collected
	 *         copies of epoch final state from previous incarnations.
	 */
	public static final long getDelayedDeleteWaitDuration() {
		return getMaxFinalStateAge();
	}

	/**
	 * @param newDP
	 * @return Old DemandProfile class.
	 */
	public static Class<?> setDemandProfile(Class<?> newDP) {
		Class<?> oldDP = demandProfileType;
		demandProfileType = newDP;
		return oldDP;
	}

	/**
	 * @return DemandProfile class.
	 */
	public static Class<?> getDemandProfile() {
		if (demandProfileType == null) {
			demandProfileType = getClassSuppressExceptions(Config
					.getGlobalString(RC.DEMAND_PROFILE_TYPE));
		}
		return demandProfileType;
	}

	/**
	 * @return True means in-place reconfigurations will still be conducted.
	 */
	public static boolean shouldReconfigureInPlace() {
		return reconfigureInPlace;
	}
	
	/**
	 * @return Default ReconfigureUponActivesChange policy.
	 */
	public static ReconfigureUponActivesChange getDefaultReconfigureUponActivesChangePolicy() {
		return 
				Config.getGlobalBoolean(RC.REPLICATE_ALL) ? ReconfigureUponActivesChange.REPLICATE_ALL:
					ReconfigureUponActivesChange.DEFAULT;
	}

	/**
	 * @param sslMode
	 */
	@Deprecated
	public static void setClientSSLMode(
			SSLDataProcessingWorker.SSL_MODES sslMode) {
		clientSSLMode = sslMode;
	}

	/**
	 * @return The default SSL mode for client-server communication.
	 */
	public static SSLDataProcessingWorker.SSL_MODES getClientSSLMode() {
		return clientSSLMode;
	}

	/**
	 * @param sslMode
	 */
	@Deprecated
	public static void setServerSSLMode(
			SSLDataProcessingWorker.SSL_MODES sslMode) {
		serverSSLMode = sslMode;
	}

	/**
	 * @return The default SSL mode for server-server communication.
	 */
	public static SSLDataProcessingWorker.SSL_MODES getServerSSLMode() {
		return serverSSLMode;
	}

	/**
	 * @return True if mutual authentication between servers is enabled.
	 */
	public static boolean isTLSEnabled() {
		return getServerSSLMode().equals(
				SSLDataProcessingWorker.SSL_MODES.MUTUAL_AUTH);
	}

	/**
	 * @return The client port offset, i.e., the port number that is to be added
	 *         to the standard port in order to get the client-facing port. A
	 *         nonzero offset is needed to support transport layer security
	 *         between servers.
	 */
	public static int getClientPortOffset() {
		return getClientSSLMode() == SSL_MODES.CLEAR ? Config
				.getGlobalInt(RC.CLIENT_PORT_OFFSET) : Config
				.getGlobalInt(RC.CLIENT_PORT_SSL_OFFSET);
	}

	/**
	 * @param port
	 * @return Translates port to corresponding client facing port.
	 */
	public static int getClientFacingPort(int port) {
		return port + getClientPortOffset();
	}

	/**
	 * @param port
	 * @return Translates port to corresponding client facing port.
	 */
	public static int getClientFacingClearPort(int port) {
		return port + getClientPortClearOffset();
	}

	/**
	 * @param port
	 * @return Translates port to corresponding client facing port.
	 */
	public static int getHTTPPort(int port) {
		return port + getHTTPPortClearOffset();
	}
	/**
	 * @param port
	 * @return Translates port to corresponding client facing port.
	 */
	public static int getHTTPSPort(int port) {
		return port + getHTTPPortSSLOffset();
	}

	/**
	 * @param port
	 * @return Translates port to corresponding client facing port.
	 */
	public static int getClientFacingSSLPort(int port) {
		return port + getClientPortSSLOffset();
	}

	/**
	 * @return The client port SSL offset, i.e., the port number that is to be added
	 *         to the standard port in order to get the client-facing SSL port. A
	 *         nonzero offset is needed to separate client-client communication from 
	 *         server-server communication and to support transport layer security
	 *         between servers.
	 */
	public static int getClientPortSSLOffset() {
		return clientPortSSLOffset;
	}

	/**
	 * @return The client port  offset, i.e., the port number that is to be added
	 *         to the standard port in order to get the client-facing port. A
	 *         nonzero offset is needed to separate client-client communication from 
	 *         server-server communication and to support transport layer security
	 *         between servers.
	 */
	public static int getClientPortClearOffset() {
		return clientPortClearOffset;
	}

	/**
	 * @return The client port HTTTPS offset, i.e., the port number that is to be added
	 *         to the standard port in order to get the client-facing HTTPS port. A
	 *         nonzero offset is needed to separate client-client communication from 
	 *         server-server communication and to support transport layer security
	 *         between servers.
	 */
	public static int getHTTPPortSSLOffset() {
		return httpPortSSLOffset;
	}
	
	/**
	 * @return The client port HTTTP offset, i.e., the port number that is to be added
	 *         to the standard port in order to get the client-facing HTTP port. A
	 *         nonzero offset is needed to separate client-client communication from 
	 *         server-server communication and to support transport layer security
	 *         between servers.
	 */
	public static int getHTTPPortClearOffset() {
		return httpPortClearOffset;
	}


	/**
	 * @return True is aggressive recreations allowed.
	 */
	public static boolean aggressiveDeletionsAllowed() {
		return aggressiveDeletions;
	}

	/**
	 * @return True is aggressive reconfigurations are allowed.
	 */
	public static boolean aggressiveReconfigurationsAllowed() {
		return aggressiveReconfigurations;
	}

	protected static String DEFAULT_RECONFIGURATOR_PREFIX = "reconfigurator.";

	/**
	 * @return A map of names and socket addresses corresponding to servers
	 *         hosting paxos replicas.
	 */
	public static Map<String, InetSocketAddress> getReconfigurators() {
		Map<String, InetSocketAddress> map = new HashMap<String, InetSocketAddress>();
		// Config config = Config.getConfig(PC.class);
		Properties config = PaxosConfig.getAsProperties();

		Set<String> keys = config.stringPropertyNames();
		for (String key : keys) {
			if (key.trim().startsWith(DEFAULT_RECONFIGURATOR_PREFIX)) {
				map.put(key.replaceFirst(DEFAULT_RECONFIGURATOR_PREFIX, ""),
						Util.getInetSocketAddressFromString(config
								.getProperty(key)));
			}
		}
		if (map.isEmpty())
			throw new RuntimeException(
					"Unable to find any reconfigurators "
							+ (!new File(
									PaxosConfig.DEFAULT_GIGAPAXOS_CONFIG_FILE)
									.exists()
									&& (System
											.getProperty(PaxosConfig.GIGAPAXOS_CONFIG_FILE_KEY) == null || !new File(
											System.getProperty(PaxosConfig.GIGAPAXOS_CONFIG_FILE_KEY))
											.exists()) ? "because no gigapaxos properties file was found"
									+ " either at the default location \""
									+ PaxosConfig.DEFAULT_GIGAPAXOS_CONFIG_FILE
									+ "\" (relative to the current directory "
									+ new File(".").getAbsolutePath().replaceFirst(".$", "")
									+ ") or at a custom location specified as -D"
									+ PaxosConfig.GIGAPAXOS_CONFIG_FILE_KEY
									+ "="
									: ""));
		return map;
	}

	/**
	 * @return Returns only reconfigurator addresses.
	 */
	public static Set<InetSocketAddress> getReconfiguratorAddresses() {
		return new HashSet<InetSocketAddress>(getReconfigurators().values());
	}

	/**
	 * @return Gets only reconfigurator String IDs.
	 */
	public static Set<String> getReconfiguratorIDs() {
		return new HashSet<String>(getReconfigurators().keySet());
	}

	/**
	 * 
	 * @param args
	 * @return Replicable app created via reflection.
	 */
	protected static Replicable createApp(String[] args) {
		if (ReconfigurationConfig.application != null) {
			try {
				return (Replicable) ReconfigurationConfig.application
						.getConstructor(String[].class).newInstance(
								new Object[] { args });
			} catch (InstantiationException | IllegalAccessException
					| IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e1) {
				getLogger().info(
						ReconfigurationConfig.application
								+ " does not support (String[]) constructor;"
								+ " trying default constructor instead");
				// if exception, try default constructor
				try {
					return (Replicable) ReconfigurationConfig.application
							.getConstructor().newInstance();
				} catch (InstantiationException | IllegalAccessException
						| IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException e2) {
					getLogger()
							.severe("App "
									+ ReconfigurationConfig.application
											.getSimpleName()
									+ " must support a constructor with a single String[] argument"
									+ " or the default constructor (with no arguments).");
					System.exit(1);
				}
			}
		}
		return null;
	}

	/**
	 * @param nameStates
	 * @param batchSize
	 * @return Array of CreateServiceName objects each of which is a batch
	 *         create of up to batchSize names and corresponds to the same RC
	 *         group.
	 */
	public static CreateServiceName[] makeCreateNameRequest(
			Map<String, String> nameStates, int batchSize) {
		return makeCreateNameRequest(nameStates, batchSize,
				ReconfigurationConfig.getReconfiguratorIDs());
	}

	/**
	 * @param nameStates
	 * @param batchSize
	 * @param reconfigurators
	 * @return Array of CreateServiceName objects each of which is a batch
	 *         create of up to batchSize names and corresponds to the same RC
	 *         group.
	 */
	public static CreateServiceName[] makeCreateNameRequest(
			Map<String, String> nameStates, int batchSize,
			Set<String> reconfigurators) {
		// each set in batches below corresponds to a different RC group
		Collection<Set<String>> batches = ConsistentReconfigurableNodeConfig
				.splitIntoRCGroups(nameStates.keySet(), reconfigurators);

		Set<CreateServiceName> creates = new HashSet<CreateServiceName>();
		// each nameStatesCur batch is limited to batchSize
		for (Set<String> batch : batches) {
			Map<String, String> nameStatesCur = new HashMap<String, String>();
			for (Iterator<String> nameIter = batch.iterator(); nameIter
					.hasNext();) {
				String name = nameIter.next();
				nameStatesCur.put(name, nameStates.get(name));
				// reached batchSize or last element of set
				if (nameStatesCur.size() == batchSize || !nameIter.hasNext()) {
					// make a single batched create
					creates.add(new CreateServiceName(nameStatesCur));
					nameStatesCur = new HashMap<String, String>();
				}
			}
		}
		return creates.toArray(new CreateServiceName[0]);
	}

	/**
	 * Command-line options
	 */
	public static enum CommandArgs {
		/**
		 * 
		 */
		START_ALL,

		/**
		 * 
		 */
		start,

		/**
		 * 
		 */
		all,

		/**
		 * 
		 */
		clear,

		/**
		 * 
		 */
		appArgs,
	};

	/**
	 * @param level
	 * 
	 */
	public static void setConsoleHandler(Level level) {
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(level);
		getLogger().setLevel(level);
		getLogger().addHandler(handler);
		getLogger().setUseParentHandlers(false);

		PaxosConfig.getLogger().setLevel(level);
		PaxosConfig.getLogger().addHandler(handler);
		PaxosConfig.getLogger().setUseParentHandlers(false);

		NIOTransport.getLogger().setLevel(level);
		NIOTransport.getLogger().addHandler(handler);
		NIOTransport.getLogger().setUseParentHandlers(false);
	}

	/**
	 * 
	 */
	public static void setConsoleHandler() {
		if (System.getProperty("java.util.logging.config.file") == null)
			setConsoleHandler(Level.INFO);
	}

	protected static CreateServiceName[] testMakeCreateNameRequest(String name,
			String state, int numRequests, int batchSize) {
		Util.assertAssertionsEnabled();
		Map<String, String> nameStates = new HashMap<String, String>();
		for (int i = 0; i < numRequests; i++)
			nameStates.put(name + i, state);
		CreateServiceName[] creates = makeCreateNameRequest(nameStates,
				batchSize);
		ConsistentHashing<String> ch = new ConsistentHashing<String>(
				ReconfigurationConfig.getReconfiguratorIDs());
		String name0 = null;
		for (CreateServiceName create : creates)
			for (String curName : create.nameStates.keySet())
				assert (ch.getReplicatedServers(curName).equals(ch
						.getReplicatedServers(name0 == null ? name0 = curName
								: name0)));
		return creates;
	}

	/**
	 * @param request
	 * @param stringify
	 * @return Stringifiable object if stringify
	 */
	public static Object getSummary(ClientRequest request, boolean stringify) {
		if (stringify)
			return getSummary(request);
		return null;
	}

	private static final boolean ENABLE_INSTRUMENTATION = Config
			.getGlobalBoolean(RC.ENABLE_INSTRUMENTATION);

	/**
	 * @param n
	 * @return Used internally by reconfiguration classes.
	 */
	public static boolean instrument(int n) {
		return ENABLE_INSTRUMENTATION && Util.oneIn(n);
	}

	/**
	 * @param request
	 * @return Stringifiable object.
	 */
	public static Object getSummary(ClientRequest request) {
		return request.getSummary();
	}

	/* Methods to use gpServer.sh */
	private static enum DefaultProps {
		GIGAPAXOS_CONFIG("gigapaxosConfig"),

		KEYSTORE("javax.net.ssl.keyStore"),

		KEYSTORE_PASSWORD("javax.net.ssl.keyStorePassword"),

		TRUSTSTORE("javax.net.ssl.trustStore"),

		TRUSTSTORE_PASSWORD("javax.net.ssl.trustStorePassword"),

		LOGGING_PROPERTIES("java.util.logging.config.file"),

		;

		final String key;

		DefaultProps(String key) {
			this.key = key;
		}

		static String getProperties() {
			String s = "", val = null;
			for (DefaultProps prop : DefaultProps.values())
				s += ((val = System.getProperty(prop.key)) != null ? " -D"
						+ prop.key + "=" + val : "");
			return s;
		}
	}


	private static String getFullCommand(String gpServerScriptFile,
			String gigapaxosPropertiesFile, String otherSystemProperties,
			String command) {
		return gpServerScriptFile
				+ (gigapaxosPropertiesFile != null ? " -DgigapaxosConfig="
						+ gigapaxosPropertiesFile
						: (System
								.getProperty(DefaultProps.GIGAPAXOS_CONFIG.key)) != null ? " -DgigapaxosConfig="
								+ System.getProperty(DefaultProps.GIGAPAXOS_CONFIG.key)
								: "") + " " + DefaultProps.getProperties()
				+ " "
				+ (otherSystemProperties != null ? otherSystemProperties : "")
				+ " " + command;
	}

	private static String homePath(String path) {
		return System.getProperty("user.home") + "/" + path;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(getFullCommand(
				homePath("gigapaxos/bin/gpServer.sh"),
				homePath("gigapaxos/conf/gigapaxos.properties"), null,
				"start all"));
		System.out.println(ReconfigurationConfig.getReconfiguratorAddresses());
	}

	/**
	 * This enum specifies the reconfiguration behavior when active replicas are
	 * added or removed.
	 */
	public static enum ReconfigureUponActivesChange {
		/**
		 * Do nothing when the set of active replicas changes.
		 */
		DEFAULT(0),

		/**
		 * Reconfigure to the current set of all active replicas.
		 */
		REPLICATE_ALL(1),

		/**
		 * Invoke app policy that specifies whether/how to reconfigure.
		 */
		CUSTOM(2);

		final int number;

		ReconfigureUponActivesChange(int n) {
			this.number = n;
		}
	}
}
