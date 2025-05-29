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
package edu.umass.cs.gigapaxos;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Assert;
import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp;
import edu.umass.cs.gigapaxos.paxosutil.E2ELatencyAwareRedirector;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DiskMap;
import edu.umass.cs.utils.MultiArrayMap;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *         A container class for storing gigapaxos config parameters as an enum.
 */
public class PaxosConfig {
	/**
	 * Default file name for gigapaxos config parameters.
	 */
	public static final String DEFAULT_GIGAPAXOS_CONFIG_FILE = "gigapaxos.properties";
	/**
	 * Gigapaxos config file information can be specified using
	 * -DgigapaxosConfig=<filename> as a JVM argument.
	 */
	public static final String GIGAPAXOS_CONFIG_FILE_KEY = "gigapaxosConfig";

	/**
	 * 
	 */
	public static final String DEFAULT_SERVER_PREFIX = "active.";
    static final Logger log = Logger.getLogger(PaxosConfig.class.getName());

    static {
		load();
	}

	/**
	 * Loads from a default file or file name specified as a system property. We
	 * take a type argument so that ReconfigurationConfig.RC can also mooch off
	 * the same properties file.
	 * 
	 * @param type
	 */
	public static void load(Class<?> type) {
		try {
			Config.register(type, GIGAPAXOS_CONFIG_FILE_KEY,
					DEFAULT_GIGAPAXOS_CONFIG_FILE).setSystemProperties();
		} catch (IOException e) {
			// ignore as default will still be used
		}
	}

	/**
	 * By default, PaxosConfig.PC will be registered.
	 */
	public static void load() {
		load(PC.class);
	}

	/**
	 * @return Properties in gigapaxos properties file.
	 */
	public static Properties getAsProperties() {
		Properties config = null;
		try {
			// need to preserve case here
			config = Config.getProperties(PC.class, GIGAPAXOS_CONFIG_FILE_KEY,
					DEFAULT_GIGAPAXOS_CONFIG_FILE);
		} catch (IOException e) {
			// ignore as default will still be used
		}
		if (config == null)
			config = Config.getConfig(PC.class);
		return config;
	}

	/**
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void ensureFileHandlerDirExists() throws FileNotFoundException, IOException {
		// ensure log file handler directory exists
		String logFile = System.getProperty("java.util.logging.config.file");
		if(logFile==null) return;
		Properties logProps = new Properties();
		if(new File(logFile).exists())
			logProps.load(new FileInputStream(logFile));
		String logFiles = logProps
				.getProperty("java.util.logging.FileHandler.pattern");
		if (logFiles != null) {
			logFiles = logFiles.replaceFirst("%.*", "").trim().replaceFirst("/[^/]*$", "");
			new File(logFiles).mkdirs();
		}
	}
	/** 
	 * @return True if all parameters are sane.
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static boolean sanityCheck() throws FileNotFoundException, IOException {
		Assert.assertTrue(PC.GIGAPAXOS_DATA_DIR + " must be an absolute path",
				Config.getGlobalString(PC.GIGAPAXOS_DATA_DIR).startsWith("/")
				// only for backwards compatibility
						|| Config.getGlobalString(PC.GIGAPAXOS_DATA_DIR)
								.trim().equals("."));
		if (Config.getGlobalString(PC.GIGAPAXOS_DATA_DIR).trim().equals("."))
			getLogger().warning(
					PC.GIGAPAXOS_DATA_DIR + " must be an absolute path;"
							+ " specifying \".\" is deprecated and discouraged.");
		return true;
	}

	/**
	 * @return A map of names and socket addresses corresponding to servers
	 *         hosting paxos replicas.
	 */
	public static Map<String, InetSocketAddress> getActives() {
		Map<String, InetSocketAddress> map = new HashMap<String, InetSocketAddress>();
		Properties config = getAsProperties();

		Set<String> keys = config.stringPropertyNames();
		for (String key : keys) {
			if (key.trim().startsWith(DEFAULT_SERVER_PREFIX)) {
				map.put(key.replaceFirst(DEFAULT_SERVER_PREFIX, ""),
						Util.getInetSocketAddressFromString(config
								.getProperty(key)));
			}
		}
		return map;
	}

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
	 * Paxos controlled application.
	 */
	public static final Class<?> application = getClassSuppressExceptions(Config
			.getGlobalString(PC.APPLICATION));

	/**
	 * @return Default service name that is replicated at all active replicas.
	 */
	public static final String getDefaultServiceName() {
		return application.getSimpleName() + "0";
	}

    /**
     * @return Logger used by PaxosConfig.
     */
    public static final Logger getLogger() {
        return log
        ;
    }

    /**
	 * All gigapaxos config parameters that can be specified via a configuration
	 * file.
	 */
	public static enum PC implements Config.ConfigurableEnum,
			Config.Disableable {

		/**
		 * Default application managed by gigapaxos.
		 */
		APPLICATION(NoopPaxosApp.class.getName()),

		/**
		 * Default offset for the client facing port.
		 */
		CLIENT_PORT_OFFSET(100),

		/**
		 * Default offset for the client facing ssl port.
		 */
		CLIENT_PORT_SSL_OFFSET(200),

		/**
		 * Verbose debugging and request instrumentation
		 */
		DEBUG(false),
		/**
		 * True means no persistent logging
		 */
		DISABLE_LOGGING(false),

		/**
		 * FIXME: Journaling based log for efficiency. Needs periodic compaction
		 * that is not yet implemented, so this log exists only for performance
		 * instrumentation.
		 */
		ENABLE_JOURNALING(true),

		/**
		 * True means no checkpointing. If logging is enabled (as is the
		 * default), the logs will grow unbounded. So either both should be true
		 * or both should be false.
		 */
		DISABLE_CHECKPOINTING(false),

		/**
		 * 
		 */
		ENABLE_COMPRESSION(true),

		/**
		 * 
		 */
		COMPRESSION_THRESHOLD(4 * 1024 * 1024),

		/**
		 * The default size of the {@link MultiArrayMap} used to store paxos
		 * instances.
		 */
		PINSTANCES_CAPACITY(2000000), // 2M
		/**
		 * The waiting period for paxos instance corpses in order to prevent
		 * inadvertant rebirths because of the missed birthing paxos instance
		 * creation mechanism.
		 */
		MORGUE_DELAY(30000),
		/**
		 * Whether the hibernate option is enabled.
		 */
		HIBERNATE_OPTION(false),
		/**
		 * Whether the pause option is enabled.
		 */
		PAUSE_OPTION(true),

		/**
		 * Fraction of capacity to be reached in order for pausing to get
		 * enabled.
		 */
		PAUSE_SIZE_THRESHOLD(0),

		/**
		 * The time after which the deactivation thread will attempt to pause
		 * idle paxos instances by making a pass over all currently unpaused
		 * instances. This is also the period for which a paxos instance must be
		 * idle in order to be paused.
		 */
		DEACTIVATION_PERIOD(60000), // 30s default

		/**
		 * Limits the rate of pausing to not interfere with request processing.
		 * But it has the downside of increasing the total pause time as well as
		 * limiting the paxos instance creation rate. For example, pausing a
		 * million instances will take 1000s with the default rate limit of
		 * 1000/s.
		 */
		PAUSE_RATE_LIMIT(1000), // /s

		/**
		 * Refer to documentation in {@link SQLPaxosLogger}.
		 */
		MAX_FINAL_STATE_AGE(3600 * 1000),
		/**
		 * Whether request batching is enabled.
		 */
		BATCHING_ENABLED(true),

		/**
		 * 
		 */
		MAX_LOG_FILE_SIZE(64 * 1024 * 1024),

		/**
		 * Wait period for forcibly killing a lower paxos instance version in
		 * order to start a higher version.
		 * <p>
		 * FIXME: Unclear what a good default is for good liveness. It doesn't
		 * really matter for safety of reconfiguration.
		 */
		CAN_CREATE_TIMEOUT(5000),
		/**
		 * Wait period before going forth with a missed birthing paxos instance
		 * creation to see if the instance gets normally created anyway.
		 */
		WAIT_TO_GET_CREATED_TIMEOUT(2000),

		/**
		 * The maximum log message size. The higher the batching, the higher
		 * this value needs to be.
		 */
		MAX_LOG_MESSAGE_SIZE(1024 * 1024 * 5),

		/**
		 * The maximum checkpoint size. The default below is the maximum size of
		 * varchar in embedded derby, which is probably somewhat faster than
		 * clobs, which would be automatically used with bigger checkpoint
		 * sizes.
		 */
		MAX_CHECKPOINT_SIZE(32672),

		/**
		 * Number of checkpoints after which log messages will be garbage
		 * collected for a paxos group. We really don't need to do garbage
		 * collection at all until the size of the table starts affecting log
		 * message retrieval time or the size of the table starts causing the
		 * indexing overhead to become high at insertion time; the latter is
		 * unlikely as we maintain an index on the paxosID key.
		 */
		LOG_GC_FREQUENCY(10),

		/**
		 * Number of log files after which GC will be attempted.
		 */
		JOURNAL_GC_FREQUENCY(5),

		/**
		 * Number of log files after which compaction will be attempted.
		 */
		COMPACTION_FREQUENCY(2),

		/**
		 * The number of log messages after which they are indexed into the DB.
		 * Indexing every log message doubles the logging overhead and doesn't
		 * have to be done (unless #SYNC_INDEX_JOURNAL is enabled). Upon
		 * recovery however, we need to do slightly more work to ensure that all
		 * uncommitted log messages are indexed into the DB.
		 */
		LOG_INDEX_FREQUENCY(100),

		/**
		 * Whether fields in the log messages table in the DB should be indexed.
		 * This index refers to the DB's internal index as opposed to
		 * {@link #DB_INDEX_JOURNAL} that is an explicit index of log messages
		 * journaled on the file system.
		 */
		INDEX_LOG_TABLE(true),

		/**
		 * A tiny amount of minimum sleep imposed on every request in order to
		 * improve batching benefits. Also refer to {@link #BATCH_OVERHEAD}.
		 */
		BATCH_SLEEP_DURATION(0),

		/**
		 * Inverse of the percentage overhead of agreement latency added to the
		 * sleep duration used for increasing batching gains. Also refer to
		 * {@value #BATCH_SLEEP_DURATION}.
		 */
		BATCH_OVERHEAD(0.01),

		/**
		 * 
		 */
		DISABLE_SYNC_DECISIONS(false),

		/**
		 * Maximum number of batched requests. Setting it to infinity means that
		 * the log message size will still limit it.
		 */
		MAX_BATCH_SIZE(2000),

		/**
		 * Checkpoint interval. A larger value means slower recovery, slower
		 * coordinator changes, and less frequent garbage collection, but it
		 * also means less frequent IO or higher request throughput.
		 */
		CHECKPOINT_INTERVAL(400),

		/**
		 * Value of initial state used by the default service name (
		 * {@link PaxosConfig#getDefaultServiceName()}).
		 */
		DEFAULT_NAME_INITIAL_STATE("{}"),

		/**
		 * Number of threads in packet demultiplexer. More than 0 means that we
		 * may not preserve the order of client requests while processing them.
		 */
		PACKET_DEMULTIPLEXER_THREADS(4),

		/**
		 * Whether request order is preserved for requests sent by the same
		 * replica and committed by the same coordinator.
		 */
		ORDER_PRESERVING_REQUESTS(true),

		/**
		 * The replica receiving the request will simply send the request to the
		 * local application replica, i.e., this essentially disables all paxos
		 * coordination. This is used only for testing.
		 */
		EMULATE_UNREPLICATED(false),

		/**
		 * If true, the servers will send no response. For testing only.
		 */
		NO_RESPONSE(false),

		/**
		 * Only for performance instrumentation. If true, replicas will simply
		 * execute the request upon receiving an ACCEPT. This option will break
		 * RSM safety.
		 */
		EXECUTE_UPON_ACCEPT(false),

		/**
		 * Also used for testing. Lazily propagates requests to other replicas
		 * when emulating unreplicated execution mode.
		 */
		LAZY_PROPAGATION(false),

		/**
		 * Enables coalescing of accept replies.
		 */
		BATCHED_ACCEPT_REPLIES(true),

		/**
		 * Enables coalescing of commits. For coalesced commits, a replica must
		 * have the corresponding accept logged, otherwise it will end up
		 * sync'ing and increasing overhead. Enabling this option but not
		 * persistent logging can cause liveness problems.
		 */
		BATCHED_COMMITS(true),

		/**
		 * Whether to store compressed logs in the DB.
		 */
		DB_COMPRESSION(true),

		/**
		 * Instrumentation at various places. Should be enabled only during
		 * testing and disabled during production use.
		 */
		ENABLE_INSTRUMENTATION(false),

		/**
		 * 
		 */
		ENABLE_STATIC_CHECKS(false),

		/**
		 * Whether DelayProfiler should be enabled.
		 */
		DELAY_PROFILER(true),

		/**
		 * 
		 */
		JSON_LIBRARY("json.smart"),

		/**
		 * The top-level directory inside which all gigapaxos-related data is
		 * stored. Paxos logs and checkpoints are stored in the sub-directory
		 * PAXOS_LOGS_DIR. Reconfiguration logs are stored in the sub-directory
		 * RECONFIGURATION_DB_DIR.
		 */
		GIGAPAXOS_DATA_DIR("."),

		/**
		 * Location for paxos logs when an embedded DB is used. This can not be
		 * changed.
		 */
		PAXOS_LOGS_DIR("paxos_logs"),

		/**
		 * Prefix of the paxos DB's name. The whole name is obtained by
		 * concatenating this prefix with the node ID.
		 * 
		 * In the embedded DB case, paxos logs for "nodeID" are stored at
		 * GIGAPAXOS_DATA_DIR/PAXOS_LOGS_DIR/PAXOS_DB_PREFIX"nodeID".
		 */
		PAXOS_DB_PREFIX("paxos_db"),

		/**
		 * Maximum length in characters of a paxos group name.
		 */
		MAX_PAXOS_ID_SIZE(40),

		/**
		 * {@link edu.umass.cs.gigapaxos.paxosutil.SQL.SQLType} type. Currently,
		 * the only other alternative is "MYSQL". Refer the above class to
		 * specify the user name and password.
		 */
		SQL_TYPE("EMBEDDED_DERBY"),

		/**
		 * Maximum size of a paxos replica group.
		 */
		MAX_GROUP_SIZE(16),

		/**
		 * Threshold for throttling client request load.
		 */
		MAX_OUTSTANDING_REQUESTS(8000),

		/**
		 * Sleep millis to throttle client requests if overloaded. Used only for
		 * testing.
		 */
		THROTTLE_SLEEP(0),

		/**
		 * Client-server SSL mode.
		 */
		CLIENT_SSL_MODE(SSLDataProcessingWorker.SSL_MODES.CLEAR),

		/**
		 * Server-server SSL mode.
		 */
		SERVER_SSL_MODE(SSLDataProcessingWorker.SSL_MODES.CLEAR),

		/**
		 * Number of additional sending connections used by paxos. We need this
		 * because the sending throughput of a single TCP connection is limited
		 * and can become a bottleneck at the coordinator.
		 */
		NUM_MESSENGER_WORKERS(1),

		/**
		 * True means respond with sync request to sync yourself if received
		 * sync request from a replica that is already ahead of you.
		 */
		REVERSE_SYNC(true),

		/**
		 * 
		 */
		USE_NIO_SENDER_TASK(false),

		/**
		 * Disable congestion pushback.
		 */
		DISABLE_CC(false),

		/**
		 * If true, we use a garbage collected map that has higher overhead than
		 * a regular map, but is still not a bottleneck.
		 */
		USE_GC_MAP(true),

		/**
		 * Only log meta decisions if corresponding accept was previously
		 * received.
		 */
		LOG_META_DECISIONS(true),

		/**
		 * Whether select packets should be byteified as opposed to
		 * json-stringified.
		 */
		BYTEIFICATION(true),

		/**
		 * 
		 */
		INSTRUMENT_SERIALIZATION(false),

		/**
		 * 
		 */
		STRICT_ADDRESS_CHECKS(false),

		/**
		 * 
		 */
		CLIENT_DEMULTIPLEXER_THREADS(4),
		
		/**
		 * 
		 */
		ENABLE_HANDLE_MESSAGE_REPORT (false),
		
		/**
		 * Disables the feature that redirects requests to the most recent
		 * replica from which a response to a coordinated request, i.e., a
		 * {@link ReplicableRequest} with
		 * {@link ReplicableRequest#needsCoordination()} {@code true}, was
		 * received. Note that enabling this feature means that latency-based
		 * request redirection at the client will not happen, so the client may
		 * be sending requests to far away active replicas even though nearer
		 * ones exist. This is because latency-based redirection works by
		 * probing, i.e., a small fraction,
		 * {@link E2ELatencyAwareRedirector#PROBE_RATIO} of requests are sent to
		 * a random active replica, in order to enable nearest-server
		 * redirection.
		 */
		READ_YOUR_WRITES(false),

		/**
		 * If true, gigapaxos will try to prevent executing (retransmitted)
		 * requests if they had already been executed in the <i>recent</i> past.
		 * In such cases, the response from the first execution will be returned
		 * to the app. This method can not be relied upon for safety as
		 * gigapaxos will cache executed requests for a limited time and is
		 * space-contrained.
		 */
		PREVENT_DOUBLE_EXECUTION(true),

		/**
		 * Whether journal entries should be synchronously indexed in the DB.
		 * Makes journaling (and overall throughput) slower but makes safe
		 * retrieval of logged messages easier both during normal operations and
		 * upon recovery. During normal operations, we just have to check the DB
		 * and also check the pending log in memory to ensure that we don't miss
		 * any log messages. Upon recovery however, there is no pending log in
		 * memory and the system may have crashed with some pending log messages
		 * before they could be inserted into the DB, so we just have to find
		 * the last message logged in the DB and put the rest in a pending queue
		 * for logging into the DB.
		 */
		PAUSABLE_INDEX_JOURNAL(true),

		/**
		 * Whether more than one thread is used to log messages.
		 */
		MULTITHREAD_LOGGER(false),

		/**
		 * True means that the journal entries will be indexed in the DB. False (default) 
		 * means we maintain an in-memory index while infrequently pausing unused entries
		 * to disk (using {@link DiskMap} while reconstructing unsaved entries upon 
		 * recovery using the written-ahead journal log files.
		 */
		DB_INDEX_JOURNAL(false),

		/**
		 * Failure detection timeout in seconds after which a node will be
		 * considered dead if no keepalives have been received from it. Used to
		 * detect coordinator failures.
		 */
		FAILURE_DETECTION_TIMEOUT(6),

		/**
		 * Request timeout in seconds after which the request will be deleted
		 * from the outstanding queue. Currently, there is no effort to remove
		 * any other state for that request from the system.
		 */
		REQUEST_TIMEOUT(10),

		/**
		 * Whether the mapdb package should be used. It seems too slow for our
		 * purposes, so we use our own custom write-ahead logger.
		 */
		USE_MAP_DB(false, true),

		/**
		 * If true, the checkpoints table will be used to also store paused
		 * state as opposed to storing paused state in its own table. The reason
		 * to store paused state in the same table as checkpoints is that we can
		 * easily compute FIXME:
		 */
		USE_CHECKPOINTS_AS_PAUSE_TABLE(true),

		/**
		 * 
		 */
		USE_DISK_MAP(true),

		/**
		 * 
		 */
		USE_HEX_TIMESTAMP(true),

		/**
		 * 
		 */
		LAZY_COMPACTION(true),

		/**
		 * 
		 */
		PAUSE_BATCH_SIZE(1000),

		/**
		 * 
		 */
		SYNC(false),

		/**
		 * 
		 */
		FLUSH(true),

		/**
		 * 
		 */
		SYNC_FCLOSE(true),

		/**
		 * 
		 */
		FLUSH_FCLOSE(true),

		/**
		 * Minimum seconds after last modification when a compaction attempt can
		 * be made.
		 */
		LOGFILE_AGE_THRESHOLD(10 * 60),

		/**
		 * Percentage variation around mean in checkpoint interval.
		 */
		CPI_NOISE(0),

		/**
		 * Whether checkpoints should be batched. We rely on the fact that
		 * synchronous checkpointing is not necessary for safety except for the
		 * initial state and the state after the stop request. We just need to
		 * ensure that the state for the checkpoint is synchronously obtained
		 * from the application immediately after the corresponding request
		 * execution.
		 */
		BATCH_CHECKPOINTS(true),

		/**
		 * 
		 */
		MAX_DB_BATCH_SIZE(10000),

		/**
		 * Number of milliseconds after which
		 * {@link edu.umass.cs.gigapaxos.interfaces.Replicable#execute(edu.umass.cs.gigapaxos.interfaces.Request)}
		 * will be re-attempted until it returns true.
		 */
		HANDLE_REQUEST_RETRY_INTERVAL(100),

		/**
		 * Maximum number of retry attempts for
		 * {@link edu.umass.cs.gigapaxos.interfaces.Replicable#execute(edu.umass.cs.gigapaxos.interfaces.Request)}
		 * . After these many unsuccessful attempts, the paxos instance will be
		 * killed.
		 */
		HANDLE_REQUEST_RETRY_LIMIT(10),

		/**
		 * Broadcast requests at entry replica and use digests in accepts. This
		 * makes a noticeable difference only when the number of groups is small
		 * (like 1 or 2). For more groups, the reordering effects seem to hurt
		 * more than help.
		 * 
		 * Disabled by default. May have liveness problems if no alive server
		 * has a copy of the original request unless requests broadcast by
		 * entry replica are logged.
		 */
		DIGEST_REQUESTS(false),

		/**
		 * Number of active groups up to which digesting is done. Digests seem
		 * to hurt with many groups probably because the cost more than offsets
		 * the benefit. With many groups, the coordinator load balancing benefit
		 * of digests is negligible. The main benefit is that they save one
		 * transmission of the request body, but they increase the total number
		 * of messages by one and lose out on stringification optimizations at
		 * acceptors. Furthermore, a digested accept is useless unless the
		 * corresponding request body arrives, which seems to slow down the
		 * overall throughput of the system.
		 */
		DIGEST_THRESHOLD(5),

		/**
		 * Whether paxos packets across different paxos groups should be batched
		 * if they are going to the same set of destinations.
		 */
		BATCH_ACROSS_GROUPS(true),

		/**
		 * Whether accept batching should be different from
		 * {@link #BATCH_ACROSS_GROUPS}. True means that we won't batch accepts
		 * even if {@link #BATCH_ACROSS_GROUPS} is true. Accept batching doesn't
		 * really help unless {@link #DIGEST_REQUESTS} is enabled and that is by
		 * default enabled only if the number of active instances is small.
		 */
		FLIP_BATCHED_ACCEPTS(false),

		/**
		 * Whether {@link edu.umass.cs.gigapaxos.paxospackets.RequestPacket} and
		 * inherited classes should be batched. This batching is different from
		 * the batching in {@link #BATCHING_ENABLED}. It refers to batching of
		 * entire packets to marginally reduce network overhead. It really makes
		 * little difference either way. With the new byteification methods and
		 * batching opportunities (as in {@link #BATCHING_ENABLED}), this option
		 * actually has a small net cost, so it should be disabled by default.
		 * It may give small benefits with a large number of groups and little
		 * room for batching within each group.
		 */
		BATCHED_REQUESTS(true),

		/**
		 * Local messages are not put in {@link PaxosPacketBatcher}.
		 */
		SHORT_CIRCUIT_LOCAL(true),

		/**
		 * 
		 */
		NUM_MESSAGE_DIGESTS(5),

		/**
		 * 
		 */
		DEBUG_MONITOR(0),

		/**
		 * 
		 */
		ACCEPT_TIMEOUT(Config.getGlobalInt(FAILURE_DETECTION_TIMEOUT)),

		/**
		 * 
		 */
		POKE_COORDINATOR(true),

		/**
		 * Minimum number of packets in order to consider
		 * {@link edu.umass.cs.gigapaxos.paxospackets.PaxosPacket} batching.
		 */
		MIN_PP_BATCH_SIZE(3),

		/**
		 * 
		 */
		LOG_DISKMAP_CAPACITY(Config.getGlobalInt(PINSTANCES_CAPACITY)),

		/**
		 * 
		 */
		DIGEST_THRESHOLD_SIZE(512),

		/**
		 * 
		 */
		BLOCKING_CHECKPOINT(false),

		/**
		 * True means we garbage collect accepts up to the slot that a majority
		 * have executed; otherwise up to the slot that a majority have
		 * checkpointed.
		 */
		GC_MAJORITY_EXECUTED(true),

		/**
		 * 
		 */
		DB_USER("user"),

		/**
		 * 
		 */
		DB_PASSWORD("password"),
		
		/**
		 * Turns on delay emulation.
		 */
		EMULATE_DELAYS (false),

		// seconds
		PREPARE_TIMEOUT (Config.getGlobalInt(FAILURE_DETECTION_TIMEOUT)),

		ENABLE_FRAGMENTATION (true),

		/* NIO max payload is 64MB, so environments with larger messages need
		 to set this appropriately. Prepare replies are the only potentially
		 problematically large messages as they can include batched ACCEPTs
		 for a large number of slots that can grow large.
		 */
		NIO_MAX_PAYLOAD_SIZE (16 * 1024 * 1024),

		/**
		 * If true, identical requests, i.e., requests with the same requestID,
		 * paxosID, and sending client's socket address, will get a recently
		 * cached response if any instead of re-coordinating the request. This
		 * is useful to reduce the likelihood of double-execution, but does not
		 * eliminate it completely.
		 */
		ENABLE_RESPONSE_CACHING(true),

		FORWARD_PREEMPTED_REQUESTS(true),

		/**
		 * If true, an Active Replica will start coordinator election process
		 * (i.e., Phase 1 of Paxos or Leader Election) during startup, when an
		 * Active has not yet run for a Coordinator.
		 * Checkout {@link PaxosInstanceStateMachine#notRunYet()}.
		 * Note that this option ensures better liveness since replica groups
		 * will have an elected coordinator faster. However, the options can cause
		 * flaky leadership during replica group startup: rapid leader changes
		 * before a long-running coordinator is elected.
		 * <p>
		 * If false, there will be a coordinator chosen deterministically during
		 * startup, even when all the Nodes do not start with Phase 1 of Paxos.
		 * This option is more stable but can cause liveness issue when the
		 * deterministically chosen coordinator during startup suddenly crashed,
		 * making all the Nodes need to wait until coordinator timeout.
		 */
		ENABLE_STARTUP_COORDINATOR_ELECTION(true),


		/**
		 * FIXME: The options below only exist for testing stringification
		 * overhead. They should probably be moved to {@link TESTPaxosConfig}.
		 * Most of these will compromise safety.
		 */

		/***************** Start of unsafe testing options *******************/
		/**
		 * Testing option.
		 */
		JOURNAL_COMPRESSION(false, true),

		/**
		 * Testing option.
		 */
		STRINGIFY_WO_JOURNALING(false, true),

		/**
		 * Testing option.
		 */
		NON_COORD_ONLY(false, true),

		/**
		 * Testing option.
		 */
		NO_STRINGIFY_JOURNALING(false, true),

		/**
		 * Testing option.
		 */
		COORD_STRINGIFIES_WO_JOURNALING(false, true),

		/**
		 * Testing option.
		 */
		DONT_LOG_DECISIONS(false, true),

		/**
		 * Testing option.
		 */
		NON_COORD_DONT_LOG_DECISIONS(false, true),

		/**
		 * Testing option.
		 */
		COORD_DONT_LOG_DECISIONS(false, true),

		/**
		 * Testing option.
		 */
		COORD_JOURNALS_WO_STRINGIFYING(false, true),

		/**
		 * Testing option
		 */
		ALL_BUT_APPEND(false, true),

		/**
		 * Testing option. Implies no coordinator fault-tolerance.
		 */
		DISABLE_GET_LOGGED_MESSAGES(false, true),
		



		/*********** End of unsafe testing options *****************/

		;

		final Object defaultValue;
		final boolean unsafeTestingOnly;

		PC(Object defaultValue) {
			this(defaultValue, false);
		}

		PC(Object defaultValue, boolean testingOnly) {
			this.defaultValue = defaultValue;
			this.unsafeTestingOnly = testingOnly;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}

		@Override
		public String getDefaultConfigFile() {
			return DEFAULT_GIGAPAXOS_CONFIG_FILE;
		}

		@Override
		public String getConfigFileKey() {
			return GIGAPAXOS_CONFIG_FILE_KEY;
		}

		@Override
		public boolean isDisabled() {
			return UNSAFE_TESTING ? false : this.unsafeTestingOnly;
		}
	}

	private static boolean UNSAFE_TESTING = false;

	/**
	 * @return Default client port offset based on whether ssl is enabled.
	 */
	public static int getClientPortOffset() {
		if (SSL_MODES.valueOf(Config.getGlobalString(PC.CLIENT_SSL_MODE)) == SSL_MODES.CLEAR)
			return Config.getGlobalInt(PC.CLIENT_PORT_OFFSET);
		else
			return Config.getGlobalInt(PC.CLIENT_PORT_SSL_OFFSET);
	}

	/**
	 * @param servers
	 * @param globalInt
	 * @return Socket addresses with the port offset added to each element.
	 */
	public static Set<InetSocketAddress> offsetSocketAddresses(
			Set<InetSocketAddress> servers, int globalInt) {
		Set<InetSocketAddress> offsetted = new HashSet<InetSocketAddress>();
		for (InetSocketAddress isa : servers) {
			offsetted.add(new InetSocketAddress(isa.getAddress(), isa.getPort()
					+ globalInt));
		}
		return offsetted;
	}

	/**
	 * @param level
	 */
	public static void setConsoleHandler(Level level) {
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(level);
		getLogger().setLevel(level);
		getLogger().addHandler(handler);
		getLogger().setUseParentHandlers(false);

		NIOTransport.getLogger().setLevel(level);
		NIOTransport.getLogger().addHandler(handler);
		NIOTransport.getLogger().setUseParentHandlers(false);

	}

	protected static void setConsoleHandler() {
		if (System.getProperty("java.util.logging.config.file") == null)
			setConsoleHandler(Level.INFO);
	}

	@SuppressWarnings({ "javadoc", "unchecked", "rawtypes" })
	public static void sanityCheck(NodeConfig nodeConfig) throws IOException {
		for (Object n : nodeConfig.getNodeIDs()) {
			for (Object m : nodeConfig.getNodeIDs())
				if (!n.equals(m)
						&& nodeConfig.getNodeAddress(n).equals(
								nodeConfig.getNodeAddress(m))
						&& (nodeConfig.getNodePort(n) == nodeConfig
								.getNodePort(m)
								|| nodeConfig.getNodePort(n)
										+ Config.getGlobalInt(PC.CLIENT_PORT_OFFSET) == nodeConfig
										.getNodePort(m) || nodeConfig
								.getNodePort(n)
								+ Config.getGlobalInt(PC.CLIENT_PORT_SSL_OFFSET) == nodeConfig
								.getNodePort(m)

						))
					throw new IOException(
							"Clash in nodeConfig between "
									+ n
									+ " and "
									+ m
									+ ": node socket addresses should not be identical or "
									+ "be exactly "
									+ "CLIENT_PORT_OFFSET="
									+ Config.getGlobalInt(PC.CLIENT_PORT_OFFSET)
									+ " or "
									+ "CLIENT_PORT_SSL_OFFSET="
									+ Config.getGlobalInt(PC.CLIENT_PORT_SSL_OFFSET)
									+ " apart.");
		}
	}

	/**
	 * @return Default node config.
	 */
	public static NodeConfig<String> getDefaultNodeConfig() {
		final Map<String, InetSocketAddress> actives = PaxosConfig.getActives();

		// FIXME: don't use reconfiguration classes in gigapaxos
		return new /*Reconfigurable*/NodeConfig<String>() {

			@Override
			public boolean nodeExists(String id) {
				return actives.containsKey(id);
			}

			@Override
			public InetAddress getNodeAddress(String id) {
				return actives.containsKey(id) ? actives.get(id).getAddress()
						: null;
			}

			/**
			 * Bind address is returned as the same as the regular address coz
			 * we don't really need a bind address after all.
			 * 
			 * @param id
			 * @return Bind address.
			 */
			@Override
			public InetAddress getBindAddress(String id) {
				return actives.containsKey(id) ? actives.get(id).getAddress()
						: null;
			}

			@Override
			public int getNodePort(String id) {
				return actives.containsKey(id) ? actives.get(id).getPort()
						: null;
			}

			@Override
			public Set<String> getNodeIDs() {
				return new HashSet<String>(actives.keySet());
			}

			@Override
			public String valueOf(String strValue) {
				return this.nodeExists(strValue) ? strValue : null;
			}

			@Override
			public Set<String> getValuesFromStringSet(Set<String> strNodes) {
				throw new RuntimeException("Method not yet implemented");
			}

			@Override
			public Set<String> getValuesFromJSONArray(JSONArray array)
					throws JSONException {
				throw new RuntimeException("Method not yet implemented");
			}

//			@Override
//			public Set<String> getActiveReplicas() {
//				return new HashSet<String>(actives.keySet());
//			}
//
//			@Override
//			public Set<String> getReconfigurators() {
//				return new HashSet<String>(actives.keySet());
//			}
		};
	}

	private static boolean noPropertiesFile = false;

	/**
	 * Used mainly for testing.
	 */
	public static void setNoPropertiesFile() {
		noPropertiesFile = true;
	}

	/**
	 * @return Name of gigapaxos properties file.
	 */
	public static String getPropertiesFile() {
		//assert(System.getProperty(PaxosConfig.GIGAPAXOS_CONFIG_FILE_KEY) ==null);
		return System.getProperty(PaxosConfig.GIGAPAXOS_CONFIG_FILE_KEY) != null ? System
				.getProperty(PaxosConfig.GIGAPAXOS_CONFIG_FILE_KEY)
				: !noPropertiesFile ? PaxosConfig.DEFAULT_GIGAPAXOS_CONFIG_FILE
						: null;
	}

}
