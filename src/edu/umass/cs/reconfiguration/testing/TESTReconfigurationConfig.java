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
package edu.umass.cs.reconfiguration.testing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.utils.Config;

/**
 * @author V. Arun
 * 
 *         Configuration parameters for the gigapaxos testing suite.
 */
public class TESTReconfigurationConfig {

	protected static void load() {
		ReconfigurationConfig.load();
		// testing specific config parameters
		try {
			Config.register(TRC.class, TESTING_CONFIG_FILE_KEY,
					DEFAULT_TESTING_CONFIG_FILE);
		} catch (IOException e) {
			// ignore as defaults will be used
		}
	}

	static {
		load();
	}

	private static final String TESTING_CONFIG_FILE_KEY = "testingConfig";
	private static final String DEFAULT_TESTING_CONFIG_FILE = "testing.properties";

	/**
	 * Reconfiguration testing config parameters.
	 */
	public static enum TRC implements Config.ConfigurableEnum {

		/**
		 * 
		 */
		NUM_RECONFIGURATORS(5),

		/**
		 * 
		 */
		NUM_ACTIVES(8),

		/**
		 * 
		 */
		AR_PREFIX("AR"),

		/**
		 * 
		 */
		RC_PREFIX("RC"),

		/**
		 * 
		 */
		AR_START_PORT(2000),

		/**
		 * 
		 */
		RC_START_PORT(3000),

		/**
		 * 
		 */
		NAME_PREFIX("name"),

		/**
		 * 
		 */
		TEST_CREATE_RATE(1000),

		/**
		 * 
		 */
		TEST_APP_REQUEST_RATE(50),

		/**
		 * 
		 */
		TEST_NUM_APP_NAMES(1),

		/**
		 * 
		 */
		TEST_RECONFIGURATION_THROUGHPUT_NUM_APP_NAMES(100),

		/**
		 * 
		 */
		TEST_BATCH_SIZE(100),

		/**
		 * 
		 */
		NUM_CLIENTS(1),

		/**
		 * 
		 */
		TEST_RTX_TIMEOUT(TESTReconfigurationClient.DEFAULT_RTX_TIMEOUT),

		/**
		 * Default number of retries when expecting success.
		 */
		TEST_RETRIES(2),

		/**
		 * 
		 */
		TEST_APP_REQUEST_TIMEOUT(TESTReconfigurationClient.DEFAULT_APP_REQUEST_TIMEOUT),

		/**
		 * 
		 */
		TEST_EXISTS_TIMEOUT(1000),

		/**
		 * 
		 */
		TEST_NUM_REQUESTS_PER_NAME(10),

		/**
		 * 
		 */
		TEST_PORT(61000),

		;

		final Object defaultValue;

		TRC(Object defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}
		
		@Override
		public String getDefaultConfigFile() {
			return DEFAULT_TESTING_CONFIG_FILE;
		}

		@Override
		public String getConfigFileKey() {
			return TESTING_CONFIG_FILE_KEY;
		}
	}

	/**
	 * @return A map of names and socket addresses corresponding to servers
	 *         hosting paxos replicas.
	 */
	protected static Map<String, InetSocketAddress> getLocalActives() {
		Map<String, InetSocketAddress> map = new HashMap<String, InetSocketAddress>();
		for (int i = 0; i < Config.getGlobalInt(TRC.NUM_ACTIVES); i++)
			map.put(Config.getGlobalString(TRC.AR_PREFIX) + i,
					new InetSocketAddress("localhost", Config
							.getGlobalInt(TRC.AR_START_PORT) + i));
		return map;
	}

	protected static Map<String, InetSocketAddress> getLocalReconfigurators() {
		Map<String, InetSocketAddress> map = new HashMap<String, InetSocketAddress>();
		for (int i = 0; i < Config.getGlobalInt(TRC.NUM_RECONFIGURATORS); i++)
			map.put(Config.getGlobalString(TRC.RC_PREFIX) + i,
					new InetSocketAddress("localhost", Config
							.getGlobalInt(TRC.RC_START_PORT) + i));
		return map;
	}

	/**
	 * 
	 */
	public static final int MAX_NODE_ID = 10000;

	/**
	 * to enable retransmission of requests by TESTPaxosClient
	 */
	public static final boolean ENABLE_CLIENT_REQ_RTX = false;
	/**
	 * default retransmission timeout
	 */
	public static final long CLIENT_REQ_RTX_TIMEOUT = 8000;

	private static boolean clean_db = Config
			.getGlobalBoolean(PC.DISABLE_LOGGING)
			&& !Config.getGlobalBoolean(PC.ENABLE_JOURNALING);

	private static ArrayList<Object> failedNodes = new ArrayList<Object>();

	/**
	 * @param b
	 */
	public static void setCleanDB(boolean b) {
		clean_db = b;
	}

	/**
	 * @return True if DB should be cleaned.
	 */
	public static boolean getCleanDB() {
		return clean_db;
	}

	/**
	 * 
	 */
	public static final boolean PAXOS_MANAGER_UNIT_TEST = false;

	/******************** End of distributed settings **************************/

	/**
	 * Cleans DB if -c command line arg is specified.
	 * 
	 * @param args
	 * @return True if -c flag is present.
	 */
	public static final boolean shouldCleanDB(String[] args) {
		for (String arg : args)
			if (arg.trim().equals("-c"))
				return true;
		return false;
	}

	/**
	 * @param args
	 * 
	 * @return Config directory parsed as the first argument other than "-c".
	 */
	public static final String getConfDirArg(String[] args) {
		for (String arg : args)
			if (arg.trim().startsWith("-T"))
				return arg.replace("-T", "");
		return null;
	}

	/**
	 * @param nodeID
	 */
	public synchronized static void crash(int nodeID) {
		TESTReconfigurationConfig.failedNodes.add(nodeID);
	}

	/**
	 * @param nodeID
	 */
	public synchronized static void recover(int nodeID) {
		TESTReconfigurationConfig.failedNodes.remove(new Integer(nodeID));
	}

	/**
	 * @param nodeID
	 * @return True if crash is being simulated.
	 */
	public static boolean isCrashed(Object nodeID) {
		return TESTReconfigurationConfig.failedNodes.contains(nodeID);
	}

	/**
	 * @param level
	 */
	public static void setConsoleHandler(Level level) {
		PaxosConfig.setConsoleHandler(level);
	}

	protected static void setConsoleHandler() {
		setConsoleHandler(Level.INFO);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("edu.umass.cs".replaceAll(".*\\.", ""));
	}
}
