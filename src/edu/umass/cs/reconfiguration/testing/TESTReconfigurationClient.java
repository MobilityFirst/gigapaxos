package edu.umass.cs.reconfiguration.testing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationConfig.TRC;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;

/**
 * @author arun
 * 
 *         This class is designed to test all client commands including
 *         creation, deletion, request actives, and app requests to names.
 */
// @FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class TESTReconfigurationClient {

	private static Logger log = Reconfigurator.getLogger();

	private static Set<TESTReconfigurationClient> allInstances = new HashSet<TESTReconfigurationClient>();

	class RCClient extends ReconfigurableAppClientAsync {

		public RCClient(Set<InetSocketAddress> reconfigurators)
				throws IOException {
			super(reconfigurators);
		}

		@Override
		public Request getRequest(String stringified)
				throws RequestParseException {
			try {
				return NoopApp.staticGetRequest(stringified);
			} catch (JSONException e) {
				// e.printStackTrace();
			}
			return null;
		}

		@Override
		public Set<IntegerPacketType> getRequestTypes() {
			return NoopApp.staticGetRequestTypes();
		}

		public void close() {
			super.close();
		}
	}

	private final RCClient[] clients;
	private final Set<String> reconfigurators;

	private static boolean loopbackMode = true;

	protected static void setLoopbackMode(boolean b) {
		loopbackMode = b;
	}

	/**
	 * @throws IOException
	 */
	public TESTReconfigurationClient() throws IOException {
		this(loopbackMode ? TESTReconfigurationConfig.getLocalReconfigurators()
				: ReconfigurationConfig.getReconfigurators());
	}

	protected TESTReconfigurationClient(
			Map<String, InetSocketAddress> reconfigurators) throws IOException {
		allInstances.add(this);
		clients = new RCClient[Config.getGlobalInt(TRC.NUM_CLIENTS)];
		for (int i = 0; i < clients.length; i++)
			clients[i] = new RCClient(new HashSet<InetSocketAddress>(
					reconfigurators.values()));
		this.reconfigurators = reconfigurators.keySet();
	}

	private String NAME = Config.getGlobalString(TRC.NAME_PREFIX);
	private String INITIAL_STATE = "some_initial_state";

	private static void monitorWait(boolean[] monitor, Long timeout) {
		synchronized (monitor) {
			if (!monitor[0])
				try {
					if (timeout != null)
						monitor.wait(timeout);
					else
						monitor.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
	}

	private static void monitorNotify(Object monitor) {
		synchronized (monitor) {
			monitor.notify();
		}
	}

	private final ConcurrentHashMap<Long, Request> outstanding = new ConcurrentHashMap<Long, Request>();
	private static int numReconfigurations = 0;

	private static synchronized void setNumReconfigurations(int numRC) {
		numReconfigurations = numRC;
	}

	private RCClient getRandomClient() {
		return clients[(int) (Math.random() * clients.length)];
	}

	private void testAppRequest(String name) throws NumberFormatException,
			IOException {
		this.testAppRequest(new AppRequest(name, Long.valueOf(name.replaceAll(
				"[a-z]*", "")), "request_value",
				AppRequest.PacketType.DEFAULT_APP_REQUEST, false));
	}

	private void testAppRequest(AppRequest request)
			throws NumberFormatException, IOException {
		long t = System.currentTimeMillis();
		this.outstanding.put(request.getRequestID(), request);
		log.log(Level.INFO,
				"Sending app request {0} for name {1}",
				new Object[] { request.getClass().getSimpleName(),
						request.getServiceName() });
		getRandomClient().sendRequest(request, new RequestCallback() {
			@Override
			public void handleResponse(Request response) {
				outstanding.remove(request.getRequestID());
				synchronized (outstanding) {
					outstanding.notify();
				}
				DelayProfiler.updateDelay("appRequest", t);
				if (response instanceof ActiveReplicaError) {
					log.log(Level.INFO,
							"Received {0} for app request to name {1} in {2}ms; |outstanding|={3}",
							new Object[] {
									ActiveReplicaError.class.getSimpleName(),
									request.getServiceName(),
									(System.currentTimeMillis() - t),
									outstanding.size() });
				}
				if (response instanceof AppRequest) {
					log.log(Level.INFO,
							"Received response for app request to name {0} exists in {1}ms; |outstanding|={2}",
							new Object[] { request.getServiceName(),
									(System.currentTimeMillis() - t),
									outstanding.size() });
					String reqValue = ((AppRequest) response).getValue();
					assert (reqValue != null && reqValue.split(" ").length == 2) : reqValue;
					setNumReconfigurations(Integer.valueOf(reqValue.split(" ")[1]));
				}
			}
		});
	}

	/**
	 * 
	 * @param names
	 * @param rounds
	 *            Number of rounds wherein each round sends one request to each
	 *            name, i.e., a total of names.length*rounds requests.
	 * @return
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private boolean testAppRequests(String[] names, int rounds)
			throws NumberFormatException, IOException {
		long t = System.currentTimeMillis();
		int numReconfigurationsBefore = numReconfigurations;
		boolean done = true;
		for (int i = 0; i < rounds; i++) {
			done = done && this.testAppRequests(names, true);
			log.log(Level.INFO, "Completed round {0} of {1} of app requests",
					new Object[] { i, rounds });
		}
		int delta = numReconfigurations - numReconfigurationsBefore;
		if (delta > 0)
			DelayProfiler.updateValue("reconfiguration_rate", (delta * 1000)
					/ (System.currentTimeMillis() - t));
		return done;
	}

	private void testAppRequests(Collection<Request> requests, RateLimiter r)
			throws NumberFormatException, IOException {
		for (Request request : requests)
			if (request instanceof AppRequest) {
				testAppRequest((AppRequest) request);
				r.record();
			}
	}

	private boolean testAppRequests(String[] names, boolean retryUntilSuccess)
			throws NumberFormatException, IOException {
		RateLimiter r = new RateLimiter(
				Config.getGlobalDouble(TRC.TEST_APP_REQUEST_RATE));
		for (int i = 0; i < names.length; i++) {
			// non-blocking
			this.testAppRequest(names[i]);
			r.record();
		}
		waitForAppResponses(Config.getGlobalLong(TRC.TEST_RTX_TIMEOUT));
		if (retryUntilSuccess) {
			while (!outstanding.isEmpty()) {
				testAppRequests(outstanding.values(), r);
				log.log(Level.INFO, "Retrying {0} outstanding app requests",
						new Object[] { outstanding.size() });
				try {
					Thread.sleep(Config.getGlobalLong(TRC.TEST_RTX_TIMEOUT));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return outstanding.isEmpty();
	}

	private boolean testExists(String[] names) throws IOException {
		boolean exists = testExists(names, true);
		assert (exists);
		return exists;
	}

	private boolean testNotExists(String[] names) throws IOException {
		return testExists(names, false);
	}

	private boolean testExists(String[] names, boolean exists)
			throws IOException {
		boolean retval = true;
		for (int i = 0; i < names.length; i++) {
			retval = retval && testExists(names[i], exists);
		}
		return retval;
	}

	private boolean testExists(String name, boolean exists) throws IOException {
		return testExists(name, exists, null);
	}

	/**
	 * Blocks until existence verified or until timeout. Attempts
	 * retransmissions during this interval if it gets failed responses.
	 * 
	 * @param name
	 * @param exists
	 * @param timeout
	 * @return
	 * @throws IOException
	 */
	private boolean testExists(String name, boolean exists, Long timeout)
			throws IOException {
		long t = System.currentTimeMillis();
		if (timeout == null)
			timeout = Config.getGlobalLong(TRC.TEST_RTX_TIMEOUT);
		boolean[] success = new boolean[1];
		do {
			log.log(Level.INFO, "Testing "
					+ (exists ? "existence" : "non-existence") + " of {0}",
					new Object[] { name });
			getRandomClient().sendRequest(new RequestActiveReplicas(name),
					new RequestCallback() {

						@Override
						public void handleResponse(Request response) {
							DelayProfiler.updateDelay("requestActiveReplicas",
									t);
							if (response instanceof RequestActiveReplicas) {
								log.log(Level.INFO,
										"Verified that name {0} {1} in {2}ms",
										new Object[] {
												name,
												!((RequestActiveReplicas) response)
														.isFailed() ? "exists"
														: "does not exist",
												(System.currentTimeMillis() - t) });
								success[0] = ((RequestActiveReplicas) response)
										.isFailed() ^ exists;
								monitorNotify(success);
							}
						}
					});
			monitorWait(success, timeout);

		} while (!success[0]
				&& (timeout == null || System.currentTimeMillis() - t < timeout));
		return success[0];
	}

	private boolean testBatchCreate(String[] names, int batchSize)
			throws IOException {
		Map<String, String> nameStates = new HashMap<String, String>();
		for (int i = 0; i < names.length; i++)
			nameStates.put(names[i], "some_initial_state" + i);
		return testBatchCreate(nameStates, batchSize);
	}

	private boolean testBatchCreate(Map<String, String> nameStates,
			int batchSize) throws IOException {
		if (simpleBatchCreate)
			return testBatchCreateSimple(nameStates, batchSize);

		// else
		CreateServiceName[] creates = CreateServiceName.makeCreateNameRequest(
				nameStates, batchSize, reconfigurators);

		boolean created = true;
		for (CreateServiceName create : creates) {
			created = created && testCreate(create);
		}
		return created;
	}

	private static final boolean simpleBatchCreate = true;

	private boolean testBatchCreateSimple(Map<String, String> nameStates,
			int batchSize) throws IOException {
		return testCreate(new CreateServiceName(null, nameStates));
	}

	private boolean testCreate(String name, String state) throws IOException {
		return testCreate(new CreateServiceName(name, state));
	}

	private boolean testCreate(CreateServiceName create) throws IOException {
		return testCreate(create, null);
	}

	private boolean testCreate(CreateServiceName create, Long timeout)
			throws IOException {
		long t = System.currentTimeMillis();
		boolean[] success = new boolean[1];
		getRandomClient().sendRequest(create, new RequestCallback() {

			@Override
			public void handleResponse(Request response) {
				if (response instanceof CreateServiceName) {
					log.log(Level.INFO,
							"{0} name {1}{2} in {3}ms : {4}",
							new Object[] {
									!((CreateServiceName) response).isFailed() ? "Created"
											: "Failed to create",
									create.getServiceName(),
									create.nameStates != null
											&& !create.nameStates.isEmpty() ? "("
											+ create.nameStates.size() + ")"
											: "",
									(System.currentTimeMillis() - t), response });
					success[0] = !((CreateServiceName) response).isFailed();
					monitorNotify(success);
				}
			}
		});
		monitorWait(success, timeout);
		return success[0];
	}

	// sequential creates
	private boolean testCreates(String[] names) throws IOException {
		boolean created = true;
		for (int i = 0; i < names.length; i++)
			created = created && testCreate(names[i], generateRandomState());
		return created;
	}

	// sequentially tests deletes of names
	private boolean testDeletes(String[] names) throws IOException {
		boolean deleted = true;
		for (String name : names)
			deleted = deleted && this.testDelete(name);
		assert (deleted);
		return deleted;
	}

	private boolean testDelete(String name) throws IOException {
		return testDelete(name, null);
	}

	// blocking delete until success or timeout of a single name
	private boolean testDelete(String name, Long timeout) throws IOException {
		long t = System.currentTimeMillis();
		if (timeout == null)
			timeout = Config.getGlobalLong(TRC.TEST_RTX_TIMEOUT);
		boolean[] success = new boolean[1];
		log.log(Level.INFO, "Sending delete request for name {0}",
				new Object[] { name });
		do {
			getRandomClient().sendRequest(new DeleteServiceName(name),
					new RequestCallback() {

						@Override
						public void handleResponse(Request response) {
							if (response instanceof DeleteServiceName) {
								log.log(Level.INFO,
										"{0} name {1} in {2}ms",
										new Object[] {
												!((DeleteServiceName) response)
														.isFailed() ? "Deleted"
														: "Failed to delete",
												name,
												(System.currentTimeMillis() - t) });
								success[0] = !((DeleteServiceName) response)
										.isFailed();
								monitorNotify(success);
							}
						}
					});
			monitorWait(success, timeout);
		} while (!success[0]);
		return success[0];
	}

	private void waitForAppResponses(long duration) {
		long t = System.currentTimeMillis(), remaining = duration;
		while (!outstanding.isEmpty()
				&& (remaining = duration - (System.currentTimeMillis() - t)) > 0)
			synchronized (outstanding) {
				try {
					outstanding.wait(remaining);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
	}

	/**
	 * 
	 */
	public void close() {
		for (int i = 0; i < this.clients.length; i++)
			this.clients[i].close();
	}

	private String generateRandomState() {
		return INITIAL_STATE + (long) (Math.random() * Long.MAX_VALUE);
	}

	private String generateRandomName() {
		return NAME + (long) (Math.random() * Long.MAX_VALUE);
	}

	private String[] generateRandomNames(int n) {
		String[] names = new String[n];
		for (int i = 0; i < n; i++)
			names[i] = generateRandomName();
		return names;
	}

	/**
	 * Tests creation, existence, app requests, deletion, and non-existence of a
	 * set of names. Assumes that we start from a clean slate, i.e., none of the
	 * randomly generated names exists before the test.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testBasic() throws IOException {
		String[] names = generateRandomNames(Config
				.getGlobalInt(TRC.TEST_NUM_APP_NAMES));
		DelayProfiler.clear();

		boolean test = testNotExists(names)
				&& testCreates(names)
				&& testExists(names)
				&& testAppRequests(names,
						Config.getGlobalInt(TRC.TEST_NUM_REQUESTS_PER_NAME))
				&& testDeletes(names) && testNotExists(names);
		log.info("testBasic: " + DelayProfiler.getStats());
		Assert.assertEquals(test, true);
	}

	/**
	 * Same as {@link #testBasic()} but with batch created names.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testBatchedBasic() throws IOException {
		// test batched creates
		String[] bNames = generateRandomNames(Config
				.getGlobalInt(TRC.TEST_NUM_APP_NAMES));
		boolean test = testNotExists(bNames)
				&& testBatchCreate(bNames,
						Config.getGlobalInt(TRC.TEST_BATCH_SIZE))
				&& (testExists(bNames))
				&& testAppRequests(bNames,
						Config.getGlobalInt(TRC.TEST_NUM_REQUESTS_PER_NAME))
				&& testDeletes(bNames) && testNotExists(bNames);
		log.info("testBatchedBasic: " + DelayProfiler.getStats());
		Assert.assertEquals(test, true);
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testReconfigurationThroughput() throws IOException,
			InterruptedException {
		DelayProfiler.clear();
		String[] names = generateRandomNames(Math.max(1,
				Config.getGlobalInt(TRC.TEST_NUM_APP_NAMES)));
		long t = System.currentTimeMillis();
		int before = numReconfigurations;
		Assert.assertEquals(
				testBatchCreate(names, Config.getGlobalInt(TRC.TEST_BATCH_SIZE))
						&& testExists(names) && testAppRequests(names, 1), true);
		int delta = numReconfigurations - before;
		if (delta > 0) {
			Thread.sleep(1000);
			DelayProfiler.updateValue("reconfiguration_rate", (delta * 1000)
					/ (System.currentTimeMillis() - t));
			System.out.println("testReconfigurationThroughput: "
					+ DelayProfiler.getStats());
			log.info("testReconfigurationThroughput: "
					+ DelayProfiler.getStats());
		}
		Assert.assertEquals(testDeletes(names) && testNotExists(names), true);
		Thread.sleep(1000);
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void startServers() throws IOException, InterruptedException {
		TESTReconfigurationMain.startLocalServers();
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@AfterClass
	public static void closeServers() throws IOException, InterruptedException {
		for (TESTReconfigurationClient client : allInstances)
			client.close();
		TESTReconfigurationMain.closeServers();
	}

	protected TESTReconfigurationClient allTests() throws InterruptedException {
		try {
			testBasic();
			testBatchedBasic();
			testReconfigurationThroughput();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return this;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ReconfigurationConfig.setConsoleHandler();
		TESTReconfigurationConfig.load();

		setLoopbackMode(false);

	}
}
