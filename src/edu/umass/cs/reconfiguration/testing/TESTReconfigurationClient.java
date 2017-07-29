package edu.umass.cs.reconfiguration.testing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
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
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.AppRequest.PacketType;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureActiveNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureRCNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ServerReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationConfig.TRC;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Repeat;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *         This class is designed to test all client commands including
 *         creation, deletion, request actives, and app requests to names.
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class TESTReconfigurationClient extends DefaultTest {

	private static final Logger log = ReconfigurationConfig.getLogger();
	private static final int REPEAT = 10;

	private static enum ProfilerKeys {
		reconfiguration_rate, request_actives, app_request, create, delete
	};

	private static Set<TESTReconfigurationClient> allInstances = new HashSet<TESTReconfigurationClient>();

	class RCClient extends ReconfigurableAppClientAsync<Request> {

		public RCClient(Set<InetSocketAddress> reconfigurators)
				throws IOException {
			super(reconfigurators, true);
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

		@Override
		public Set<IntegerPacketType> getMutualAuthRequestTypes() {
			return NoopApp.staticGetMutualAuthRequestTypes();
		}

		public void close() {
			super.close();
		}
	}

	private static RCClient[] clients;
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
		if (clients == null) {
			clients = new RCClient[Config.getGlobalInt(TRC.NUM_CLIENTS)];
			for (int i = 0; i < clients.length; i++)
				clients[i] = new RCClient(new HashSet<InetSocketAddress>(
						reconfigurators.values()));
		}
		this.reconfigurators = reconfigurators.keySet();
	}

	private static String NAME = Config.getGlobalString(TRC.NAME_PREFIX);
	private static String INITIAL_STATE = "some_initial_state";

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

	private static int numReconfigurations = 0;

	private static synchronized void setNumReconfigurations(int numRC) {
		numReconfigurations = numRC;
	}

	private RCClient getRandomClient() {
		return clients[(int) (Math.random() * clients.length)];
	}

	private boolean testAppRequest(String name,
			Map<Long, Request> outstandingQ, Long timeout)
			throws NumberFormatException, IOException {
		return this.testAppRequest(getAppRequest(name), outstandingQ, timeout);
	}

	private static AppRequest getAppRequest(String name) {
		return getAppRequest(name, AppRequest.PacketType.DEFAULT_APP_REQUEST);
	}

	private static AppRequest getAppRequest(String name,
			AppRequest.PacketType type) {
		return new AppRequest(name,
				// Long.valueOf(name.replaceAll("[a-z]*", "")),
				(long) (Math.random() * Long.MAX_VALUE), "request_value", type,
				false);
	}

	// Similar to testAppRequest but with a non-default request type
	private boolean testAppRequestNonDefault(String name,
			AppRequest.PacketType type) throws NumberFormatException,
			IOException {
		return this.testAppRequest(getAppRequest(name, type),
				new ConcurrentHashMap<Long, Request>(), null);
	}

	private static final String appName = ReconfigurationConfig.application
			.getSimpleName();

	/**
	 * Tests an app request and synchronously waits for the response if
	 * {@code outstandingQ} is null, else it asynchronously sends the request.
	 * 
	 * @param request
	 * @param outstandingQ
	 * @return
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private boolean testAppRequest(AppRequest request,
			Map<Long, Request> outstandingQ, Long timeout)
			throws NumberFormatException, IOException {
		long t = System.currentTimeMillis();
		boolean[] success = new boolean[1];
		if (outstandingQ != null)
			outstandingQ.put(request.getRequestID(), request);
		log.log(Level.INFO,
				"Sending app request {0} for name {1}",
				new Object[] { request.getClass().getSimpleName(),
						request.getServiceName() });
		getRandomClient()
				.sendRequest(
						(this.wrapReplicableClientRequest ? ReplicableClientRequest.wrap(request)
								: request), new RequestCallback() {
							@Override
							public void handleResponse(Request response) {
								if (outstandingQ != null)
									synchronized (outstandingQ) {
										if (!(response instanceof ActiveReplicaError))
											outstandingQ.remove(request
													.getRequestID());
										outstandingQ.notify();
									}
								DelayProfiler.updateDelay(appName + ".e2e", t);
								if (response instanceof ActiveReplicaError) {
									log.log(Level.INFO,
											"Received {0} for app request to name {1} in "
													+ "{2}ms; |outstanding|={3}",
											new Object[] {
													ActiveReplicaError.class
															.getSimpleName(),
													request.getServiceName(),
													(System.currentTimeMillis() - t),
													outstandingQ != null ? outstandingQ
															.size() : 0 });
								}
								if (response instanceof AppRequest) {
									log.log(Level.INFO,
											"Received response for app request to name {0} "
													+ "exists in {1}ms; |outstanding|={2}",
											new Object[] {
													request.getServiceName(),
													(System.currentTimeMillis() - t),
													outstandingQ != null ? outstandingQ
															.size() : 0 });
									String reqValue = ((AppRequest) response)
											.getValue();
									setNumReconfigurations(Integer
											.valueOf(reqValue.split(" ")[1]));
									success[0] = true;
								}
								monitorNotify(success[0]);
							}
						});
		if (outstandingQ == null)
			monitorWait(success, timeout);
		return outstandingQ != null || success[0];
	}

	private boolean testAppRequests(String[] names, int rounds)
			throws NumberFormatException, IOException {
		return this.testAppRequests(names, rounds, true);
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
	private boolean testAppRequests(String[] names, int rounds,
			boolean retryUntilSuccess) throws NumberFormatException,
			IOException {
		long t = System.currentTimeMillis();
		int numReconfigurationsBefore = numReconfigurations;
		boolean done = true;
		for (int i = 0; i < rounds; i++) {
			done = done && this.testAppRequests(names, retryUntilSuccess);
			log.log(Level.INFO, "Completed round {0} of {1} of app requests",
					new Object[] { i, rounds });
		}
		int delta = numReconfigurations - numReconfigurationsBefore;
		if (delta > 0)
			DelayProfiler.updateValue(
					ProfilerKeys.reconfiguration_rate.toString(),
					(delta * 1000) / (System.currentTimeMillis() - t));
		return done;
	}

	private void testAppRequests(Map<Long, Request> requests, RateLimiter r)
			throws NumberFormatException, IOException {
		for (Request request : requests.values())
			if (request instanceof AppRequest) {
				try {
					testAppRequest((AppRequest) request, requests, null);
				} catch (IOException ioe) {
					ioe.printStackTrace();
					throw ioe;
				}
				r.record();
			}
	}

	private boolean testAppRequests(String[] names, boolean retryUntilSuccess)
			throws NumberFormatException, IOException {
		RateLimiter rateLimiter = new RateLimiter(
				Config.getGlobalDouble(TRC.TEST_APP_REQUEST_RATE));
		final ConcurrentHashMap<Long, Request> outstanding = new ConcurrentHashMap<Long, Request>();

		for (int i = 0; i < names.length; i++) {
			// non-blocking
			this.testAppRequest(names[i], outstanding, null);
			rateLimiter.record();
		}
		waitForAppResponses(Config.getGlobalLong(TRC.TEST_APP_REQUEST_TIMEOUT),
				outstanding);
		if (retryUntilSuccess) {
			while (!outstanding.isEmpty()) {
				log.log(Level.INFO, "Retrying {0} outstanding app requests",
						new Object[] { outstanding.size() });
				testAppRequests(outstanding, rateLimiter);
				try {
					Thread.sleep(Config
							.getGlobalLong(TRC.TEST_APP_REQUEST_TIMEOUT));
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

	private boolean testNotExists(String name) throws IOException {
		return testExists(name, false);
	}

	private boolean testExists(String[] names, boolean exists)
			throws IOException {
		boolean retval = true;
		for (int i = 0; i < names.length; i++) {
			retval = retval && testExists(names[i], exists);
		}
		return retval;
	}

	private boolean testExists(String name) throws IOException {
		return testExists(name, true);
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
							DelayProfiler.updateDelay(
									ProfilerKeys.request_actives.toString(), t);
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
				&& (timeout == null || System.currentTimeMillis() - t < timeout*Config.getGlobalLong(TRC.TEST_RETRIES)));
		if (!success[0])
			log.log(Level.INFO, "testExists {0} failed after {1}ms", new Object[] {
					name, System.currentTimeMillis() - t });
		return success[0];
	}

	private boolean testBatchCreate(String[] names, int batchSize)
			throws IOException {
		Map<String, String> nameStates = new HashMap<String, String>();
		for (int i = 0; i < names.length; i++)
			nameStates.put(names[i], "some_initial_state" + i);
		boolean created = testBatchCreate(nameStates, batchSize);
		log.log(Level.FINE,
				"Finished batch-creating {0} names with head name {1}",
				new Object[] { names.length,
						nameStates.keySet().iterator().next() });
		return created;
	}

	private boolean testBatchCreate(Map<String, String> nameStates,
			int batchSize) throws IOException {
		if (SIMPLE_BATCH_CREATE)
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

	private static final boolean SIMPLE_BATCH_CREATE = true;

	private boolean testBatchCreateSimple(Map<String, String> nameStates,
			int batchSize) throws IOException {
		return testCreate(new CreateServiceName(nameStates));
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
									(System.currentTimeMillis() - t), response.getSummary() });
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

	private boolean testCreate(String name) throws IOException {
		return testCreate(name, generateRandomState());
	}

	// sequentially tests deletes of names
	private boolean testDeletes(String[] names) throws IOException,
			InterruptedException {
		boolean deleted = true;
		for (String name : names)
			deleted = deleted && this.testDelete(name);
		assert (deleted);
		return deleted;
	}

	private boolean testDelete(String name) throws IOException,
			InterruptedException {
		return testDelete(name, null);
	}

	// blocking delete until success or timeout of a single name
	private boolean testDelete(String name, Long timeout) throws IOException,
			InterruptedException {
		long t = System.currentTimeMillis();
		boolean[] success = new boolean[1];
		log.log(Level.INFO, "Sending delete request for name {0}",
				new Object[] { name });
		do {
			getRandomClient().sendRequest(new DeleteServiceName(name),
					new RequestCallback() {
						@Override
						public void handleResponse(Request response) {
							if (!(response instanceof DeleteServiceName))
								return;
							log.log(Level.INFO,
									"{0} name {1} in {2}ms {3}",
									new Object[] {
											!((DeleteServiceName) response)
													.isFailed() ? "Deleted"
													: "Failed to delete",
											name,
											(System.currentTimeMillis() - t),
											((DeleteServiceName) response)
													.isFailed() ? ((DeleteServiceName) response)
													.getResponseMessage() : "" });
							success[0] = !((DeleteServiceName) response)
									.isFailed();
							monitorNotify(success);
						}
					});
			monitorWait(success, timeout);
		} while (!success[0]
				&& (timeout == null || System.currentTimeMillis() - t < timeout));
		return success[0];
	}

	private void waitForAppResponses(long duration,
			Map<Long, Request> outstandingQ) {
		long t = System.currentTimeMillis(), remaining = duration;
		while (!outstandingQ.isEmpty()
				&& (remaining = duration - (System.currentTimeMillis() - t)) > 0)
			synchronized (outstandingQ) {
				try {
					outstandingQ.wait(remaining);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
	}

	private boolean testReconfigureReconfigurators(
			Map<String, InetSocketAddress> newlyAddedRCs,
			Set<String> deletedNodes, Long timeout) throws IOException {
		boolean[] success = new boolean[1];
		long t = System.currentTimeMillis();

		this.getRandomClient().sendRequest(
				new ReconfigureRCNodeConfig<String>(null, newlyAddedRCs,
						deletedNodes), new RequestCallback() {
					@Override
					public void handleResponse(Request response) {
						log.log(Level.INFO,
								"{0} node config in {1}ms: {2} ",
								new Object[] {
										((ServerReconfigurationPacket<?>) response)
												.isFailed() ? "Failed to reconfigure"
												: "Successfully reconfigured",
										(System.currentTimeMillis() - t),
										((ServerReconfigurationPacket<?>) response)
												.getSummary() });
						if (response instanceof ReconfigureRCNodeConfig
								&& !((ReconfigureRCNodeConfig<?>) response)
										.isFailed()) {
							success[0] = true;
						}
						monitorNotify(success);
					}
				});
		monitorWait(success, timeout);
		return success[0];

	}

	private boolean testReconfigureActives(
			Map<String, InetSocketAddress> newlyAddedActives,
			Set<String> deletes, Long timeout) throws IOException {
		boolean[] success = new boolean[1];
		long t = System.currentTimeMillis();

		this.getRandomClient().sendRequest(
				new ReconfigureActiveNodeConfig<String>(null,
						newlyAddedActives, deletes), new RequestCallback() {
					@Override
					public void handleResponse(Request response) {
						log.log(Level.INFO,
								"{0} node config in {1}ms: {2} ",
								new Object[] {
										((ServerReconfigurationPacket<?>) response)
												.isFailed() ? "Failed to reconfigure"
												: "Successfully reconfigured",
										(System.currentTimeMillis() - t),
										((ServerReconfigurationPacket<?>) response)
												.getSummary() });
						if (response instanceof ReconfigureActiveNodeConfig
								&& !((ReconfigureActiveNodeConfig<?>) response)
										.isFailed())
							success[0] = true;
						monitorNotify(success);
					}
				});
		monitorWait(success, timeout);
		return success[0];
	}

	/**
	 * 
	 */
	public void close() {
		if (clients != null)
			for (int i = 0; i < clients.length; i++)
				clients[i].close();
	}

	private static String generateRandomState() {
		return INITIAL_STATE + (long) (Math.random() * Long.MAX_VALUE);
	}

	private static String generateRandomName() {
		return generateRandomName(NAME);
	}

	private static String generateRandomName(String prefix) {
		return prefix + (long) (Math.random() * Long.MAX_VALUE);
	}

	private static String[] generateRandomNames(int n) {
		String[] names = new String[n];
		for (int i = 0; i < n; i++)
			names[i] = generateRandomName();
		return names;
	}


	private static final long DEFAULT_TIMEOUT = 1000;
	protected static final long DEFAULT_RTX_TIMEOUT = 2000;
	protected static final long DEFAULT_APP_REQUEST_TIMEOUT = 2000;

	/**
	 * Tests that a request to a random app name fails as expected.
	 * 
	 * @throws IOException
	 * @throws NumberFormatException
	 * @throws InterruptedException
	 */
	@Test(timeout = DEFAULT_TIMEOUT + DEFAULT_APP_REQUEST_TIMEOUT)
	public void test00_RandomNameAppRequestFails() throws IOException,
			NumberFormatException, InterruptedException {
		boolean test = testAppRequests(generateRandomNames(1), 1, false);
		Assert.assertEquals(test, false);
	}

	/**
	 * Tests that a random name does not exist.
	 * 
	 * @throws IOException
	 * @throws NumberFormatException
	 * @throws InterruptedException
	 */
	@Test(timeout = DEFAULT_TIMEOUT)
	public void test00_RandomNameNotExists() throws IOException,
			NumberFormatException, InterruptedException {
		boolean test = testNotExists(generateRandomNames(10));
		Assert.assertEquals(test, true);
	}

	/**
	 * @throws IOException
	 */
	@Test(timeout = DEFAULT_TIMEOUT)
	public void test00_RequestRandomActive() throws IOException {
		boolean test = testExists(Config.getGlobalString(RC.SPECIAL_NAME), true);
		Assert.assertEquals(test, true); // should pass
		;
	}

	/**
	 * @throws IOException
	 */
	@Test(timeout = DEFAULT_APP_REQUEST_TIMEOUT)
	public void test00_AnycastRequest() throws IOException {
		boolean test = testAppRequest(generateRandomName(), null,
				DEFAULT_TIMEOUT);
		Assert.assertEquals(test, false); // should fail
		;
	}

	/**
	 * Tests creation and deletion of a single name.
	 * 
	 * @throws IOException
	 * @throws NumberFormatException
	 * @throws InterruptedException
	 */
	@Test(timeout = 4 * DEFAULT_TIMEOUT)
	public void test00_CreateExistsDelete() throws IOException,
			NumberFormatException, InterruptedException {
		String[] names = generateRandomNames(1);
		boolean test = testNotExists(names) && testCreates(names)
				&& testDeletes(names);
		Assert.assertEquals(test, true);
		;
	}

	/**
	 * Tests that a delete of a random (non-existent) name fails.
	 * 
	 * @throws IOException
	 * @throws NumberFormatException
	 * @throws InterruptedException
	 */
	@Test(timeout = DEFAULT_TIMEOUT + DEFAULT_RTX_TIMEOUT)
	public void test00_RandomNameDeleteFails() throws IOException,
			NumberFormatException, InterruptedException {
		boolean test = testDelete(generateRandomName(), DEFAULT_TIMEOUT);
		Assert.assertEquals(test, false);
		;
	}

	/**
	 * Tests creation, existence, app requests, deletion, and non-existence of a
	 * set of names. Assumes that we start from a clean slate, i.e., none of the
	 * randomly generated names exists before the test.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws NumberFormatException
	 */
	@Test
	public void test01_BasicSequence() throws IOException,
			NumberFormatException, InterruptedException {
		String[] names = generateRandomNames(Config
				.getGlobalInt(TRC.TEST_NUM_APP_NAMES));
		DelayProfiler.clear();
		boolean test = testNotExists(names)
				&& testCreates(names)
				&& testExists(names)
				&& testAppRequests(names,
						Config.getGlobalInt(TRC.TEST_NUM_REQUESTS_PER_NAME))
				&& testDeletes(names) && testNotExists(names);
		log.log(Level.INFO,
				"{0} {1}",
				new Object[] {
						testName.getMethodName(),
						DelayProfiler.getStats(new HashSet<String>(Util
								.arrayOfObjectsToStringSet(ProfilerKeys
										.values()))) });
		Assert.assertEquals(true, test);
		;
	}

	private boolean wrapReplicableClientRequest = false;

	/**
	 * @throws IOException
	 * @throws NumberFormatException
	 * @throws InterruptedException
	 */
	@Test
	public void test02_ReplicableClientRequest() throws IOException,
			NumberFormatException, InterruptedException {
		String[] names = generateRandomNames(Config
				.getGlobalInt(TRC.TEST_NUM_APP_NAMES));
		this.wrapReplicableClientRequest = true;
		DelayProfiler.clear();
		boolean test = testNotExists(names)
				&& testCreates(names)
				&& testExists(names)
				&& testAppRequests(names,
						Config.getGlobalInt(TRC.TEST_NUM_REQUESTS_PER_NAME))
				&& testDeletes(names) && testNotExists(names);
		this.wrapReplicableClientRequest = false;
		log.log(Level.INFO,
				"{0} {1}",
				new Object[] {
						testName.getMethodName(),
						DelayProfiler.getStats(new HashSet<String>(Util
								.arrayOfObjectsToStringSet(ProfilerKeys
										.values()))) });
		Assert.assertEquals(test, true);
		;
	}

	/**
	 * Method to test {@link PacketType#APP_REQUEST3} that is a transaction.
	 * 
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test02_NonDefaultRequest() throws NumberFormatException,
			IOException, InterruptedException {
		String[] names = generateRandomNames(1);
		boolean test = testNotExists(names)
				&& testCreates(names)
				&& testExists(names)
				&& testAppRequestNonDefault(names[0],
						AppRequest.PacketType.APP_REQUEST3)
				&& testDeletes(names) && testNotExists(names);
		Assert.assertEquals(true, test);
		;
	}

	/**
	 * @throws IOException
	 * @throws NumberFormatException
	 * @throws InterruptedException
	 */
	@Test
	@Repeat(times = REPEAT)
	public void test02_MutualAuthRequest() throws IOException,
			NumberFormatException, InterruptedException {
		String name = generateRandomName();
		boolean testAppReq=false;
		boolean test = testNotExists(name)
				&& testCreate(name)
				&& testExists(name)
				&& (testAppReq=testAppRequest(getAppRequest(name,
						AppRequest.PacketType.ADMIN_APP_REQUEST)))
				&& testDelete(name) && testNotExists(name);
		Assert.assertEquals("testAppReq="+testAppReq, true, test);
		;
	}

	/**
	 * Blocking app request with default number of retries.
	 * 
	 * @param request
	 * @return
	 * @throws IOException
	 */
	private boolean testAppRequest(AppRequest request) throws IOException {
		return this.testAppRequest(request,
				Config.getGlobalInt(TRC.TEST_RETRIES));
	}

	/**
	 * Blocking app request with retries.
	 * 
	 * @param request
	 * @param retries
	 * @return True if response received within {@code retries} retransmissions.
	 * @throws IOException
	 */
	private boolean testAppRequest(AppRequest request, int retries)
			throws IOException {
		Request response = null;
		for (int i = 0; i <= retries && response==null; i++) {
			response = this.getRandomClient().sendRequest(request,
					Config.getGlobalLong(TRC.TEST_APP_REQUEST_TIMEOUT));
		}
		return response != null;
	}

	/**
	 * Same as {@link #test01_BasicSequence()} but with batch created names.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws NumberFormatException
	 */
	@Test
	public void test03_BasicSequenceBatched() throws IOException,
			NumberFormatException, InterruptedException {
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
		log.log(Level.INFO, "{0}: {1}", new Object[] {
				testName.getMethodName(), DelayProfiler.getStats() });
		Assert.assertEquals(test, true);
		;
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test04_ReconfigurationRate() throws IOException,
			InterruptedException {
		DelayProfiler.clear();
		String[] names = generateRandomNames(Math
				.max(Config
						.getGlobalInt(TRC.TEST_RECONFIGURATION_THROUGHPUT_NUM_APP_NAMES),
						Config.getGlobalInt(TRC.TEST_NUM_APP_NAMES)));
		log.log(Level.INFO,
				"Initiating reconfiguration throughput test with {0} names",
				new Object[] { names.length });
		long t = System.currentTimeMillis();
		int before = numReconfigurations;
		Assert.assertEquals(
				testBatchCreate(names, Config.getGlobalInt(TRC.TEST_BATCH_SIZE))
						&& testExists(names) && testAppRequests(names, 1), true);
		int delta = numReconfigurations - before;
		if (delta > 0) {
			Thread.sleep(1000);
			DelayProfiler.updateValue(
					ProfilerKeys.reconfiguration_rate.toString(),
					(delta * 1000) / (System.currentTimeMillis() - t));
			System.out.print(DelayProfiler.getStats(Util
					.arrayOfObjectsToStringSet(ProfilerKeys.values())) + " ");
			log.log(Level.INFO,
					"{0}: {1}",
					new Object[] {
							testName.getMethodName(),
							DelayProfiler.getStats(Util
									.arrayOfObjectsToStringSet(ProfilerKeys
											.values())) });
		}
		// some sleep needed to ensure reconfigurations settle down
		Thread.sleep(1000);
		Assert.assertEquals(testDeletes(names) && testNotExists(names), true);
		;
	}

	/**
	 * Deletion of a non-existent active replica succeeds.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test(timeout = DEFAULT_TIMEOUT)
	public void test20_RandomActiveReplicaDelete() throws IOException,
			InterruptedException {
		String name = null;
		Assert.assertEquals(
				testReconfigureActives(
						null,
						new HashSet<String>(Arrays
								.asList(name=generateRandomName(Config
										.getGlobalString(TRC.AR_PREFIX)))),
						null), true);
		
		System.out.print("[-" + name + "] ");
	}

	/**
	 * Addition of a random active replica succeeds (as it appears to other
	 * nodes as simply failed). Nothing bad will happen as long as a majority of
	 * replicas in each group is available.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test20_RandomActiveReplicaDeleteAfterAdd() throws IOException,
			InterruptedException {
		Map<String, InetSocketAddress> adds = new HashMap<String, InetSocketAddress>();
		adds.put(generateRandomName(Config.getGlobalString(TRC.AR_PREFIX)),
				new InetSocketAddress(61001));
		Assert.assertEquals(testReconfigureActives(adds, null, null), true);
		System.out.print("[+" + adds.keySet().iterator().next() + "] ");
		Assert.assertEquals(testReconfigureActives(null, adds.keySet(), null),
				true);
		System.out.print("[-" + adds.keySet().iterator().next() + "] ");
		;
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	@Test
	public void test21_DeleteActiveReplica() throws IOException,
			InterruptedException {
		String[] names = null;
		boolean test = testCreates(names = generateRandomNames(Config
				.getGlobalInt(TRC.TEST_NUM_APP_NAMES)));
		Map<String, InetSocketAddress> actives = TESTReconfigurationConfig
				.getLocalActives();
		Map<String, InetSocketAddress> deletes = new HashMap<String, InetSocketAddress>();
		deletes.put(actives.keySet().iterator().next(),
				actives.get(actives.keySet().iterator().next()));
		test = test
				&& this.testReconfigureActives(null, deletes.keySet(), null)
				&& this.testDeletes(names);
		if (test)
			// store this for subsequent addition
			justDeletedActives.putAll(deletes);
		Assert.assertEquals(true, true); // no-op
		System.out.print("[-" + Arrays.asList(deletes.keySet().iterator().next()) + "] ");		
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 */
	@Test
	public void test22_AddActiveReplica() throws IOException,
			InterruptedException {
		Map<String, InetSocketAddress> newlyAddedActives = new HashMap<String, InetSocketAddress>();

		if (!justDeletedActives.isEmpty()) {
			newlyAddedActives.putAll(justDeletedActives);
			boolean test = this.testReconfigureActives(newlyAddedActives, null,
					null);
			assert (test);
			Assert.assertEquals(test, true);
			justDeletedActives.clear();
			System.out.print("[+" + newlyAddedActives.keySet().iterator().next() + "] ");		
			Thread.sleep(1000);
		} else
			log.log(Level.INFO,
					"No active replicas added as none deleted in previous test step");
		;
	}

	/**
	 * @throws IOException
	 */
	@Test
	public void test31_AddReconfigurator() throws IOException {
		boolean test = testCreates(generateRandomNames(Config
				.getGlobalInt(TRC.TEST_NUM_APP_NAMES)));
		assert (test);
		Map<String, InetSocketAddress> newlyAddedRCs = new HashMap<String, InetSocketAddress>();
		newlyAddedRCs.put(
				Config.getGlobalString(TRC.RC_PREFIX)
						+ (int) (Math.random() * Short.MAX_VALUE),
				new InetSocketAddress(InetAddress.getByName("localhost"),
						Config.getGlobalInt(TRC.TEST_PORT)));
		test = test
				&& this.testReconfigureReconfigurators(newlyAddedRCs, null,
						null);
		/* Add reconfigurator may fail if a reconfigurator with the same name
		 * was previously deleted and it has not been long enough yet. */
		if (test)
			justAddedRCs.putAll(newlyAddedRCs);
		Assert.assertEquals(true, true); // no-op
		;
	}

	/**
	 * @throws IOException
	 * 
	 */
	@Test
	public void test32_DeleteReconfigurator() throws IOException {
		if (!justAddedRCs.isEmpty()) {
			boolean test = this.testReconfigureReconfigurators(null,
					justAddedRCs.keySet(), null);
			assert (test);
			justAddedRCs.clear();
			Assert.assertEquals(test, true);
		} else
			log.log(Level.INFO,
					"No reconfigurators deleted as none added in previous test step");
		;
	}

	private static Map<String, InetSocketAddress> justAddedRCs = new HashMap<String, InetSocketAddress>();
	private static Map<String, InetSocketAddress> justDeletedActives = new HashMap<String, InetSocketAddress>();

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void startServers() throws IOException, InterruptedException {
		TESTReconfigurationMain.startLocalServers();
		System.out
				.println("Reconfigurators and actives ready, initiating tests..\n");
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@AfterClass
	public static void closeServers() throws IOException, InterruptedException {
		Thread.sleep(1000);
		for (TESTReconfigurationClient client : allInstances)
			client.close();
		TESTReconfigurationMain.closeServers();
	}

	public String toString() {
		return TESTReconfigurationClient.class.getSimpleName();
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
		Config.getConfig(ReconfigurationConfig.RC.class).put(
				ReconfigurationConfig.RC.RECONFIGURE_IN_PLACE, true);
		// ReconfigurationConfig.setDemandProfile(ProximateBalance.class);

		Result result = JUnitCore.runClasses(TESTReconfigurationClient.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.toString());
			failure.getException().printStackTrace();
		}
	}
}
