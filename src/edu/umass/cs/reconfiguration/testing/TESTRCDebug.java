package edu.umass.cs.reconfiguration.testing;

import java.io.IOException;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.examples.AppRequest;
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
import edu.umass.cs.utils.DelayProfiler;

/**
 * @author arun
 * 
 *         This is a temporary class for testing a specific pattern of
 *         name creation.
 */
@Deprecated
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class TESTRCDebug {

	private static Logger log = ReconfigurationConfig.getLogger();

	private static enum ProfilerKeys {
		reconfiguration_rate, request_actives, app_request, create, delete
	};

	private static Set<TESTRCDebug> allInstances = new HashSet<TESTRCDebug>();

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
	public TESTRCDebug() throws IOException {
		this(loopbackMode ? TESTReconfigurationConfig.getLocalReconfigurators()
				: ReconfigurationConfig.getReconfigurators());
	}

	protected TESTRCDebug(
			Map<String, InetSocketAddress> reconfigurators) throws IOException {
		allInstances.add(this);
		clients = new RCClient[Config.getGlobalInt(TRC.NUM_CLIENTS)];
		for (int i = 0; i < clients.length; i++)
			clients[i] = new RCClient(new HashSet<InetSocketAddress>(
					reconfigurators.values()));
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

	private boolean testAppRequest(String name, Map<Long, Request> outstandingQ, Long timeout)
			throws NumberFormatException, IOException {
		return this.testAppRequest(
				new AppRequest(name,
						//Long.valueOf(name.replaceAll("[a-z]*", "")),
						(long)(Math.random()*Long.MAX_VALUE),
						"request_value",
						AppRequest.PacketType.DEFAULT_APP_REQUEST, false),
				outstandingQ, timeout);
	}

	private static final String appName = ReconfigurationConfig.application.getSimpleName();
	
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
			Map<Long, Request> outstandingQ, Long timeout) throws NumberFormatException,
			IOException {
		long t = System.currentTimeMillis();
		boolean[] success = new boolean[1];
		if (outstandingQ != null)
			outstandingQ.put(request.getRequestID(), request);
		log.log(Level.INFO,
				"Sending app request {0} for name {1}",
				new Object[] { request.getClass().getSimpleName(),
						request.getServiceName() });
		getRandomClient().sendRequest((this.wrapReplicableClientRequest ? ReplicableClientRequest.wrap(request) : request), new RequestCallback() {
			@Override
			public void handleResponse(Request response) {
				if (outstandingQ != null)
					synchronized (outstandingQ) {
						if (!(response instanceof ActiveReplicaError))
							outstandingQ.remove(request.getRequestID());
						outstandingQ.notify();
					}
				DelayProfiler.updateDelay(appName+".e2e",
						t);
				if (response instanceof ActiveReplicaError) {
					log.log(Level.INFO,
							"Received {0} for app request to name {1} in "
									+ "{2}ms; |outstanding|={3}",
							new Object[] {
									ActiveReplicaError.class.getSimpleName(),
									request.getServiceName(),
									(System.currentTimeMillis() - t),
									outstandingQ != null ? outstandingQ.size()
											: 0 });
				}
				if (response instanceof AppRequest) {
					log.log(Level.INFO,
							"Received response for app request to name {0} "
									+ "exists in {1}ms; |outstanding|={2}",
							new Object[] {
									request.getServiceName(),
									(System.currentTimeMillis() - t),
									outstandingQ != null ? outstandingQ.size()
											: 0 });
					String reqValue = ((AppRequest) response).getValue();
					setNumReconfigurations(Integer.valueOf(reqValue.split(" ")[1]));
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
				} catch(IOException ioe) {
					System.out.println(this+"--------------------"+request);
					ioe.printStackTrace();
					throw ioe;
				}
				r.record();
			}
	}

	private boolean testAppRequests(String[] names, boolean retryUntilSuccess)
			throws NumberFormatException, IOException {
		RateLimiter r = new RateLimiter(
				Config.getGlobalDouble(TRC.TEST_APP_REQUEST_RATE));
		final ConcurrentHashMap<Long, Request> outstanding = new ConcurrentHashMap<Long, Request>();

		for (int i = 0; i < names.length; i++) {
			// non-blocking
			this.testAppRequest(names[i], outstanding, null);
			r.record();
		}
		waitForAppResponses(Config.getGlobalLong(TRC.TEST_APP_REQUEST_TIMEOUT),
				outstanding);
		if (retryUntilSuccess) {
			while (!outstanding.isEmpty()) {
				log.log(Level.INFO, "Retrying {0} outstanding app requests",
						new Object[] { outstanding.size() });
				testAppRequests(outstanding, r);
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

	@SuppressWarnings("unused")
	private boolean testExists(String[] names) throws IOException {
		boolean exists = testExists(names, true);
		assert (exists);
		return exists;
	}

	@SuppressWarnings("unused")
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
				&& (timeout == null || System.currentTimeMillis() - t < timeout));
		if (!success[0])
			log.log(Level.INFO, "testExists failed after {1}ms", new Object[] {
					success[0], System.currentTimeMillis() - t });
		return success[0];
	}

	@SuppressWarnings("unused")
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
			if (!success[0])
				Thread.sleep(Config.getGlobalLong(TRC.TEST_RTX_TIMEOUT));
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

	@SuppressWarnings("unused")
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

	@SuppressWarnings("unused")
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
		for (int i = 0; i < this.clients.length; i++)
			this.clients[i].close();
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

	/**
	 * 
	 */
	@Rule
	public TestName testName = new TestName();

	private static TestName prevTestName = null;

	/**
	 * 
	 */
	@Before
	public void beforeTestMethod() {
		System.out.print((prevTestName != null
				&& !testCategory(prevTestName.getMethodName()).equals(
						testCategory(testName.getMethodName())) ? "\n" : "")
				+ testName.getMethodName() + " ");
	}

	private static String testCategory(String methodName) {
		return methodName.split("_")[0].replace("test", "").substring(0, 1);
	}

	/**
	 * 
	 */
	@After
	public void afterTestMethod() {
		System.out.println(succeeded() ? "succeeded" : "FAILED");
		prevTestName = testName;
	}

	protected static final long DEFAULT_RTX_TIMEOUT = 2000;
	protected static final long DEFAULT_APP_REQUEST_TIMEOUT = 2000;

	/**
	 * Specifically for testing the creation of four names and an update to the
	 * first.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test000_debug() throws IOException, InterruptedException {
		String[] names = generateRandomNames(4);
		String[] name = Arrays.copyOf(names, 1);
		Assert.assertEquals(true, testCreates(names)
				&& testAppRequests(name, 1) && testDeletes(names));
		success();
	}	
	
	private boolean wrapReplicableClientRequest = false;



	private boolean testSuccess = false;

	private void success() {
		this.testSuccess = true;
	}

	private boolean succeeded() {
		return this.testSuccess;
	}

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
		for (TESTRCDebug client : allInstances)
			client.close();
		//TESTReconfigurationMain.closeServers();
		TESTReconfigurationMain.wipeoutServers();
	}

	public String toString() {
		return TESTRCDebug.class.getSimpleName();
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
		//ReconfigurationConfig.setDemandProfile(ProximateBalance.class);

		Result result = JUnitCore.runClasses(TESTRCDebug.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.toString());
			failure.getException().printStackTrace();
		}
	}
}
