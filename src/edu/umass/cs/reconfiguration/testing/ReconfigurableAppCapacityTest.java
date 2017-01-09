package edu.umass.cs.reconfiguration.testing;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *         Tests the capacity of the GNS using a configurable number of async
 *         clients.
 *
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class ReconfigurableAppCapacityTest extends DefaultTest {

	private static ScheduledThreadPoolExecutor executor;

	private static RCClient[] clients;

	/**
	 * @throws Exception
	 */
	public ReconfigurableAppCapacityTest() throws Exception {
	}

	/**
	 * @throws Exception
	 */
	@BeforeClass
	public static void setup() throws Exception {
		initStaticParams();
		setupClientsAndGuids();
	}

	static class RCClient extends ReconfigurableAppClientAsync<Request> implements
			AppRequestParserBytes {

		public RCClient() throws IOException {
			super();
		}

		@Override
		public Request getRequest(String stringified)
				throws RequestParseException {
			try {
				return NoopAppTesting.staticGetRequest(stringified);
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

		@Override
		public Request getRequest(byte[] message, NIOHeader header)
				throws RequestParseException {
			return NoopAppTesting.staticGetRequest(message, header);
		}
	}

	private static int NUM_CLIENTS;
	private static String TEST_GUID_PREFIX;
	private static int NUM_GROUPS_CLIENT;
	private static String GIBBERISH;

	private static String[] names;

	// need a method like this to support command-line initialization
	private static void initStaticParams() {
		NUM_CLIENTS = Config.getGlobalInt(TC.NUM_CLIENTS);
		TEST_GUID_PREFIX = ReconfigurationConfig.application.getSimpleName(); // Config.getGlobalString(TC.TEST_GUID_PREFIX);
		NUM_GROUPS_CLIENT = Config.getGlobalInt(TC.NUM_GROUPS_CLIENT);
		GIBBERISH = new String(Util.getRandomAlphanumericBytes(Config
				.getGlobalInt(TC.REQUEST_BAGGAGE_SIZE)));
		names = new String[NUM_GROUPS_CLIENT];
		for (int i = 0; i < names.length; i++) {
			names[i] = genRandomName();
		}
	}

	private static void setupClientsAndGuids() throws Exception {
		clients = new RCClient[NUM_CLIENTS];
		executor = (ScheduledThreadPoolExecutor) Executors
				.newScheduledThreadPool(NUM_CLIENTS);
		for (int i = 0; i < NUM_CLIENTS; i++)
			clients[i] = new RCClient();

		boolean[] created = new boolean[NUM_GROUPS_CLIENT];
		// create names
		for (int i = 0; i < NUM_GROUPS_CLIENT; i++) {
			final int j = 0;
			clients[0].sendRequest(
					new CreateServiceName(names[i],
							"somestate"), new RequestCallback() {
						@Override
						public void handleResponse(Request response) {
							created[j] = true;
							synchronized (created) {
								created.notify();
							}
							assert (!((ClientReconfigurationPacket) response)
									.isFailed() || ((ClientReconfigurationPacket) response)
									.getResponseCode()
									.equals(ClientReconfigurationPacket.ResponseCodes.DUPLICATE_ERROR)) : response;
							// ignore as we can only get duplicate errors
						}
					});
		}
		synchronized (created) {
			for (int i = 0; i < created.length; i++) {
				if (!created[i]) {
					created.wait();
					continue;
				}
			}
			// all done
		}
		System.out.println("Created " + NUM_GROUPS_CLIENT
				+ " names with prefix " + TEST_GUID_PREFIX);
	}

	private static String genRandomName() {
		return TEST_GUID_PREFIX + (long) (Math.random() * Long.MAX_VALUE);
	}

	/**
	 * Verifies a single write is successful.
	 * 
	 * @throws IOException
	 */
	@Test
	public void test_01_SingleWrite() throws IOException {
		this.blockingRead(names[0], clients[0]);
	}

	private void blockingRead(String name, RCClient client) throws IOException {
		this.blockingRead(name, client, true);
	}

	private void blockingRead(String name, RCClient client, boolean block)
			throws IOException {
		Runnable task = (new Runnable() {
			public void run() {

				boolean[] success = new boolean[1];
				try {
					client.sendRequest(new MetaRequestPacket(name, GIBBERISH),
							new RequestCallback() {

								@Override
								public void handleResponse(Request response) {
									synchronized (success) {
										success[0] = true;
										success.notify();
									}
//									System.out.println(response);
								}
							});
					synchronized (success) {
						while (!success[0])
							success.wait();
					}
					incrFinishedReads();
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		if (block)
			task.run();
		else
			executor.submit(task);
	}

	/**
	 * @throws Exception
	 */
	@Test
	public void test_02_SequentialReadCapacity() throws Exception {
		int numReads = Math.min(1000, Config.getGlobalInt(TC.NUM_REQUESTS));
		long t = System.currentTimeMillis();
		for (int i = 0; i < numReads; i++)
			this.blockingRead(names[0], clients[0]);
		System.out.print("sequential_read_rate="
				+ Util.df(numReads * 1.0 / (System.currentTimeMillis() - t))
				+ "K/s averaged over " + numReads + " reads.");
	}

	private static int numFinishedReads = 0;
	private static long lastReadFinishedTime = System.currentTimeMillis();

	synchronized static void incrFinishedReads() {
		numFinishedReads++;
		lastReadFinishedTime = System.currentTimeMillis();
	}

	private void reset() {
		numFinishedReads = 0;
		lastReadFinishedTime = System.currentTimeMillis();
	}

	/**
	 * @throws Exception
	 */
	 @Test
	public void test_04_ParallelReadCapacity() throws Exception {
		for (int k = 0; k < 5; k++) {
			int numReads = Config.getGlobalInt(TC.NUM_REQUESTS);
			reset();
			long t = System.currentTimeMillis();
			for (int i = 0; i < numReads; i++) {
				blockingRead(names[0], clients[numReads % NUM_CLIENTS], false);
			}
			int j = 1;
			System.out.print("[total_reads=" + numReads + ": ");
			while (numFinishedReads < numReads) {
				if (numFinishedReads >= j) {
					j *= 2;
					System.out.print(numFinishedReads + " ");
				}
				Thread.sleep(500);
			}
			System.out.print("] ");
			System.out.print("parallel_read_rate="
					+ Util.df(numReads * 1.0 / (lastReadFinishedTime - t))
					+ "K/s");
		}
	}

	/**
	 * Removes all account and sub-guids created during the test.
	 * 
	 * @throws Exception
	 */
	@AfterClass
	public static void cleanup() throws Exception {
		System.out.print("Cleaning up");
		boolean[] created = new boolean[NUM_GROUPS_CLIENT];
		for (int i = 0; i < NUM_GROUPS_CLIENT; i++) {
			final int j = i;
			clients[0].sendRequest(new DeleteServiceName(TEST_GUID_PREFIX + i),
					new RequestCallback() {

						@Override
						public void handleResponse(Request response) {
							created[j] = true;
							synchronized (created) {
								created.notify();
							}
						}
					});
		}
		synchronized (created) {
			for (int i = 0; i < created.length; i++) {
				if (!created[i]) {
					created.wait();
					continue;
				}
			}
			// all done
		}
		System.out.println(".. all cleaned up");
		executor.shutdown();
		for (RCClient client : clients)
			client.close();
		System.out.println(DelayProfiler.getStats());
	}

	private static void processArgs(String[] args) throws IOException {
		Config.register(args);
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Util.assertAssertionsEnabled();
		processArgs(args);
		Result result = JUnitCore
				.runClasses(ReconfigurableAppCapacityTest.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
	}
}
