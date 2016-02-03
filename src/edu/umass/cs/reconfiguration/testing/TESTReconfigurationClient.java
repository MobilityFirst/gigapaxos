package edu.umass.cs.reconfiguration.testing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
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
public class TESTReconfigurationClient {

	private static Logger log = Reconfigurator.getLogger();

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
	}

	private RCClient client = null;

	protected TESTReconfigurationClient() throws IOException {
		client = new RCClient(new HashSet<InetSocketAddress>(
				TESTReconfigurationConfig.getLocalReconfigurators().values()));
	}

	private String NAME = Config.getGlobalString(TRC.NAME_PREFIX);
	private String INITIAL_STATE = "some_initial_state";

	private static void monitorWait(Object monitor) {
		synchronized (monitor) {
			try {
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

	private void testAppRequest(String name) throws NumberFormatException,
			IOException {
		long t = System.currentTimeMillis();
		final ClientRequest request = new AppRequest(name, Long.valueOf(name
				.replaceAll("[a-z]*", "")), "request_value",
				AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
		this.outstanding.put(request.getRequestID(), request);
		client.sendRequest(request, new RequestCallback() {
			@Override
			public void handleResponse(Request response) {
				outstanding.remove(request.getRequestID());
				DelayProfiler.updateDelay("appRequest", t);
				log.log(Level.INFO,
						"Received response for app request to name {0} exists in {1}ms; |outstanding|={2}",
						new Object[] { name, (System.currentTimeMillis() - t),
								outstanding.size() });
			}
		});
	}

	private void testAppRequests(String[] names) throws NumberFormatException,
			IOException {
		RateLimiter r = new RateLimiter(10);
		for (int i = 0; i < names.length; i++) {
			this.testAppRequest(names[i]);
			r.record();
		}
	}

	private void testExists(String name) throws IOException {
		Object monitor = new Object();
		long t = System.currentTimeMillis();
		client.sendRequest(new RequestActiveReplicas(name),
				new RequestCallback() {

					@Override
					public void handleResponse(Request response) {
						DelayProfiler.updateDelay("requestActiveReplicas", t);
						log.log(Level.INFO,
								"Verified that name {0} exists in {1}ms",
								new Object[] { name,
										(System.currentTimeMillis() - t) });
						monitorNotify(monitor);
					}
				});
		monitorWait(monitor);
	}

	protected String testCreate() throws IOException {
		String name = NAME + (long) (Math.random() * Long.MAX_VALUE);
		long t = System.currentTimeMillis();
		Object monitor = new Object();
		client.sendRequest(new CreateServiceName(name, INITIAL_STATE),
				new RequestCallback() {

					@Override
					public void handleResponse(Request response) {
						try {
							if (response instanceof CreateServiceName)
								log.log(Level.INFO,
										"Created name {0} in {1}ms",
										new Object[] {
												name,
												(System.currentTimeMillis() - t) });
							monitorNotify(monitor);
							testExists(name);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
		monitorWait(monitor);
		return name;
	}

	private String[] testCreates(int numCreates) throws IOException {
		String[] names = new String[numCreates];
		for (int i = 0; i < numCreates; i++)
			names[i] = testCreate();
		return names;
	}

	private void waitForAppResponses(long duration) {
		if (!this.outstanding.isEmpty())
			synchronized (outstanding) {
				try {
					outstanding.wait(duration);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
	}

	protected void startTests() {
		try {
			long t = System.currentTimeMillis();
			int numRequests = 100;
			String[] names = testCreates(numRequests);
			testAppRequests(names);
			waitForAppResponses(5000);
			System.out.println("Number of missing responses = "
					+ outstanding.size() + "; reconfiguration_rate = "
					+ ((numRequests - outstanding.size()) * 1000)
					/ (System.currentTimeMillis() - t));
			System.out.println(DelayProfiler.getStats());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
