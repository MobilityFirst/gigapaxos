package edu.umass.cs.reconfiguration.examples;

import java.io.IOException;
import java.util.Set;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author arun
 *
 */
public class NoopAppClient extends ReconfigurableAppClientAsync<Request> implements AppRequestParserBytes{

	private int numResponses = 0;

	/**
	 * @throws IOException
	 */
	public NoopAppClient() throws IOException {
		super();
	}
	
	private synchronized void incrNumResponses() {
		this.numResponses++;
	}

	private static final long INTER_REQUEST_TIME = 50;

	private void testSendBunchOfRequests(String name, int numRequests)
			throws IOException {
		System.out.println("Created " + name
				+ " and beginning to send test requests");
		new Thread(new Runnable() {
			public void run() {
				for (int i = 0; i < numRequests; i++) {
					long reqInitime = System.currentTimeMillis();
					try {
						NoopAppClient.this.sendRequest(ReplicableClientRequest.wrap(new AppRequest(name,
								"request_value" + i,
								AppRequest.PacketType.DEFAULT_APP_REQUEST,
								false)), new RequestCallback() {

							@Override
							public void handleResponse(Request response) {
								if (response instanceof ActiveReplicaError)
									return;
								// else
								System.out.println("Received response: "
										+ response
										+ "  ["
										+ (System.currentTimeMillis() - reqInitime)
										+ "ms]");
								synchronized (NoopAppClient.this) {
									NoopAppClient.this.incrNumResponses();;
									NoopAppClient.this.notify();
								}
							}
						});
						Thread.sleep(INTER_REQUEST_TIME);
					} catch (IOException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}).start();
	}

	@Override
	public Request getRequest(String stringified) {
		try {
			return NoopApp.staticGetRequest(stringified);
		} catch (RequestParseException | JSONException e) {
			// do nothing by design
		}
		return null;
	}

	@Override
	public Request getRequest(byte[] message, NIOHeader header)
			throws RequestParseException {
		return NoopApp.staticGetRequest(message, header);
	}


	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return NoopApp.staticGetRequestTypes();
	}

	/**
	 * This simple client creates a bunch of names and sends a bunch of requests
	 * to each of them. Refer to the parent class
	 * {@link ReconfigurableAppClientAsync} for other utility methods available
	 * to this method or to know how to write your own client.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		final NoopAppClient client = new NoopAppClient();
		final int numNames = 10;
		final int numReqs = 20;
		String namePrefix = "some_name";
		String initialState = "some_default_initial_state";

		for (int i = 0; i < numNames; i++) {
			final String name = namePrefix
					+ ((int) (Math.random() * Integer.MAX_VALUE));
			System.out.println("Creating name " + name);
			client.sendRequest(new CreateServiceName(name, initialState),
					new RequestCallback() {

						@Override
						public void handleResponse(Request response) {
							try {
								if (response instanceof CreateServiceName
										&& !((CreateServiceName) response)
												.isFailed())
									client.testSendBunchOfRequests(name,
											numReqs);
								else
									System.out.println(this
											+ " failed to create name " + name);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					});
		}
		synchronized (client) {
			long maxWaitTime = numReqs * INTER_REQUEST_TIME + 4000, waitInitTime = System
					.currentTimeMillis();
			while (client.numResponses < numNames * numReqs
					&& (System.currentTimeMillis() - waitInitTime < maxWaitTime))
				try {
					client.wait(maxWaitTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
		System.out.println("\n" + client + " successfully created " + numNames
				+ " names, sent " + numNames * numReqs
				+ " requests, and received " + client.numResponses
				+ " responses.");
		client.close();
	}

}
