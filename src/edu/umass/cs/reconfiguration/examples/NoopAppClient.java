package edu.umass.cs.reconfiguration.examples;

import java.io.IOException;
import java.util.Set;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author arun
 *
 */
public class NoopAppClient extends ReconfigurableAppClientAsync {

	/**
	 * @throws IOException
	 */
	public NoopAppClient() throws IOException {
		super();
	}

	private void testSendBunchOfRequests(String name, int numRequests)
			throws IOException, JSONException {
		System.out.println("Created " + name
				+ " and beginning to send test requests");
		for (int i = 0; i < numRequests; i++) {
			this.sendRequest(new AppRequest(name, "request_value" + i,
					AppRequest.PacketType.DEFAULT_APP_REQUEST, false),
					new RequestCallback() {

						@Override
						public void handleResponse(Request response) {
							System.out
									.println("Received response: " + response);
						}
					});
			try {
				Thread.sleep(50);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
			System.out.println("Creating name");
			client.sendRequest(new CreateServiceName(name, initialState),
					new RequestCallback() {

						@Override
						public void handleResponse(Request response) {
							try {
								client.testSendBunchOfRequests(name, numReqs);
							} catch (IOException | JSONException e) {
								e.printStackTrace();
							}
						}
					});
		}
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
	public Set<IntegerPacketType> getRequestTypes() {
		return NoopApp.staticGetRequestTypes();
	}
}
