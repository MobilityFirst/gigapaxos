package edu.umass.cs.gigapaxos.examples.noop;

import edu.umass.cs.gigapaxos.PaxosClientAsync;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import org.json.JSONException;

import java.io.IOException;

/**
 * @author arun
 * 
 *         A simple client for NoopApp.
 */
public class NoopPaxosAppClient extends PaxosClientAsync {

	/**
	 * @throws IOException
	 */
	public NoopPaxosAppClient() throws IOException {
		super();
	}

	/**
	 * A simple example of asynchronously sending a few requests with a callback
	 * method that is invoked when the request has been executed or is known to
	 * have failed.
	 * 
	 * @param args
	 * @throws IOException
	 * @throws JSONException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, JSONException, InterruptedException {
		NoopPaxosAppClient noopClient = new NoopPaxosAppClient();
		for (int i = 0; i < 100; i++) {
			final String requestValue = "hello world" + i;
			noopClient.sendRequest(PaxosConfig.getDefaultServiceName(),
					requestValue, new RequestCallback() {
				long createTime = System.currentTimeMillis();
				@Override
				public void handleResponse(Request response) {
					System.out.println("Response for request ["
									+ requestValue
									+ "] = "
									+ ((RequestPacket)response).getResponseValue()
									+ " received in "
									+ (System.currentTimeMillis() - createTime)
									+ "ms");
					}
				});
			Thread.sleep(100);
		}
		noopClient.close();
	}
}
