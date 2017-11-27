package edu.umass.cs.reconfiguration.examples.linwrites;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author arun
 *
 *         A simple client for {@link LinWritesLocReadsApp}.
 */
public class LinWritesLocReadsAppClient extends
		ReconfigurableAppClientAsync<SimpleAppRequest> {

	/**
	 * @throws IOException
	 */
	public LinWritesLocReadsAppClient() throws IOException {
		super();
	}

	private static long getRandomLong() {
		return (long)(Math.random()*Long.MAX_VALUE);
	}

	private static SimpleAppRequest makeReadRequest() {
		return new SimpleAppRequest(PaxosConfig.getDefaultServiceName(),
				"", // empty request value
				SimpleAppRequest.PacketType.LOCAL_READ);
	}

	private static SimpleAppRequest makeWriteRequest(int value) {
		return new SimpleAppRequest(PaxosConfig.getDefaultServiceName(),
				value+"", // string representation of value
				SimpleAppRequest.PacketType.COORDINATED_WRITE);
	}
	private static SimpleAppRequest makeWriteRequest() {
		return makeWriteRequest((int)(Math.random()*Integer.MAX_VALUE));
	}


	@Override
	public Request getRequest(String stringified) throws RequestParseException {
		try {
			return new SimpleAppRequest(new JSONObject(stringified));
		} catch (JSONException je) {
			throw new RequestParseException(je);
		}
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>(Arrays.asList(SimpleAppRequest
				.PacketType.values()));
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
	public static void main(String[] args) throws IOException, InterruptedException {
		LinWritesLocReadsAppClient asyncClient = new
				LinWritesLocReadsAppClient();
		int numRequests = args.length>=1 ? Integer.valueOf(args[0]) : 100;
		for (int i = 0; i < numRequests; i++) {
			final String requestValue = (int)(Math.random()*Integer.MAX_VALUE)+"";
			SimpleAppRequest request;
			final int j=i;
			asyncClient.sendRequest(

					// even numbered requests are writes, odd-numbered reads
					request = i%2==0 ? makeReadRequest() :
							makeWriteRequest(),

					// to redirect request to specific active replica
					//new InetSocketAddress("localhost", 8001),

					new Callback<Request, SimpleAppRequest>() {
						long createTime = System.currentTimeMillis();
						@Override
						public SimpleAppRequest processResponse(Request response) {
							assert(response instanceof SimpleAppRequest) :
									response.getSummary();
							System.out
									.println(j+": Response for request ["
											+ request.getSummary()
											+ "] = "
											+ ((SimpleAppRequest)response).getValue()
											+ " received in "
											+ (System.currentTimeMillis() - createTime)
											+ "ms");
							return (SimpleAppRequest) response;
						}
					});
			Thread.sleep(100);
		}
		asyncClient.close();
	}

}
