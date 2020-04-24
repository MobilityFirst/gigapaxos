package edu.umass.cs.reconfiguration.http;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.examples.AppRequest.ResponseCodes;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author gaozy
 *
 * A no-op example app for HttpActiveReplica
 */
public class HttpActiveReplicaTestApp extends AbstractReconfigurablePaxosApp<String>
	implements Replicable, Reconfigurable, ClientMessenger, AppRequestParserBytes {
	
	private boolean verbose = false;
	
	private String myID; // used only for pretty printing
	private final HashMap<String, AppData> appData = new HashMap<String, AppData>();
	
	/**
	 * 
	 */
	public HttpActiveReplicaTestApp(){		
	}
	
	private class AppData {
		final String name;
		String state = "";

		AppData(String name, String state) {
			this.name = name;
			this.state = state;
		}

		void setState(String state) {
			this.state = state;
		}

		String getState() {
			return this.state;
		}
	}
	
	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) {
		if (request.toString().equals(Request.NO_OP))
			return true;
		if (request instanceof ReplicableClientRequest)
			return true;
		return processRequest((HttpActiveReplicaRequest) request, doNotReplyToClient);
	}
	
	private boolean processRequest(HttpActiveReplicaRequest request,
			boolean doNotReplyToClient) {
		if (request.getServiceName() == null)
			return true; // no-op
		if (request.isStop())
			return true;
		AppData data = this.appData.get(request.getServiceName());
		if (data == null) {
			System.out.println("App-" + myID + " has no record for "
					+ request.getServiceName() + " for " + request);
			assert (request.getResponse() == null);
			return false;
		}
		assert (data != null);
		data.setState(request.getValue());
		this.appData.put(request.getServiceName(), data);
		if (verbose)
			System.out.println("App-" + myID + " wrote to " + data.name
					+ " with state " + data.getState());
		
		request.setResponse(ResponseCodes.ACK.toString());
		
		return true;
	}

	
	@Override
	public boolean execute(Request request) {
		return this.execute(request, false);
	}


	@Override
	public String checkpoint(String name) {
		AppData data = this.appData.get(name);
		return data != null ? data.getState() : null;
	}

	@Override
	public boolean restore(String name, String state) {
		AppData data = this.appData.get(name);

		// if no previous state, this is a creation epoch.
		if (data == null && state != null) {
			data = new AppData(name, state);
			if (verbose)
				System.out.println(">>>App-" + myID + " creating " + name
						+ " with state " + state);
		}
		// if state==null => end of epoch
		else if (state == null) {
			if (data != null)
				if (verbose)
					System.out.println("App-" + myID + " deleting " + name
							+ " with final state " + data.state);
			this.appData.remove(name);
			assert (this.appData.get(name) == null);
		} 
		// typical reconfiguration or epoch change
		else if (data != null && state != null) {
			System.out.println("App-" + myID + " updating " + name
					+ " with state " + state);
			data.state = state;
		} 
		else
			// do nothing when data==null && state==null
			;
		if (state != null)
			this.appData.put(name, data);

		return true;
	}

    @Override
    public Request getRequest(String s) throws RequestParseException {
        try {
            return new HttpActiveReplicaRequest(new JSONObject(s));
        } catch (JSONException e) {
            throw new RequestParseException(e);
        }
    }

    private static HttpActiveReplicaPacketType[] types = HttpActiveReplicaPacketType.values();

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>(Arrays.asList(types));
    }


	@Override
	public Request getRequest(byte[] bytes, NIOHeader header) throws RequestParseException {
		try {
            return new HttpActiveReplicaRequest(new JSONObject(new String(bytes, NIOHeader.CHARSET)));
        } catch (JSONException | UnsupportedEncodingException e) {
            throw new RequestParseException(e);
        }
	}


	@Override
	public void setClientMessenger(SSLMessenger<?, JSONObject> messenger) {
		// no need to use a messenger to send back response to client
		this.myID = messenger.getMyID().toString();
	}
}
