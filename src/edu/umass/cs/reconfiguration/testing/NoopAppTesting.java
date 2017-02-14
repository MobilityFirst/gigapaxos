/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun */
package edu.umass.cs.reconfiguration.testing;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.AppRequest.ResponseCodes;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;

/**
 * @author V. Arun
 * 
 *         A simple no-op app for testing.
 */
public class NoopAppTesting extends AbstractReconfigurablePaxosApp<String>
		implements Replicable, Reconfigurable, ClientMessenger,
		AppRequestParserBytes {

	private static final String DEFAULT_INIT_STATE = "";
	// total number of reconfigurations across all records
	private int numReconfigurationsSinceRecovery = -1;
	private boolean verbose = false;

	private class AppData {
		final String name;
		String state = DEFAULT_INIT_STATE;

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

	private String myID; // used only for pretty printing
	private final HashMap<String, AppData> appData = new HashMap<String, AppData>();
	// only address based communication needed in app
	private SSLMessenger<?, JSONObject> messenger;

	/**
	 * Constructor used to create app replica via reflection. A reconfigurable
	 * app must support a constructor with a single String[] as an argument.
	 * 
	 * @param args
	 */
	public NoopAppTesting(String[] args) {
	}

	// Need a messenger mainly to send back responses to the client.
	@Override
	public void setClientMessenger(SSLMessenger<?, JSONObject> msgr) {
		this.messenger = msgr;
		this.myID = msgr.getMyID().toString();
	}

	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) {
		assert (request instanceof AppRequest || request instanceof RequestPacket) : request.getSummary();
		IntegerPacketType type = request.getRequestType();
		if(type instanceof AppRequest.PacketType)
		switch ((AppRequest.PacketType) (request.getRequestType())) {
		case DEFAULT_APP_REQUEST:
			return processRequest((AppRequest) request, doNotReplyToClient);
		case META_REQUEST_PACKET:
			// noop
			((MetaRequestPacket) request).setResponse(RequestPacket.ResponseCodes.ACK.toString());
			return true;
		default:
			break;
		}
		else if(type == PaxosPacket.PaxosPacketType.PAXOS_PACKET)
			((RequestPacket)request).setResponse(RequestPacket.ResponseCodes.ACK.toString());
		return false;
	}

	private static final boolean DELEGATE_RESPONSE_MESSAGING = true;

	private boolean processRequest(AppRequest request,
			boolean doNotReplyToClient) {
		if (request.getServiceName() == null)
			return true; // no-op
		if (request.isStop())
			return processStopRequest(request);
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
		if (DELEGATE_RESPONSE_MESSAGING)
			this.sendResponse(request);
		else
			sendResponse(request, doNotReplyToClient);
		return true;
	}

	/**
	 * This method exemplifies one way of sending responses back to the client.
	 * A cleaner way of sending a simple, single-message response back to the
	 * client is to delegate it to the replica coordinator, as exemplified below
	 * in {@link #sendResponse(AppRequest)} and supported by gigapaxos.
	 * 
	 * @param request
	 * @param doNotReplyToClient
	 */
	private void sendResponse(AppRequest request, boolean doNotReplyToClient) {
		assert (this.messenger != null && this.messenger.getClientMessenger() != null);
		if (this.messenger == null || doNotReplyToClient)
			return;

		InetSocketAddress sockAddr = request.getSenderAddress();
		try {
			this.messenger.getClientMessenger().sendToAddress(sockAddr,
					request.toJSONObject());
		} catch (JSONException | IOException e) {
			e.printStackTrace();
		}
	}

	private void sendResponse(AppRequest request) {
		// set to whatever response value is appropriate
		request.setResponse(ResponseCodes.ACK.toString() + " "
				+ numReconfigurationsSinceRecovery);
	}

	// no-op
	private boolean processStopRequest(AppRequest request) {
		return true;
	}

	@Override
	public Request getRequest(String stringified) throws RequestParseException {
		try {
			return staticGetRequest(stringified);
		} catch (JSONException je) {
			ReconfigurationConfig.getLogger().fine(
					"App-" + myID + " unable to parse request " + stringified);
			throw new RequestParseException(je);
		}
	}

	private static final boolean BYTEIFICATION = Config.getGlobalBoolean(PC.BYTEIFICATION);
	/**
	 * We use this method also at the client, so it is static.
	 * 
	 * @param stringified
	 * @return App request
	 * @throws RequestParseException
	 * @throws JSONException
	 */
	public static Request staticGetRequest(String stringified)
			throws RequestParseException, JSONException {
		if (stringified.equals(Request.NO_OP)) 
			return getNoopRequest();
		
		JSONObject json = new JSONObject(stringified);
		if (JSONPacket.getPacketType(json) == AppRequest.PacketType.META_REQUEST_PACKET
				.getInt())
			return new MetaRequestPacket(json);

		return new AppRequest(new JSONObject(stringified));
	}

	@Override
	public Request getRequest(byte[] bytes, NIOHeader header)
			throws RequestParseException {
		return staticGetRequest(bytes, header);
	}

	/**
	 * @param bytes
	 * @param header
	 * @return Request constructed from bytes.
	 * @throws RequestParseException
	 */
	public static Request staticGetRequest(byte[] bytes, NIOHeader header)
			throws RequestParseException {
		try {
			if (BYTEIFICATION && ByteBuffer.wrap(bytes).getInt() == AppRequest.PacketType.META_REQUEST_PACKET.getInt()) {
				return MetaRequestPacket.getMetaRequestPacket(bytes);
			}
			return staticGetRequest(new String(bytes, NIOHeader.CHARSET));
		} catch (UnsupportedEncodingException | JSONException
				| UnknownHostException e) {
			throw new RequestParseException(e);
		}
	}

	/* This is a special no-op request unlike any other NoopAppRequest. */
	private static Request getNoopRequest() {
		return new AppRequest(null, 0, 0, Request.NO_OP,
				AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
	}

	private static AppRequest.PacketType[] types = {
			AppRequest.PacketType.DEFAULT_APP_REQUEST,
			AppRequest.PacketType.ANOTHER_APP_REQUEST,
			AppRequest.PacketType.META_REQUEST_PACKET };

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return staticGetRequestTypes();
	}

	/**
	 * We use this method also at the client, so it is static.
	 * 
	 * @return App request types.
	 */
	public static Set<IntegerPacketType> staticGetRequestTypes() {
		return new HashSet<IntegerPacketType>(Arrays.asList(types));
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
			numReconfigurationsSinceRecovery++;
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
		} else
			// do nothing when data==null && state==null
			;
		if (state != null)
			this.appData.put(name, data);

		return true;
	}

	public String toString() {
		return NoopAppTesting.class.getSimpleName();
	}
}
