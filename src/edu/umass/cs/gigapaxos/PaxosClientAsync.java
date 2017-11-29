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

package edu.umass.cs.gigapaxos;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.PaxosPacketDemultiplexerFast;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;

/**
 * @author arun
 *
 *         This class is meant to only send and receive {@link RequestPacket}
 *         requests. To use other app-specific request types, use
 *         ReconfigurableAppClientAsync.
 */
public class PaxosClientAsync {

	private static final long DEFAULT_TIMEOUT = 8000;
	protected final InetSocketAddress[] servers;
	private final MessageNIOTransport<String, JSONObject> niot;
	private final GCConcurrentHashMap<Long, RequestCallback> callbacks = new GCConcurrentHashMap<Long, RequestCallback>(
			new GCConcurrentHashMapCallback() {
				@Override
				public void callbackGC(Object key, Object value) {
					System.out.println("Request " + key + " timed out");
				}

			}, DEFAULT_TIMEOUT);
	private RequestCallback defaultCallback = null;

	class ClientPacketDemultiplexer extends
			AbstractPacketDemultiplexer<RequestPacket> {
		final PaxosClientAsync client;

		ClientPacketDemultiplexer(PaxosClientAsync client) {
			super(1);
			this.client = client;
			this.register(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
		}

		@Override
		public boolean handleMessage(RequestPacket message, edu.umass.cs.nio.nioutils.NIOHeader header) {
			ClientRequest response = message.getResponse();
			if (response != null)
				if (callbacks.containsKey(response.getRequestID()))
					callbacks.remove(response.getRequestID()).handleResponse(
							response);
				else if (PaxosClientAsync.this.defaultCallback != null)
					PaxosClientAsync.this.defaultCallback
							.handleResponse(response);

			return true;
		}

		@Override
		protected Integer getPacketType(RequestPacket message) {
			assert (message instanceof PaxosPacket);
			return PaxosPacketType.PAXOS_PACKET.getInt();
		}

		@Override
		protected RequestPacket processHeader(byte[] message, NIOHeader header) {
			return PaxosPacketDemultiplexerFast.getRequestPacket(message,
					header);
		}

		@Override
		protected boolean matchesType(Object message) {
			return message instanceof JSONObject || message instanceof byte[];
		}
	}

	/**
	 * @param callback
	 * @return The previous value of the default callback if any.
	 */
	public RequestCallback setDefaultCallback(RequestCallback callback) {
		RequestCallback old = this.defaultCallback;
		this.defaultCallback = callback;
		return old;
	}

	/**
	 * @param paxosID Name of the replicated state machine.
	 * @param value
	 * @param server Server to which the request is redirected.
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 * @throws JSONException
	 */
	public Long sendRequest(String paxosID, String value,
			InetSocketAddress server, RequestCallback callback)
			throws IOException, JSONException {
		RequestPacket request = null;
		RequestCallback prev = null;
		do {
			request = new RequestPacket(value, false);
			request.putPaxosID(paxosID, 0);
			prev = this.callbacks.putIfAbsent((long) request.requestID,
					callback);
		} while (prev != null);
		return this.sendRequest(request, server, callback);
	}

	/**
	 * @param paxosID Name of the replicated state machine.
	 * @param value
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 * @throws JSONException
	 */
	public long sendRequest(String paxosID, String value,
			RequestCallback callback) throws IOException, JSONException {
		Long reqID = null;
		do {
			RequestPacket request = new RequestPacket(value, false);
			request.putPaxosID(paxosID, 0);
			reqID = this.sendRequest(request, callback);
		} while (reqID == null);
		return reqID;
	}

	/**
	 * @param request
	 * @param server Server to which the request is redirected.
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 * @throws JSONException
	 */
	public Long sendRequest(RequestPacket request, InetSocketAddress server,
			RequestCallback callback) throws IOException, JSONException {
		int sent = -1;
		assert (request.getPaxosID() != null);
		try {
			this.callbacks.putIfAbsent((long) request.requestID, callback);
			if (this.callbacks.get((long) request.requestID) == callback) {
				sent = this.niot.sendToAddress(server, request.toJSONObject());
			}
		} finally {
			if (sent <= 0) {
				this.callbacks.remove(request.requestID, callback);
				return null;
			}
		}
		return (long) request.requestID;
	}

	/**
	 * @param request
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 * @throws JSONException
	 */
	public Long sendRequest(RequestPacket request, RequestCallback callback)
			throws IOException, JSONException {
		return this.sendRequest(request,
				servers[(int) (Math.random() * this.servers.length)], callback);
	}

	/**
	 * @param servers Set of all servers.
	 * @throws IOException
	 */
	public PaxosClientAsync(Set<InetSocketAddress> servers) throws IOException {
		this.niot = (new MessageNIOTransport<String, JSONObject>(null, null,
				(new ClientPacketDemultiplexer(this)), true,
				SSL_MODES.valueOf(Config.getGlobalString(PC.CLIENT_SSL_MODE))));
		this.servers = PaxosConfig.offsetSocketAddresses(servers,
				PaxosConfig.getClientPortOffset()).toArray(
				new InetSocketAddress[0]);
	}

	/**
	 * 
	 */
	public void close() {
		this.niot.stop();
	}

	/**
	 * @throws IOException
	 */
	public PaxosClientAsync() throws IOException {
		this(PaxosServer.getDefaultServers());
	}
}
