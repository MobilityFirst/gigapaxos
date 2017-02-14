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
package edu.umass.cs.gigapaxos.paxosutil;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;

import edu.umass.cs.gigapaxos.PaxosConfig;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.BatchedAcceptReply;
import edu.umass.cs.gigapaxos.paxospackets.BatchedCommit;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageExtractor;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 *         <p>
 *         This is a faster demultiplexer than PaxosPacketDemultiplexerJSON.
 *         JSON turns out to be the bottleneck. This class supports json-smart
 *         and direct serialization to and from byte[]. The last option is the
 *         fastest. We need faster options only for RequestPacket and
 *         AcceptPacket; for everything else JSON is just fine.
 * 
 *         Byteification is a bit harder to maintain, especially the
 *         processHeader part, but is worth it for this critical demultiplexer.
 */
public abstract class PaxosPacketDemultiplexerFast extends
		AbstractPacketDemultiplexer<Object> {
	/**
	 * @param numThreads
	 */
	public PaxosPacketDemultiplexerFast(int numThreads) {
		super(numThreads);
	}

	private static PaxosPacket toPaxosPacket(byte[] bytes)
			throws UnsupportedEncodingException, UnknownHostException {
		assert (bytes != null);
		ByteBuffer bbuf = ByteBuffer.wrap(bytes);

		PaxosPacket.PaxosPacketType type = bbuf.getInt() == PaxosPacketType.PAXOS_PACKET
				.getInt() ? PaxosPacketType.getPaxosPacketType(bbuf.getInt())
				: null;

		if (type == null)
			fatal(bytes);

		// bbuf = ByteBuffer.wrap(bytes);
		bbuf.rewind();

		PaxosPacket paxosPacket = null;
		switch (type) {
		case REQUEST:
			// log.info("before new RequestPacket(ByteBuffer)");
			paxosPacket = new RequestPacket(bbuf);
			// log.info("after new RequestPacket(ByteBuffer)");
			break;
		case ACCEPT:
			paxosPacket = new AcceptPacket(bbuf);
			break;
		case BATCHED_COMMIT:
			paxosPacket = new BatchedCommit(bbuf);
			break;
		case BATCHED_ACCEPT_REPLY:
			paxosPacket = new BatchedAcceptReply(bbuf);
			break;

		default:
			assert (false);
		}
		return paxosPacket;
	}

	/**
	 * @param jsonS
	 * @param unstringer
	 * @return Parsed PaxosPacket.
	 * @throws JSONException
	 */
	public static PaxosPacket toPaxosPacket(net.minidev.json.JSONObject jsonS,
			Stringifiable<?> unstringer) throws JSONException {
		assert (jsonS != null);
		assert (jsonS.get(PaxosPacket.Keys.PT.toString()) != null) : jsonS;
		PaxosPacket.PaxosPacketType type = PaxosPacket.PaxosPacketType
				.getPaxosPacketType((Integer) jsonS.get(PaxosPacket.Keys.PT
						.toString()));
		if (type == null)
			fatal(jsonS);

		PaxosPacket paxosPacket = null;
		switch (type) {
		case REQUEST:
			paxosPacket = (new RequestPacket(jsonS));
			break;
		case ACCEPT:
			paxosPacket = (new AcceptPacket(jsonS));
			break;
		case DECISION:
			// not really needed as a special case if we use batched commits
			paxosPacket = (new PValuePacket(jsonS));
			break;

		default:
			return PaxosPacketDemultiplexer.toPaxosPacket(toJSONObject(jsonS),
					unstringer);
		}
		assert (paxosPacket != null) : jsonS;
		return paxosPacket;
	}

	/**
	 * @param message
	 * @param header
	 * @return RequestPacket if parseable from {@code message}.
	 */
	public static RequestPacket getRequestPacket(byte[] message,
			NIOHeader header) {
		Object packet = processHeaderUtil(message, header);
		if (packet != null && packet instanceof RequestPacket)
			return (RequestPacket) packet;
		if(JSONPacket.couldBeJSON(message))
		try {
			packet = PaxosPacketDemultiplexer.toPaxosPacket(new JSONObject(
					MessageExtractor.decode(message)));
			if (packet != null && packet instanceof RequestPacket)
				return (RequestPacket) packet;
		} catch (UnsupportedEncodingException | JSONException e) {
			throw new RuntimeException("Unable to parse "
					+ RequestPacket.class.getSimpleName()
					+ " from byte[] of length " + message.length);
		}
		return null;
	}

	/**
	 * @param jsonS
	 * @return JSONObject.
	 * @throws JSONException
	 */
	public static JSONObject toJSONObject(net.minidev.json.JSONObject jsonS)
			throws JSONException {
		JSONObject json = new JSONObject();
		for (String key : jsonS.keySet()) {
			Object value = jsonS.get(key);
			if (value instanceof Collection<?>)
				json.put(key, new JSONArray((Collection<?>) value));
			else
				json.put(key, value);
		}
		return json;
	}

	private static void fatal(Object json) {
		PaxosConfig.getLogger().severe(
				PaxosPacketDemultiplexerFast.class.getSimpleName()
						+ " received " + json);
		throw new RuntimeException(
				"PaxosPacketDemultiplexer recieved unrecognized paxos packet type");
	}

	public abstract boolean handleMessage(Object message, edu.umass.cs.nio.nioutils.NIOHeader header);

	@Override
	protected Integer getPacketType(Object message) {
		if (message instanceof net.minidev.json.JSONObject)
			return (Integer) ((net.minidev.json.JSONObject) message)
					.get(JSONPacket.PACKET_TYPE.toString());

		assert (message instanceof PaxosPacket || message instanceof byte[]) : message;

		return (message instanceof byte[]) ? ByteBuffer.wrap((byte[]) message,
				0, 4).getInt() : PaxosPacketType.PAXOS_PACKET.getInt();
	}

	// currently only RequestPacket is byteable
	private static boolean isByteable(byte[] bytes) {
		ByteBuffer bbuf;
		int type = -1;
		if ((bbuf = ByteBuffer.wrap(bytes, 0, 8)).getInt() == PaxosPacket.PaxosPacketType.PAXOS_PACKET
				.getInt()
				&& ((type = bbuf.getInt()) == PaxosPacket.PaxosPacketType.REQUEST
						.getInt()
						|| (type == PaxosPacket.PaxosPacketType.ACCEPT.getInt())
						|| type == PaxosPacketType.BATCHED_COMMIT.getInt() || type == PaxosPacketType.BATCHED_ACCEPT_REPLY
						.getInt()))
			return true;
		assert (type != PaxosPacket.PaxosPacketType.PROPOSAL.getInt());
		return false;
	}

	@Override
	protected Object processHeader(byte[] bytes, NIOHeader header) {
		return processHeaderUtil(bytes, header);
	}

	/**
	 * @param bytes
	 * @param header
	 * @return A static utility method to convert bytes to RequestPacket with
	 *         header processing.
	 */
	public static final Object processHeaderUtil(byte[] bytes, NIOHeader header) {
		if (isByteable(bytes)) {
			long t = System.nanoTime();
			if (PaxosPacket.getType(bytes) == PaxosPacketType.REQUEST) {
				// affix header info only for request packets
				byte[] caddress = header.sndr.getAddress().getAddress();
				short cport = (short) header.sndr.getPort();
				byte[] laddress = header.rcvr.getAddress().getAddress();
				short lport = (short) header.rcvr.getPort();
				ByteBuffer bbuf = ByteBuffer.wrap(bytes);
				for (int i = 0; i < 3; i++)
					bbuf.getInt();
				int paxosIDLength = bbuf.get();

				int offset = 13 + paxosIDLength + 8 + 1;
				int expectedPos = offset + 4 + 2 + 4 + 2;
				assert (bytes.length > offset + 12) : bytes.length + " <= "
						+ expectedPos;
				
				//bbuf = ByteBuffer.wrap(bytes, offset, 12);
				bbuf.position(offset).limit(offset +12);
				
				boolean noCA = bytes[offset + 4] == 0 && (bytes[offset + 5] == 0); 
				boolean noLA = bytes[offset + 6 + 4] == 0
						&& bytes[offset + 6 + 5] == 0;
				try {
					if (noCA)
						bbuf.put(caddress).putShort(cport);
					else
						bbuf.position(bbuf.position() + 6);
					if (noLA)
						bbuf.put(laddress).putShort(lport);
					else
						bbuf.position(bbuf.position() + 6);

				} catch (Exception e) {
					assert (false) : bytes.length + " ? " + 16 + 4
							+ paxosIDLength + 8 + 1;
				}
			}
			try {
				PaxosPacket pp = toPaxosPacket(bytes);
				if (PaxosMessenger.INSTRUMENT_SERIALIZATION && Util.oneIn(100)) {
					if (pp.getType() == PaxosPacketType.REQUEST)
						DelayProfiler.updateDelayNano("<-request", t);
					else if (pp.getType() == PaxosPacketType.BATCHED_ACCEPT_REPLY)
						DelayProfiler.updateDelayNano("<-acceptreply", t);
				}
				return pp;
			} catch (UnsupportedEncodingException | UnknownHostException e) {
				e.printStackTrace();
			}
			return null;
		}
		
		if(!JSONPacket.couldBeJSON(bytes)) return bytes;

		String message;
		long t = System.nanoTime();
		try {
			message = MessageExtractor.decode(bytes);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
		net.minidev.json.JSONObject json = MessageExtractor
				.parseJSONSmart(message);
		assert (json != null) : message;
		net.minidev.json.JSONObject retval = MessageExtractor
				.stampAddressIntoJSONObject(header.sndr, header.rcvr,
						insertStringifiedSelf(json, message));
		assert (retval != null) : message + " " + header;
		try {
			if (PaxosMessenger.INSTRUMENT_SERIALIZATION && Util.oneIn(100))
				if (PaxosPacket.getPaxosPacketType(retval) == PaxosPacket.PaxosPacketType.REQUEST)
					DelayProfiler.updateDelayNano("requestJSONification", t);
				else if (PaxosPacket.getPaxosPacketType(retval) == PaxosPacket.PaxosPacketType.BATCHED_ACCEPT_REPLY)
					DelayProfiler.updateDelayNano(
							"batchedAcceptReplyJSONification", t);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return retval;
	}

	private final boolean ORDER_PRESERVING_REQUESTS = Config
			.getGlobalBoolean(PC.ORDER_PRESERVING_REQUESTS);

	@Override
	public boolean isOrderPreserving(Object msg) {
		if (!ORDER_PRESERVING_REQUESTS)
			return false;
		if (msg instanceof PaxosPacket)
			return ((PaxosPacket) msg).getType() == PaxosPacketType.REQUEST;

		if (msg instanceof byte[]) {
			ByteBuffer bbuf = ByteBuffer.wrap((byte[]) msg, 0, 8);
			int type;
			if (bbuf.getInt() == PaxosPacket.PaxosPacketType.PAXOS_PACKET
					.getInt()) {
				type = bbuf.getInt();
				if (type == PaxosPacket.PaxosPacketType.REQUEST.getInt())
					return true;
			}
		}
		// else
		assert (msg instanceof net.minidev.json.JSONObject);
		// only preserve order for REQUEST or PROPOSAL packets
		PaxosPacketType type = PaxosPacket.PaxosPacketType
				.getPaxosPacketType(((Integer) ((net.minidev.json.JSONObject) msg)
						.get(PaxosPacket.Keys.PT.toString())));
		return (type != null && type
				.equals(PaxosPacket.PaxosPacketType.REQUEST));
	}

	private static net.minidev.json.JSONObject insertStringifiedSelf(
			net.minidev.json.JSONObject json, String message) {
		// sigh: we need the string to avoid restringification overhead
		try {
			if (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.ACCEPT)
				json.put(RequestPacket.Keys.STRINGIFIED.toString(), message);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return json;
	}
}
