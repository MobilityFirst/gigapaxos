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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.BatchedPaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.ProposalPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageExtractor;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;

/**
 * @author V. Arun
 *         <p>
 *         Used to get NIO to send paxos packets to PaxosManager. This class has
 *         been merged into PaxosManager now and will be soon deprecated.
 */
public abstract class PaxosPacketDemultiplexerJSONSmart extends
		AbstractPacketDemultiplexer<Object> {
	/**
	 * @param numThreads
	 */
	public PaxosPacketDemultiplexerJSONSmart(int numThreads) {
		super(numThreads);
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
		case PROPOSAL:
			// should never come here anymore
			paxosPacket = (new ProposalPacket(jsonS));
			break;
		case ACCEPT:
			paxosPacket = (new AcceptPacket(jsonS));
			break;
		case DECISION:
			// not really needed as a special case if we use batched commits
			paxosPacket = (new PValuePacket(jsonS));
			break;
		// case BATCHED_PAXOS_PACKET:
		// paxosPacket = (new BatchedPaxosPacket(jsonS));
		// break;

		default:
			return PaxosPacketDemultiplexer.toPaxosPacket(toJSONObject(jsonS),
					unstringer);
		}
		assert (paxosPacket != null) : jsonS;
		return paxosPacket;
	}

	// private static final boolean UNCACHE_STRINGIFIED =
	// Config.getGlobalBoolean(PC.UNCACHE_STRINGIFIED);

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
		PaxosManager.getLogger().severe(
				PaxosPacketDemultiplexerJSONSmart.class.getSimpleName()
						+ " received " + json);
		throw new RuntimeException(
				"PaxosPacketDemultiplexer recieved unrecognized paxos packet type");
	}

	public abstract boolean handleMessage(Object message);

	@Override
	protected Integer getPacketType(Object message) {
		if (message instanceof net.minidev.json.JSONObject)
			return (Integer) ((net.minidev.json.JSONObject) message)
					.get(JSONPacket.PACKET_TYPE.toString());

		assert (message instanceof byte[]);
		ByteBuffer bbuf = ByteBuffer.wrap((byte[]) message, 0, 4);
		return bbuf.getInt();
	}

	@Override
	protected Object getMessage(byte[] bytes) {
		String message = null;
		try {
			message = MessageExtractor.decode(bytes);
			return this.insertStringifiedSelf(
					MessageExtractor.parseJSONSmart(message), message);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	// currently only RequestPacket is byteable
	private boolean isByteable(byte[] bytes) {
		ByteBuffer bbuf;
		int type;
		if ((bbuf = ByteBuffer.wrap(bytes, 0, 8)).getInt() == PaxosPacket.PaxosPacketType.PAXOS_PACKET
				.getInt()
				&& ((type =bbuf.getInt()) == PaxosPacket.PaxosPacketType.REQUEST
						.getInt() 
						|| type == PaxosPacket.PaxosPacketType.PROPOSAL.getInt()
						))
			return true;
		return false;
	}

	@Override
	protected Object processHeader(byte[] bytes, NIOHeader header) {
		if (this.isByteable(bytes)) {
			// affix header info
			byte[] caddress = header.sndr.getAddress().getAddress();
			short cport = (short) header.sndr.getPort();
			byte[] laddress = header.rcvr.getAddress().getAddress();
			short lport = (short) header.rcvr.getPort();
			ByteBuffer bbuf = ByteBuffer.wrap(bytes, 0, 16);
			for (int i = 0; i < 3; i++)
				bbuf.getInt();
			int paxosIDLength = bbuf.get();

			int offset = 13 + paxosIDLength + 8 + 1;
			int expectedPos = offset + 4 + 2 + 4 + 2;
			assert (bytes.length > offset + 12) : bytes.length
					+ " <= " + expectedPos;
			bbuf = ByteBuffer.wrap(bytes, offset,
					12);
			boolean noCA = bytes[offset + 4] == 0 && bytes[offset + 5] == 0;
			boolean noLA = bytes[offset + 6 + 4] == 0
					&& bytes[offset + 6 + 5] == 0;
			try {
				if (noCA)
					bbuf.put(caddress).putShort(cport);
				if (noLA)
					bbuf.put(laddress).putShort(lport);
			} catch (Exception e) {
				assert (false) : bytes.length + " ? " + 16 + 4 + paxosIDLength
						+ 8 + 1;
			}
			return bytes;
		}

		String message;
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
						this.insertStringifiedSelf(json, message));
		assert (retval != null) : message + " " + header;
		return retval;
	}

	@Override
	public boolean isOrderPreserving(Object msg) {
		if (msg instanceof byte[]) {
			ByteBuffer bbuf = ByteBuffer.wrap((byte[]) msg, 0, 8);
			int type;
			if (bbuf.getInt() == PaxosPacket.PaxosPacketType.PAXOS_PACKET
					.getInt()) {
				type = bbuf.getInt();
				if (type == PaxosPacket.PaxosPacketType.REQUEST.getInt()
						|| type == PaxosPacket.PaxosPacketType.PROPOSAL
								.getInt())
					return true;
			}
		}
		// else
		assert (msg instanceof net.minidev.json.JSONObject);
		// only preserve order for REQUEST or PROPOSAL packets
		PaxosPacketType type = PaxosPacket.PaxosPacketType
				.getPaxosPacketType(((Integer) ((net.minidev.json.JSONObject) msg)
						.get(PaxosPacket.Keys.PT.toString())));
		return (type != null
				&& type.equals(PaxosPacket.PaxosPacketType.REQUEST) || type
					.equals(PaxosPacket.PaxosPacketType.PROPOSAL));
	}

	private net.minidev.json.JSONObject insertStringifiedSelf(
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
