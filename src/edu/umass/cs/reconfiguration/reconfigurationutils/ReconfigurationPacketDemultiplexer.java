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
package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.utils.Config;

/**
 * @author V. Arun
 */
public class ReconfigurationPacketDemultiplexer extends
		AbstractPacketDemultiplexer<Request> {
	private Stringifiable<?> unstringer;
	private AppRequestParser appRequestparser;

	/**
	 * @param unstringer
	 * @param arp
	 * 
	 */
	public ReconfigurationPacketDemultiplexer(Stringifiable<?> unstringer,
			AppRequestParser arp) {
		super(Config.getGlobalInt(PC.PACKET_DEMULTIPLEXER_THREADS));
		this.unstringer = unstringer;
		this.appRequestparser = arp;
	}

	/**
	 * @param unstringer
	 */
	public ReconfigurationPacketDemultiplexer(Stringifiable<?> unstringer) {
		this(unstringer, null);

	}
	
	/**
	 * @param parser
	 * @return {@code this}
	 */
	public ReconfigurationPacketDemultiplexer setAppRequestParser(AppRequestParser parser) {
		this.appRequestparser = parser;
		return this;
	}

	public ReconfigurationPacketDemultiplexer setThreadName(String name) {
		super.setThreadName(name);
		return this;
	}

	private static final boolean BYTEIFICATION = Config
			.getGlobalBoolean(PC.BYTEIFICATION);

	@Override
	protected Integer getPacketType(Request message) {
		return message.getRequestType().getInt();
	}

	@Override
	protected boolean matchesType(Object message) {
		return message instanceof Request;
	}

	@Override
	protected Request processHeader(byte[] message, NIOHeader header) {
		assert (message != null);
		ByteBuffer bbuf = ByteBuffer.wrap(message);
		ReconfigurationPacket.PacketType rcType = null;
		JSONObject json = null;
		// try to get reconfiguration packet JSON first
		if ((BYTEIFICATION
				&& (rcType = ReconfigurationPacket.PacketType.intToType
						.get(bbuf.getInt())) != null
				&& rcType != ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST && (json = AbstractJSONPacketDemultiplexer
				.processHeaderStatic(message, Integer.BYTES, header, true)) != null)

				|| (!BYTEIFICATION
						&& JSONPacket.couldBeJSON(message, Integer.BYTES) && (json = AbstractJSONPacketDemultiplexer
						.processHeaderStatic(message, 0, header, true)) != null)) {
			// decode json as reconfiguration packet
			return ReconfigurationPacket
					.getReconfigurationPacketSuppressExceptions(json,
							this.unstringer);
		}
		// else must be app packet
		if (this.appRequestparser != null) {
			try {
				if (this.appRequestparser instanceof AppRequestParserBytes)
					return ((AppRequestParserBytes) this.appRequestparser)
							.getRequest(message, header);
				else
					return this.appRequestparser.getRequest(new String(message,
							MessageNIOTransport.NIO_CHARSET_ENCODING));
			} catch (UnsupportedEncodingException | RequestParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public boolean handleMessage(Request message,
			edu.umass.cs.nio.nioutils.NIOHeader header) {
		log.info(this + " throwing runtime exception for message " + message);
		throw new RuntimeException("Should never be called");
	}

	boolean handleMessage(JSONObject json) {
		throw new RuntimeException(
				"This method should never be called unless we have \"forgotten\" to register or handle some packet types.");
	}

	// TODO: unused, remove
	JSONObject processHeaderOld(byte[] msg, NIOHeader header) {
		int type;
		JSONObject json = null;
		log.log(Level.FINEST,
				"{0} processHeader received message with header {1}",
				new Object[] { this, header });
		if (BYTEIFICATION // first four bytes contain type
				&& msg.length >= Integer.BYTES
				&& ReconfigurationPacket.PacketType.intToType
						.containsKey(type = ByteBuffer.wrap(msg, 0, 4).getInt())
				&& JSONPacket.couldBeJSON(msg, Integer.BYTES)
				&& type != PacketType.REPLICABLE_CLIENT_REQUEST.getInt()
				&& (json = AbstractJSONPacketDemultiplexer.processHeaderStatic(
						msg, Integer.BYTES, header, true)) != null
				|| 
				// no byteification => plain json
				!BYTEIFICATION
				&& JSONPacket.couldBeJSON(msg)
				&& (json = AbstractJSONPacketDemultiplexer.processHeaderStatic(
						msg, 0, header, true)) != null) {
			return json;
		}
		// else prefix msg with addresses
		byte[] stamped = new byte[NIOHeader.BYTES + msg.length];
		ByteBuffer bbuf = ByteBuffer.wrap(stamped);
		bbuf.put(header.toBytes());
		bbuf.put(msg);
		return new JSONMessenger.JSONObjectWrapper(stamped);
	}

	// TODO: unused, remove
	Integer getPacketType(JSONObject json) {
		if (json instanceof JSONMessenger.JSONObjectWrapper) {
			byte[] bytes = (byte[]) ((JSONMessenger.JSONObjectWrapper) json)
					.getObj();
			if (!JSONPacket.couldBeJSON(bytes, NIOHeader.BYTES)) {
				// first 4 bytes (after 12 bytes of address) must be the type
				return ByteBuffer.wrap(bytes, NIOHeader.BYTES, Integer.BYTES)
						.getInt();
			} else {
				// return any valid type (assuming no more chained
				// demultiplexers)
				return PacketType.ECHO_REQUEST.getInt();
			}
		} else
			try {
				return JSONPacket.getPacketType(json);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		return null;
	}

}
