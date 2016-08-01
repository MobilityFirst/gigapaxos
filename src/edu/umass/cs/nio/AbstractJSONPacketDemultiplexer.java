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
package edu.umass.cs.nio;

import java.io.UnsupportedEncodingException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;

/**
 * @author V. Arun
 */
public abstract class AbstractJSONPacketDemultiplexer extends
		AbstractPacketDemultiplexer<JSONObject> implements
		PacketDemultiplexer<JSONObject> {

	/**
	 * 
	 * @param threadPoolSize
	 *            Refer documentation for
	 *            {@link AbstractPacketDemultiplexer#setThreadPoolSize(int)
	 *            setThreadPoolsize(int)}.
	 */
	protected AbstractJSONPacketDemultiplexer(int threadPoolSize) {
		super(threadPoolSize);
	}

	protected AbstractJSONPacketDemultiplexer() {
		super();
	}

	protected Integer getPacketType(JSONObject json) {
		try {
			if (json != null)
				return JSONPacket.getPacketType(json);
		} catch (JSONException e) {
			NIOTransport.getLogger().severe(
					"Unable to decode JSON packet type for: " + json);
			e.printStackTrace();
		}
		return null;
	}

	protected JSONObject processHeader(byte[] message, NIOHeader header) {
		return processHeader(message, header, false);
	}

	protected JSONObject processHeader(byte[] message, NIOHeader header,
			boolean cacheStringified) {
		return processHeader(message, 0, header, cacheStringified);
	}
	protected JSONObject processHeader(byte[] message, int offset, NIOHeader header,
			boolean cacheStringified) {
		return processHeaderStatic(message, offset, header, cacheStringified);
	}
	
	/**
	 * @param message
	 * @param offset
	 * @param header
	 * @param cacheStringified
	 * @return JSONObject with sender address stamped.
	 */
	public static final JSONObject processHeaderStatic(byte[] message, int offset, NIOHeader header,
			boolean cacheStringified) {
		try {
			if (JSONPacket.couldBeJSON(message, offset)) // quick reject if not
				return MessageExtractor.stampAddressIntoJSONObject(header.sndr,
						header.rcvr, MessageExtractor.parseJSON(
								MessageExtractor.decode(message, offset, message.length-offset),
								cacheStringified));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected boolean matchesType(Object message) {
		return message instanceof JSONObject;
	}

	@Override
	protected boolean isCongested(NIOHeader header) {
		return false;
	}
}
