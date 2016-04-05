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

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Application;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageExtractor;
import edu.umass.cs.nio.nioutils.NIOHeader;

/**
 * @author V. Arun
 */
public class ReconfigurationPacketDemultiplexer extends
		AbstractJSONPacketDemultiplexer {
	/**
	 * 
	 */
	public ReconfigurationPacketDemultiplexer() {
	}

	/**
	 * @param numThreads
	 */
	public ReconfigurationPacketDemultiplexer(int numThreads) {
		super(numThreads);
	}

	public ReconfigurationPacketDemultiplexer setThreadName(String name) {
		super.setThreadName(name);
		return this;
	}

	@Override
	public boolean handleMessage(JSONObject json) {
		throw new RuntimeException(
				"This method should never be called unless we have \"forgotten\" to register or handle some packet types.");
	}

	@Override
	public JSONObject processHeader(byte[] msg, NIOHeader header) {
		if (JSONPacket.couldBeJSON(msg)) {
			JSONObject json =  super.processHeader(msg, header, true);
			if(json!=null) return json;
		}
		// else prefix msg with addresses
		byte[] stamped = new byte[NIOHeader.BYTES + msg.length];
		ByteBuffer bbuf = ByteBuffer.wrap(stamped);
		bbuf.put(header.toBytes());
		bbuf.put(msg);
		return new JSONMessenger.JSONObjectWrapper(stamped);
	}

	@Override
	public Integer getPacketType(JSONObject json) {
		if (json instanceof JSONMessenger.JSONObjectWrapper) {
			// first 4 bytes (after 12 bytes of address) must be the type
			return ByteBuffer.wrap(
					(byte[]) ((JSONMessenger.JSONObjectWrapper) json).getObj(),
					NIOHeader.BYTES, Integer.BYTES).getInt();
		} else
			try {
				return JSONPacket.getPacketType(json);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		return null;
	}
}
