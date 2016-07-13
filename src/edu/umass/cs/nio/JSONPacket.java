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
import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.IntegerPacketType;

/**
 * @author V. Arun
 */

/* An abstract class that all json packets should extend. */
public abstract class JSONPacket {
	/**
	 * JSON key for the integer packet type.
	 */
	public static final String PACKET_TYPE = "type";
	protected /*final*/ int type;

	/**
	 * @param t
	 */
	public JSONPacket(IntegerPacketType t) {
		this.type = t.getInt();
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public JSONPacket(JSONObject json) throws JSONException {
		this.type = getPacketType(json);
	}

	/**
	 * @return JSONObject corresponding to fields in classes extending this
	 *         class.
	 * @throws JSONException
	 */
	protected abstract JSONObject toJSONObjectImpl() throws JSONException;

	/**
	 * @return JSONObject corresponding to this class' (including subclasses)
	 *         fields.
	 * @throws JSONException
	 */
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = toJSONObjectImpl();
		json.put(PACKET_TYPE, type);
		return json;
	}

	public String toString() {
		try {
			return toJSONObject().toString();
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return null;
	}

	/* ********************* static helper methods below ****************** */

	/**
	 * @param json
	 * @return Integer packet type.
	 * @throws JSONException
	 */
	public static final Integer getPacketType(JSONObject json)
			throws JSONException {
		if (json.has(PACKET_TYPE))
			return json.getInt(PACKET_TYPE);
		else
			return null;
	}

	/**
	 * Puts type into json.
	 * 
	 * @param json
	 * @param type
	 */
	public static final void putPacketType(JSONObject json, int type) {
		try {
			json.put(PACKET_TYPE, type);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Puts type.getInt() into json.
	 * 
	 * @param json
	 * @param type
	 */
	public static final void putPacketType(JSONObject json,
			IntegerPacketType type) {
		try {
			json.put(PACKET_TYPE, type.getInt());
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param str
	 * @return True if {@code str} could possibly (but not necessarily) be in
	 *         JSON format. This sanity check is quicker compared to taking a
	 *         JSONException hit.
	 */
	public static final boolean couldBeJSON(String str) {
		return str.startsWith("{") || str.startsWith("[");
	}
	/**
	 * @param str
	 * @return True if str starts with "{"
	 */
	public static final boolean couldBeJSONObject(String str) {
		return str.startsWith("{");
	}
	/**
	 * @param str
	 * @return True if str starts with "["
	 */
	public static final boolean couldBeJSONArray(String str) {
		return str.startsWith("[");
	}

	/**
	 * @param bytes
	 * @return True if this {@code bytes} could be possibly (but not
	 *         necessarily) be in JSON format assuming the default
	 *         {@link MessageExtractor} encoding.
	 */
	public static final boolean couldBeJSON(byte[] bytes)
			 {
		String str;
		try {
			str = MessageExtractor.decode(Arrays.copyOf(bytes, 4));
			return str.startsWith("{") || str.startsWith("[");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return bytes[0]=='{' || bytes[0]=='[';
	}
	/**
	 * @param bytes
	 * @param offset 
	 * @return True if this {@code bytes} could be possibly (but not
	 *         necessarily) be in JSON format assuming the default
	 *         {@link MessageExtractor} encoding.
	 */
	public static final boolean couldBeJSON(byte[] bytes, int offset)
			 {
		String str;
		try {
			str = MessageExtractor.decode(Arrays.copyOfRange(bytes, offset, offset+4));
			return str.startsWith("{") || str.startsWith("[");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return bytes[offset]=='{' || bytes[offset]=='[';
	}

}
