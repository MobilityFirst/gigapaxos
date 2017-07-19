/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         This class has an additional field for returning the set of active
 *         replicas in addition to the fields in the generic
 *         ClientReconfigurationPacket. This class is used to both request for
 *         and return the set of active replicas for a name. If the field
 *         actives is null, it is implicitly interpreted as a request, else as a
 *         response.
 */
public class RequestActiveReplicas extends ClientReconfigurationPacket implements ReplicableRequest {

	/**
	 *
	 */
	public static enum Keys {
		/**
		 * Active replica set.
		 */
		ACTIVE_REPLICAS,
		
		/**
		 * 
		 */
		QID,
		
	};

	/**
	 * Unstringer for InetSocketAddress of sender.
	 */
	public static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	private Set<InetSocketAddress> actives = null;

	// need this if we need to coordinate this request
	private final long requestID;

	/**
	 * @param name
	 */
	public RequestActiveReplicas(String name) {
		this(null, name, 0);
	}
	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 */
	public RequestActiveReplicas(InetSocketAddress initiator, String name,
			int epochNumber) {
		super(initiator,
				ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS, name,
				epochNumber);
		this.actives = null;
		this.requestID = (long)(Math.random()*Long.MAX_VALUE);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public RequestActiveReplicas(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, RequestActiveReplicas.unstringer); // ignores arg unstringer
		// assertion may not hold with String packet demultiplexer
		//assert(this.getSender()!=null);
		//this.setSender(JSONNIOTransport.getSenderAddress(json));

		JSONArray jsonArray = json.has(Keys.ACTIVE_REPLICAS.toString()) ? json
				.getJSONArray(Keys.ACTIVE_REPLICAS.toString()) : null;
		if (jsonArray != null) {
			this.actives = new HashSet<InetSocketAddress>();
			for (int i = 0; jsonArray != null && i < jsonArray.length(); i++)
				this.actives.add(RequestActiveReplicas.unstringer
						.valueOf(jsonArray.get(i).toString()));
		}
		this.requestID = json.getLong(Keys.QID.toString());
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public RequestActiveReplicas(JSONObject json) throws JSONException {
		this(json, null);
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		if (this.actives != null)
			json.put(Keys.ACTIVE_REPLICAS.toString(), new JSONArray(
					this.actives));
		json.put(Keys.QID.toString(), this.requestID);
		return json;
	}

	/**
	 * @param replicas
	 * @return {@code this} with active replicas set to {@code replicas}.
	 */
	public ClientReconfigurationPacket setActives(Set<InetSocketAddress> replicas) {
		this.actives = replicas;
		return this;
	}

	/**
	 * @return Active replica socket addresses.
	 */
	public Set<InetSocketAddress> getActives() {
		return this.actives;
	}

	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		String[] addrs = { "128.119.240.21" };
		int[] ports = { 3245 };
		assert (addrs.length == ports.length);
		InetSocketAddress[] isaddrs = new InetSocketAddress[addrs.length];
		try {
			for (int i = 0; i < addrs.length; i++) {
				isaddrs[i] = new InetSocketAddress(
						InetAddress.getByName(addrs[i]), ports[i]);
			}
			String name = "name";
			InetSocketAddress sender = new InetSocketAddress(
					InetAddress.getLoopbackAddress(), 1234);
			RequestActiveReplicas req1 = new RequestActiveReplicas(sender,
					name, 0);
			System.out.println(req1);
			JSONObject json1;
			json1 = req1.toJSONObject();
			RequestActiveReplicas req2 = new RequestActiveReplicas(json1, null);
			System.out.println(req2);
//			assert(req1.toString().equals(new RequestActiveReplicas(req1.toJSONObject(), null).toJSONObject().toString()));
		} catch (UnknownHostException | JSONException e) {
			e.printStackTrace();
		}
	}
	@Override
	public long getRequestID() {
		return this.requestID ;
	}
	@Override
	public boolean needsCoordination() {
		return this.needsCoordination;
	}
	
	private boolean needsCoordination = false;
	/**
	 * @return {@code this} with needsCoordination=true
	 */
	public RequestActiveReplicas setNeedsCoordination() {
		this.needsCoordination = true;
		return this;
	}
	/**
	 * @return {@code this} with needsCoordination=false
	 */
	public RequestActiveReplicas unsetNeedsCoordination() {
		this.needsCoordination = false;
		return this;
	}

	public String getSummary() {
		return super.getSummary() + this.actives + ":" + this.getHashRCs();
	}
}
