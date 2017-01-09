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
package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorRequest;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public abstract class ServerReconfigurationPacket<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> implements ReconfiguratorRequest {

	private static enum Keys {
		NEWLY_ADDED_NODES, NODE_ID, SOCKET_ADDRESS, DELETED_NODES, FAILED, RESPONSE_MESSAGE
	};

	/**
	 * Map used for adding new RC nodes.
	 */
	public final Map<NodeIDType, InetSocketAddress> newlyAddedNodes;
	/**
	 * Set of RC nodes being deleted.
	 */
	public final Set<NodeIDType> deletedNodes;

	/**
	 * Requester address to send the confirmation.
	 */
	public final InetSocketAddress creator;

	private boolean failed = false;
	private String responseMessage = null;
	private InetSocketAddress myReceiver = null;

	/**
	 * @param initiator
	 * @param type
	 * @param nodeID
	 * @param sockAddr
	 */
	public ServerReconfigurationPacket(NodeIDType initiator,
			ReconfigurationPacket.PacketType type, NodeIDType nodeID,
			InetSocketAddress sockAddr) {
		super(initiator, type, type.toString(), 0);
		(this.newlyAddedNodes = new HashMap<NodeIDType, InetSocketAddress>())
				.put(nodeID, sockAddr);
		this.deletedNodes = null;
		this.creator = null;
	}

	/**
	 * @param initiator
	 * @param type
	 * @param newlyAddedNodes
	 * @param deletedNodes
	 */
	public ServerReconfigurationPacket(NodeIDType initiator,
			ReconfigurationPacket.PacketType type,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes,
			Set<NodeIDType> deletedNodes) {
		super(initiator, type, type.toString(), 0);
		this.newlyAddedNodes = newlyAddedNodes;
		this.deletedNodes = deletedNodes;
		this.creator = null;
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public ServerReconfigurationPacket(JSONObject json,
			Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
		this.newlyAddedNodes = this.arrayToMap(
				json.optJSONArray(Keys.NEWLY_ADDED_NODES.toString()),
				unstringer);
		this.deletedNodes = (json.has(Keys.DELETED_NODES.toString()) ? this
				.arrayToSet(json.getJSONArray(Keys.DELETED_NODES.toString()),
						unstringer) : null);
		this.creator = JSONNIOTransport.getSenderAddress(json);
		this.myReceiver = JSONNIOTransport.getReceiverAddress(json);
		this.failed = json.optBoolean(Keys.FAILED.toString());
		this.responseMessage = json.has(Keys.RESPONSE_MESSAGE.toString()) ? json
				.getString(Keys.RESPONSE_MESSAGE.toString()) : null;

	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		if (this.newlyAddedNodes != null) {
			json.put(Keys.NEWLY_ADDED_NODES.toString(),
					this.mapToArray(newlyAddedNodes));
		}
		if (this.deletedNodes != null)
			json.put(Keys.DELETED_NODES.toString(),
					this.setToArray(this.deletedNodes));
		// no need to enter requester information
		if (this.failed)
			json.put(Keys.FAILED.toString(), this.failed);
		json.put(Keys.RESPONSE_MESSAGE.toString(), this.responseMessage);

		return json;
	}

	@Override
	public IntegerPacketType getRequestType() {
		return this.type;
	}

	/**
	 * @return Set of nodes being added.
	 */
	public Set<NodeIDType> getAddedNodeIDs() {
		return (this.newlyAddedNodes != null ? new HashSet<NodeIDType>(
				this.newlyAddedNodes.keySet()) : new HashSet<NodeIDType>());
	}

	/**
	 * @return Set of nodes being deleted.
	 */
	public Set<NodeIDType> getDeletedNodeIDs() {
		return this.deletedNodes == null ? new HashSet<NodeIDType>()
				: this.deletedNodes;
	}

	/**
	 * @return Requester address.
	 */
	public InetSocketAddress getIssuer() {
		return this.creator;
	}

	// Utility method for newly added nodes
	private Map<NodeIDType, InetSocketAddress> arrayToMap(JSONArray jArray,
			Stringifiable<NodeIDType> unstringer) throws JSONException {
		if (jArray == null || jArray.length() == 0)
			return null;
		Map<NodeIDType, InetSocketAddress> map = new HashMap<NodeIDType, InetSocketAddress>();
		for (int i = 0; i < jArray.length(); i++) {
			JSONObject jElement = jArray.getJSONObject(i);
			assert (jElement.has(Keys.NODE_ID.toString()) && jElement
					.has(Keys.SOCKET_ADDRESS.toString()));
			map.put(unstringer.valueOf(jElement.getString(Keys.NODE_ID
					.toString())), Util.getInetSocketAddressFromString(jElement
					.getString(Keys.SOCKET_ADDRESS.toString())));
		}
		return map;
	}

	// Utility method for newly added nodes
	private JSONArray mapToArray(Map<NodeIDType, InetSocketAddress> map)
			throws JSONException {
		JSONArray jArray = new JSONArray();
		for (NodeIDType node : map.keySet()) {
			JSONObject jElement = new JSONObject();
			jElement.put(Keys.NODE_ID.toString(), node.toString());
			jElement.put(Keys.SOCKET_ADDRESS.toString(), map.get(node)
					.toString());
			jArray.put(jElement);
		}
		return jArray;
	}

	private Set<NodeIDType> arrayToSet(JSONArray jArray,
			Stringifiable<NodeIDType> unstringer) throws JSONException {
		if (jArray == null || jArray.length() == 0)
			return null;
		Set<NodeIDType> set = new HashSet<NodeIDType>();
		for (int i = 0; i < jArray.length(); i++) {
			set.add(unstringer.valueOf(jArray.getString(i)));
		}
		return set;
	}

	// need this to go from NodeIDType to String set
	private JSONArray setToArray(Set<NodeIDType> set) throws JSONException {
		if (set == null || set.isEmpty())
			return null;
		Set<String> stringSet = new HashSet<String>();
		for (NodeIDType node : set)
			stringSet.add(node.toString());
		return new JSONArray(stringSet);
	}

	/* (non-Javadoc)
	 * 
	 * @see
	 * edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationRequest
	 * #setFailed() */
	@Override
	public ServerReconfigurationPacket<NodeIDType> setFailed() {
		this.failed = true;
		return this;
	}

	/* (non-Javadoc)
	 * 
	 * @see
	 * edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationRequest
	 * #isFailed() */
	@Override
	public boolean isFailed() {
		return this.failed;
	}

	/* (non-Javadoc)
	 * 
	 * @see
	 * edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationRequest
	 * #setResponseMessage(java.lang.String) */
	@Override
	public ServerReconfigurationPacket<NodeIDType> setResponseMessage(String msg) {
		this.responseMessage = msg;
		return this;
	}

	/* (non-Javadoc)
	 * 
	 * @see
	 * edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationRequest
	 * #getResponseMessage() */
	@Override
	public String getResponseMessage() {
		return this.responseMessage;
	}

	public String getSummary() {
		return getServiceName() + ":" + getEpochNumber() + "[adds="
				+ this.newlyAddedNodes + "; deletes=" + this.deletedNodes + "]";
	}

	/**
	 * @return Socket address on which this request was received.
	 */
	public InetSocketAddress getMyReceiver() {
		return this.myReceiver;
	}

	/**
	 * @return True if deletedNodes has elements.
	 */
	public boolean hasDeletedNodes() {
		return this.deletedNodes != null && !this.deletedNodes.isEmpty();
	}

	/**
	 * @return True if newlyAddedNodes has elements.
	 */
	public boolean hasAddedNodes() {
		return this.newlyAddedNodes != null && !this.newlyAddedNodes.isEmpty();
	}
}
