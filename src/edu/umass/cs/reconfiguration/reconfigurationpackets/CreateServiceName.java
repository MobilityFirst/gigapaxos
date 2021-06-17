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

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.ReconfigureUponActivesChange;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BatchedCreateServiceName.BatchKeys;
import edu.umass.cs.utils.Util;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author V. Arun
 * 
 *         This class has a field to specify the initial state in addition to
 *         the default fields in ClientReconfigurationPacket.
 */
public class CreateServiceName extends ClientReconfigurationPacket {

	/**
	 *
	 */
	public static enum Keys {
		/**
		 * 
		 */
		NAME, /**
		 * 
		 */
		STATE, /**
		 * 
		 */
		NAME_STATE_ARRAY,

		/**
		 * Set of names in a batch create that could not be created or could not
		 * be confirmed as having been successfully created (but could have
		 * gotten created after all).
		 */
		FAILED_CREATES,

		/**
		 * Initial active replica group.
		 */
		INIT_GROUP,
		
		/**
		 * Reconfiguration behavior when active replicas are added or deleted.
		 */
		RECONFIGURE_UPON_ACTIVES_CHANGE
	};

	/**
	 * Unstringer needed to handle client InetSocketAddresses as opposed to
	 * NodeIDType.
	 */
	public static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	/**
	 * Initial state.
	 */
	public final String initialState;

	/**
	 * Map of name,state pairs for batched creates.
	 */
	public final Map<String, String> nameStates;

	private final Set<String> failedCreates;

	/* To specify a set of active replicas for the initial group. The initial
	 * group is by default chosen randomly. */
	private final Set<InetSocketAddress> initGroup;
	
	private final ReconfigureUponActivesChange policy;

	/**
	 * @param name
	 * @param state
	 */
	public CreateServiceName(String name, String state) {
		this(null, name, 0, state);
	}

	/**
	 * A constructor that allows the caller to specify an initial group. This
	 * method is meant primarily for internal use. End-clients should let the
	 * reconfigurators pick the initial set of replicas randomly by default.
	 * 
	 * @param name
	 * @param state
	 * @param initGroup
	 */
	public CreateServiceName(String name, String state,
			Set<InetSocketAddress> initGroup) {
		this(null, name, 0, state, null, null, initGroup);
	}

	/**
	 * @param name
	 * @param state
	 * @param policy
	 */
	public CreateServiceName(String name, String state,
			ReconfigureUponActivesChange policy) {
		this(null, name, 0, state, null, null, null, policy);
	}
	
	/**
	 * A constructor to specify both an initial group and a policy for
	 * reconfiguration upon addition or deletion of active replicas.
	 * 
	 * @param name
	 * @param state
	 * @param initGroup
	 * @param policy
	 */
	public CreateServiceName(String name, String state,
			Set<InetSocketAddress> initGroup,
			ReconfigureUponActivesChange policy) {
		this(null, name, 0, state, null, null, initGroup, policy);
	}

	private CreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state, Map<String, String> nameStates,
			InetSocketAddress myReceiver) {
		this(initiator, name, epochNumber, state, nameStates, myReceiver, null);
	}
	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param state
	 * @param nameStates
	 */
	private CreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state, Map<String, String> nameStates,
			InetSocketAddress myReceiver, Set<InetSocketAddress> initGroup) {
		this(initiator, name, epochNumber, state, nameStates, myReceiver, initGroup, null);
		
	}
	
	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param state
	 * @param nameStates
	 */
	private CreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state, Map<String, String> nameStates,
			InetSocketAddress myReceiver, Set<InetSocketAddress> initGroup,
			ReconfigureUponActivesChange policy) {
		super(initiator, ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME,
				name, epochNumber, myReceiver);
		this.initialState = state;
		this.nameStates = nameStates;
		this.failedCreates = null;
		this.initGroup = initGroup;
		
		this.policy = policy != null ? policy
				: ReconfigurationConfig.getDefaultReconfigureUponActivesChangePolicy();
	}

	// @TODO Change access to private after moving test to test source
	public CreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state, Map<String, String> nameStates) {
		this(initiator, name, epochNumber, state, nameStates, null);
	}

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param state
	 */
	protected CreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state) {
		this(initiator, name, epochNumber, state, null, null);
	}

	/**
	 * For internal use only.
	 * 
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param state
	 * @param myReceiver
	 */
	public CreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state, InetSocketAddress myReceiver) {
		this(initiator, name, epochNumber, state, null, myReceiver);
	}

	/**
	 * @param nameStates
	 */
	public CreateServiceName(Map<String, String> nameStates) {
		this(null, nameStates.keySet().iterator().next(), 0, nameStates
				.values().iterator().next(), nameStates);
	}

	/**
	 * @param nameStates
	 * @param policy
	 */
	public CreateServiceName(Map<String, String> nameStates,
			ReconfigureUponActivesChange policy) {
		this(null, nameStates.keySet().iterator().next(), 0, nameStates
				.values().iterator().next(), nameStates, null, null, policy);
	}

	/**
	 * FIXME: need to document the reliance on the consistent ordering of the
	 * head element in nameStates.
	 * 
	 * @param nameStates
	 * @param create
	 */
	public CreateServiceName(Map<String, String> nameStates,
			CreateServiceName create) {
		this(nameStates, null, create);
	}

	/**
	 * @param nameStates
	 * @param failedCreates
	 * @param create
	 */
	public CreateServiceName(Map<String, String> nameStates,
			Set<String> failedCreates, CreateServiceName create) {
		super(nameStates.keySet().iterator().next(), create);
		this.setSender(create.getSender());
		this.nameStates = nameStates;
		this.initialState = nameStates.get(nameStates.keySet().iterator()
				.next());
		this.failedCreates = failedCreates;
		this.initGroup = null;
		this.policy = create.policy;
	}

	/**
	 * @return {@code this}with only head name and state.
	 */
	public CreateServiceName getHeadOnly() {
		this.nameStates.clear();
		return this;
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public CreateServiceName(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, CreateServiceName.unstringer); // ignores unstringer
		// may not be true for String packet demultiplexers
		// assert (this.getSender() != null);
		this.initialState = json.has(Keys.STATE.toString())?
				json.getString(Keys.STATE.toString()) :
				json.has(ClientReconfigurationPacket.Keys.INITIAL_STATE.toString())?
						json.getString(ClientReconfigurationPacket.Keys.INITIAL_STATE.toString()) : null;
		// json.optString(Keys.STATE.toString(), null);
		this.nameStates = getNameStateMap(json);
		JSONArray jsonArray = json.has(Keys.FAILED_CREATES.toString()) ? json
				.getJSONArray(Keys.FAILED_CREATES.toString()) : null;
		if (jsonArray != null && jsonArray.length() > 0) {
			this.failedCreates = new HashSet<String>();
			for (int i = 0; i < jsonArray.length(); i++)
				this.failedCreates.add(jsonArray.getString(i));
		} else
			this.failedCreates = null;

		this.initGroup = json.has(Keys.INIT_GROUP.toString()) ? Util
				.getSocketAddresses(json.getJSONArray(Keys.INIT_GROUP
						.toString())) : null;
		this.policy = json.has(Keys.RECONFIGURE_UPON_ACTIVES_CHANGE.toString()) ?
				ReconfigurationConfig.ReconfigureUponActivesChange
				.valueOf(json.getString(Keys.RECONFIGURE_UPON_ACTIVES_CHANGE
						.toString()))
				:ReconfigurationConfig.getDefaultReconfigureUponActivesChangePolicy();
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public CreateServiceName(JSONObject json) throws JSONException {
		this(json, unstringer);
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		if (initialState != null)
			json.put(Keys.STATE.toString(), initialState);

		json.putOpt(BatchKeys.NAME_STATE_ARRAY.toString(),
				getNameStateJSONArray(this.nameStates));

		if (this.failedCreates != null && !this.failedCreates.isEmpty())
			json.put(Keys.FAILED_CREATES.toString(), this.failedCreates);
		if (this.initGroup != null)
			json.put(Keys.INIT_GROUP.toString(),
					Util.getJSONArray(this.initGroup));
		json.put(Keys.RECONFIGURE_UPON_ACTIVES_CHANGE.toString(), this.policy);
		return json;
	}

	/**
	 * @return True if this is a batched create request or response.
	 */
	public boolean isBatched() {
		return this.nameStates != null && !this.nameStates.isEmpty();
	}
	
	/**
	 * Returns the initGroup specified in this message.
	 * @return
	 */
	public Set<InetSocketAddress> getInitGroup()
	{
		return this.initGroup;
	}
	
	
	protected static JSONArray getNameStateJSONArray(
			Map<String, String> nameStates) throws JSONException {
		if (nameStates != null && !nameStates.isEmpty()) {
			JSONArray jsonArray = new JSONArray();
			for (String name : nameStates.keySet()) {
				JSONObject nameState = new JSONObject();
				nameState.put(Keys.NAME.toString(), name);
				nameState.put(Keys.STATE.toString(), nameStates.get(name));
				jsonArray.put(nameState);
			}
			return jsonArray;
		}
		return null;
	}

	protected static Map<String, String> getNameStateMap(JSONObject json)
			throws JSONException {
		if (!json.has(BatchKeys.NAME_STATE_ARRAY.toString()))
			return null;
		JSONArray nameStateArray = json.getJSONArray(BatchKeys.NAME_STATE_ARRAY
				.toString());
		Map<String, String> nameStates = new HashMap<String, String>();
		for (int i = 0; i < nameStateArray.length(); i++) {
			JSONObject nameState = nameStateArray.getJSONObject(i);
			String name = nameState.getString(Keys.NAME.toString());
			String state = nameState.has(Keys.STATE.toString()) ? nameState
					.getString(Keys.STATE.toString()) : null;
			if (name == null)
				throw new JSONException("Parsed null name in batched request");
			nameStates.put(name, state);
		}
		return nameStates;
	}

	/**
	 * @return Initial state.
	 */
	public String getInitialState() {
		return initialState;
	}

	/**
	 * @return Name, state tuple map.
	 */
	public Map<String, String> getNameStates() {
		return this.nameStates;
	}

	/**
	 * @return Number of creates in this request or respose.
	 */
	public int size() {
		return this.isBatched() ? this.nameStates.size() : 1;
	}

	public String getSummary() {
		return super.getSummary()
				+ (this.isBatched() ? ":|batched|=" + this.size() : "");
	}

	/**
	 * @param nameStates
	 * @param batchSize
	 * @return Array of batched CreateServiceName requests.
	 */
	public static CreateServiceName[] makeCreateNameRequest(
			Map<String, String> nameStates, int batchSize) {
		return ReconfigurationConfig.makeCreateNameRequest(nameStates,
				batchSize);
	}

	/**
	 * @param nameStates
	 * @param batchSize
	 * @param reconfigurators
	 * @return Array of batched CreateServiceName requests.
	 */
	public static CreateServiceName[] makeCreateNameRequest(
			Map<String, String> nameStates, int batchSize,
			Set<String> reconfigurators) {
		return ReconfigurationConfig.makeCreateNameRequest(nameStates,
				batchSize, reconfigurators);
	}
	
	/**
	 * @return {@link ReconfigureUponActivesChange} policy
	 */
	public ReconfigureUponActivesChange getReconfigureUponActivesChangePolicy() {
		return this.policy;
	}


}
