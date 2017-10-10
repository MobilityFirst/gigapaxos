package edu.umass.cs.reconfiguration.reconfigurationpackets;

import edu.umass.cs.nio.interfaces.Stringifiable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;

/**
 * @author arun
 *
 * This class is not used.
 */
public class BatchedCreateServiceName extends CreateServiceName {

	protected static enum BatchKeys {
		NAME_STATE_ARRAY
	};

	final String[] initialStates;
	final String[] names;

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param state
	 * @TODO Change access to private after moving test to test source
	 */
	public BatchedCreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state) {
		super(initiator, name, epochNumber, state);
		this.initialStates = new String[1];
		this.initialStates[0] = state;
		assert (epochNumber == 0);
		this.names = new String[1];
		this.names[0] = name;
	}

	/**
	 * @param initiator
	 * @param names
	 * @param epochNumber
	 * @param states
	 * @TODO Change access to private after moving test to test source
	 */
	public BatchedCreateServiceName(InetSocketAddress initiator,
			String[] names, int epochNumber, String[] states) {
		super(initiator, names[0], epochNumber, states[0]);
		assert (epochNumber == 0);
		this.initialStates = states;
		this.names = names;
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public BatchedCreateServiceName(JSONObject json) throws JSONException {
		this(json, unstringer);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public BatchedCreateServiceName(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, unstringer);
		JSONArray nameStateArray = json.has(BatchKeys.NAME_STATE_ARRAY
				.toString()) ? json.getJSONArray(BatchKeys.NAME_STATE_ARRAY
				.toString()) : new JSONArray();
		this.names = new String[nameStateArray.length()];
		this.initialStates = new String[nameStateArray.length()];
		for (int i = 0; i < nameStateArray.length(); i++) {
			JSONObject nameState = nameStateArray.getJSONObject(i);
			this.names[i] = nameState
					.getString(BasicReconfigurationPacket.Keys.NAME
							.toString());
			this.initialStates[i] = nameState.getString(Keys.STATE
					.toString());
		}
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		JSONArray jsonArray = new JSONArray();
		for (int i = 0; i < names.length; i++) {
			JSONObject nameState = new JSONObject();
			nameState.put(
					BasicReconfigurationPacket.Keys.NAME.toString(),
					this.names[i]);
			nameState.put(Keys.STATE.toString(), this.initialStates[i]);
			jsonArray.put(nameState);
		}
		json.put(BatchKeys.NAME_STATE_ARRAY.toString(), jsonArray);
		return json;
	}
	
	public String getSummary() {
		return super.getSummary() + ":|batched|=" + this.names.length;
	}

}
