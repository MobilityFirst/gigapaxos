package edu.umass.cs.gigapaxos.paxospackets;

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class BatchedAccept extends PaxosPacket {

	/**
	 * 
	 */
	public final Ballot ballot;

	private int medianCheckpointedSlot;

	private final TreeMap<Integer, byte[]> slotDigests = new TreeMap<Integer, byte[]>();

	private final TreeMap<Integer, Long> slotRequestIDs = new TreeMap<Integer, Long>();

//	private final TreeMap<Integer, Integer> slotBatchSizes = new TreeMap<Integer, Integer>();

	private final Set<Integer> group;

	private static final String CHARSET = "ISO-8859-1";

	/**
	 * @param accept
	 * @param group
	 */
	public BatchedAccept(AcceptPacket accept, Set<Integer> group) {
		super(accept);
		this.packetType = PaxosPacketType.BATCHED_ACCEPT;
		this.ballot = accept.ballot;
		this.slotDigests.put(accept.slot, (accept.getDigest(null)));
		this.slotRequestIDs.put(accept.slot, accept.requestID);
//		this.slotBatchSizes.put(accept.slot, accept.batchSize());
		this.group = group;
		this.medianCheckpointedSlot = accept.getMedianCheckpointedSlot();
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public BatchedAccept(JSONObject json) throws JSONException {
		super(json);
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.B
				.toString()));
		this.medianCheckpointedSlot = json.getInt(PaxosPacket.Keys.GC_S
				.toString());
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.group = new HashSet<Integer>();

		JSONArray jsonArray = json.getJSONArray(PaxosPacket.NodeIDKeys.GROUP
				.toString());
		for (int i = 0; i < jsonArray.length(); i++)
			this.group.add(jsonArray.getInt(i));
		
		// slotDigests
		jsonArray = json.getJSONArray(PaxosPacket.Keys.S_DIGS.toString());
		for (int i = 0; i < jsonArray.length(); i++)
			try {
				assert(jsonArray.get(0) instanceof JSONArray) : jsonArray.get(0);
				this.slotDigests.put(jsonArray.getJSONArray(i).getInt(0),
						jsonArray.getJSONArray(i).getString(1)
								.getBytes(CHARSET));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		
		// slotRequestIDs
		jsonArray = json.getJSONArray(PaxosPacket.Keys.S_QIDS.toString());
		for (int i = 0; i < jsonArray.length(); i++)
			this.slotRequestIDs.put(jsonArray.getJSONArray(i).getInt(0), jsonArray
					.getJSONArray(i).getLong(1));
		
		// slotBatchSizes
//		jsonArray = json.getJSONArray(PaxosPacket.Keys.S_BS.toString());
//		for (int i = 0; i < jsonArray.length(); i++)
//			this.slotBatchSizes.put(jsonArray.getJSONArray(i).getInt(0), jsonArray
//					.getJSONArray(i).getInt(1));

	}

	@Override
	protected JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.GC_S.toString(), this.medianCheckpointedSlot);
		json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), this.group);

		// slotDigests
		JSONArray jsonArray = new JSONArray();
		for (Integer s : this.slotDigests.keySet()) {
			JSONArray jarray = new JSONArray();
			jarray.put(s);
			try {
				jarray.put(new String(this.slotDigests.get(s), CHARSET));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			jsonArray.put(jarray);
		}
		json.put(PaxosPacket.Keys.S_DIGS.toString(), jsonArray);
		
		// slotRequestIDs
		jsonArray = new JSONArray();
		for (Integer s : this.slotRequestIDs.keySet()) {
			JSONArray jarray = new JSONArray();
			jarray.put(s);
			jarray.put(this.slotRequestIDs.get(s));
			jsonArray.put(jarray);
		}
		json.put(PaxosPacket.Keys.S_QIDS.toString(), jsonArray);

//		// slotBatchSizes
//		jsonArray = new JSONArray();
//		for (Integer s : this.slotBatchSizes.keySet()) {
//			JSONArray jarray = new JSONArray();
//			jarray.put(s);
//			jarray.put(this.slotBatchSizes.get(s));
//			jsonArray.put(jarray);
//		}
//		json.put(PaxosPacket.Keys.S_BS.toString(), jsonArray);

		return json;
	}

	
	@Override
	protected net.minidev.json.JSONObject toJSONSmartImpl() throws JSONException {
		net.minidev.json.JSONObject json = new net.minidev.json.JSONObject();
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.GC_S.toString(), this.medianCheckpointedSlot);
		json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), this.group);

		// slotDigests
		net.minidev.json.JSONArray jsonArray = new net.minidev.json.JSONArray();
		for (Integer s : this.slotDigests.keySet()) {
			net.minidev.json.JSONArray jarray = new net.minidev.json.JSONArray();
			jarray.add(s);
			try {
				jarray.add(new String(this.slotDigests.get(s), CHARSET));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			jsonArray.add(jarray);
		}
		json.put(PaxosPacket.Keys.S_DIGS.toString(), jsonArray);
		
		// slotRequestIDs
		jsonArray = new net.minidev.json.JSONArray();
		for (Integer s : this.slotRequestIDs.keySet()) {
			net.minidev.json.JSONArray jarray = new net.minidev.json.JSONArray();
			jarray.add(s);
			jarray.add(this.slotRequestIDs.get(s));
			jsonArray.add(jarray);
		}
		json.put(PaxosPacket.Keys.S_QIDS.toString(), jsonArray);

//		// slotBatchSizes
//		jsonArray = new net.minidev.json.JSONArray();
//		for (Integer s : this.slotBatchSizes.keySet()) {
//			net.minidev.json.JSONArray jarray = new net.minidev.json.JSONArray();
//			jarray.add(s);
//			jarray.add(this.slotBatchSizes.get(s));
//			jsonArray.add(jarray);
//		}
//		json.put(PaxosPacket.Keys.S_BS.toString(), jsonArray);

		return json;
	}

	/**
	 * @return Median checkpointed slot.
	 */
	public int getMedianCheckpointedSlot() {
		return this.medianCheckpointedSlot;
	}

	/**
	 * @param bAccept
	 * @return True
	 */
	public boolean addBatchedAccept(BatchedAccept bAccept) {
		if (!bAccept.ballot.equals(this.ballot)
				|| !bAccept.paxosID.equals(this.paxosID))
			throw new RuntimeException("Unable to combine "
					+ bAccept.getSummary() + " with " + this.getSummary());
		if (bAccept.getMedianCheckpointedSlot() - this.medianCheckpointedSlot > 0)
			this.medianCheckpointedSlot = bAccept.getMedianCheckpointedSlot();
		this.slotDigests.putAll(bAccept.slotDigests);
		this.slotRequestIDs.putAll(bAccept.slotRequestIDs);
//		this.slotBatchSizes.putAll(bAccept.slotBatchSizes);
		return true;
	}

	/**
	 * @return Group as int[].
	 */
	public int[] getGroup() {
		return Util.setToIntArray(this.group);
	}

	/**
	 * @return Group as set.
	 */
	public Set<Integer> getGroupSet() {
		return this.group;
	}

	/**
	 * @param accept
	 */
	public void addAccept(AcceptPacket accept) {
		if (!accept.ballot.equals(this.ballot)
				|| !accept.paxosID.equals(this.paxosID))
			throw new RuntimeException("Unable to combine "
					+ accept.getSummary() + " with " + this.getSummary());
		if (accept.getMedianCheckpointedSlot() - this.medianCheckpointedSlot > 0)
			this.medianCheckpointedSlot = accept.getMedianCheckpointedSlot();
		this.slotDigests.put(accept.slot, accept.getDigest(null));
		this.slotRequestIDs.put(accept.slot, accept.requestID);
//		this.slotBatchSizes.put(accept.slot, accept.batchSize());
	}
	
	protected String getSummaryString() {
		return this.ballot + ":" + this.slotDigests.keySet();
	}

	/**
	 * @return Accept slots.
	 */
	public Integer[] getAcceptSlots() {
		return this.slotDigests.keySet().toArray(new Integer[0]);
	}

	/**
	 * @param slot
	 * @return Request ID.
	 */
	public Long getRequestID(Integer slot) {
		return this.slotRequestIDs.get(slot);
	}

	/**
	 * @param slot
	 * @return Digest.
	 */
	public byte[] getDigest(Integer slot) {
		return this.slotDigests.get(slot);
	}
	/**
	 * @return Size.
	 */
	public int size() {
		return this.slotDigests.size();
	}

//	/**
//	 * @param slot
//	 * @return Batch size for slot
//	 */
//	public int batchSize(int slot) {
//		return this.slotBatchSizes.containsKey(slot) ? this.slotBatchSizes
//				.get(slot) : 0;
//	}
}
