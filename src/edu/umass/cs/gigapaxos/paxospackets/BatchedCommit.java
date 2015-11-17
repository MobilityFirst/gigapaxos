package edu.umass.cs.gigapaxos.paxospackets;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class BatchedCommit extends PaxosPacket {

	/**
	 * 
	 */
	public final Ballot ballot;

	private int medianCheckpointedSlot;

	private final TreeSet<Integer> slots = new TreeSet<Integer>();
	
	private final Set<Integer> group;

	/**
	 * @param pvalue
	 * @param group
	 */
	public BatchedCommit(PValuePacket pvalue, Set<Integer> group) {
		super(pvalue);
		this.slots.add(pvalue.slot);
		this.ballot = pvalue.ballot;
		this.packetType = PaxosPacket.PaxosPacketType.BATCHED_COMMIT;
		this.group = group;
		this.medianCheckpointedSlot = pvalue.getMedianCheckpointedSlot();
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public BatchedCommit(JSONObject json) throws JSONException {
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

		jsonArray = json.getJSONArray(PaxosPacket.Keys.SLOTS.toString());
		for (int i = 0; i < jsonArray.length(); i++)
			this.slots.add(jsonArray.getInt(i));
	}

	/**
	 * @param bCommit
	 * @return True if no entry already present for the slot.
	 */
	public boolean addBatchedCommit(BatchedCommit bCommit) {
		if (!bCommit.ballot.equals(this.ballot)
				|| !bCommit.paxosID.equals(this.paxosID))
			throw new RuntimeException("Unable to combine "
					+ bCommit.getSummary() + " with " + this.getSummary());
		if(bCommit.getMedianCheckpointedSlot() - this.medianCheckpointedSlot > 0)
			this.medianCheckpointedSlot = bCommit.getMedianCheckpointedSlot();
		return this.slots.addAll(bCommit.slots);
	}

	/**
	 * @param commit
	 * @return True if no entry already present for the slot.
	 */
	public boolean addCommit(PValuePacket commit) {
		if (!commit.ballot.equals(this.ballot)
				|| !commit.paxosID.equals(this.paxosID))
			throw new RuntimeException("Unable to combine "
					+ commit.getSummary() + " with " + this.getSummary());
		if(commit.getMedianCheckpointedSlot() - this.medianCheckpointedSlot > 0)
			this.medianCheckpointedSlot = commit.getMedianCheckpointedSlot();
		return this.slots.add(commit.slot);
	}

	/**
	 * @return Committed slots.
	 */
	public Integer[] getCommittedSlots() {
		return this.slots.toArray(new Integer[0]);
	}
	

	@Override
	protected JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.GC_S.toString(),
				this.medianCheckpointedSlot);
		json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), this.group);
		json.put(PaxosPacket.Keys.SLOTS.toString(), this.slots);
		// values must be sorted in slot order
		return json;
	}

	@Override
	protected net.minidev.json.JSONObject toJSONSmartImpl() throws JSONException {
		net.minidev.json.JSONObject json = new net.minidev.json.JSONObject();
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.GC_S.toString(),
				this.medianCheckpointedSlot);
		json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), this.group);
		json.put(PaxosPacket.Keys.SLOTS.toString(), this.slots);
		// values must be sorted in slot order
		return json;
	}

	/**
	 * @return Median checkpointed slot.
	 */
	public int getMedianCheckpointedSlot() {
		return this.medianCheckpointedSlot;
	}

	/**
	 * @param slot
	 */
	public void setMedianCheckpointedSlot(int slot) {
		this.medianCheckpointedSlot = slot;
	}

	@Override
	protected String getSummaryString() {
		return this.ballot + ":" + this.slots;
	}

	/**
	 * @return Group members as int[].
	 */
	public int[] getGroup() {
		return Util.setToIntArray(this.group);
	}
	
	/**
	 * @return Group members as a set.
	 */
	public Set<Integer> getGroupSet() {
		return this.group;
	}
	
	/**
	 * @return Size.
	 */
	public int size() {
		return this.slots.size();
	}
}
