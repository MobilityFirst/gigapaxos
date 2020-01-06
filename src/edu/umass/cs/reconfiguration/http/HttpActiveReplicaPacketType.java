package edu.umass.cs.reconfiguration.http;

import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.Replicable;

/**
 * @author gaozy
 *
 */
public enum HttpActiveReplicaPacketType implements IntegerPacketType {
	/**
	 * For underlying app to execute
	 */
	EXECUTE(400),
	/**
	 * For underlying app to checkpoint a snapshot, not for the {@link Replicable} checkpoint
	 */
	SNAPSHOT(401),
	/**
	 * For underlying app to recover to the most recent state, not for the {@link Replicable} restore
	 */
	RECOVER(402),
	;

	/**
	 * 
	 */
	private static HashMap<Integer, HttpActiveReplicaPacketType> numbers = new HashMap<>();
	static {
		for (HttpActiveReplicaPacketType type : HttpActiveReplicaPacketType.values()) {
			if (!numbers.containsKey(type.number))
				numbers.put(type.number, type);
		}
	}
	
	/**
	 * @param t
	 * @return HttpPacketType
	 */
	public static HttpActiveReplicaPacketType getPacketType(int t) {
		return numbers.get(t);
	}
	
	@Override
	public int getInt() {
		return this.number;
	}

	private final int number;
	
	HttpActiveReplicaPacketType(int number){
		this.number = number;
	}
	
	/**
	 * Test
	 * @param args
	 */
	public static void main(String[] args) {
		HttpActiveReplicaPacketType type = HttpActiveReplicaPacketType.EXECUTE;
		assert(type == getPacketType(type.number));
		
		int n = 503;
		assert(getPacketType(n) == null);
	}
}
