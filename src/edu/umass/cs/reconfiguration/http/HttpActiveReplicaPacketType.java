package edu.umass.cs.reconfiguration.http;

import java.util.HashMap;

import edu.umass.cs.nio.interfaces.IntegerPacketType;

/**
 * @author gaozy
 *
 */
public enum HttpActiveReplicaPacketType implements IntegerPacketType {
	/**
	 * For underlying app to execute
	 */
	EXECUTE(400),
	SNAPSHOT(401),
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
