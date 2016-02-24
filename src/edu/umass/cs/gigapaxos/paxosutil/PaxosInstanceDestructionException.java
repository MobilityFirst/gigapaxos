package edu.umass.cs.gigapaxos.paxosutil;

/**
 * @author arun
 *
 */
public class PaxosInstanceDestructionException extends RuntimeException {
	static final long serialVersionUID = 0;

	/**
	 * @param msg
	 */
	public PaxosInstanceDestructionException(String msg) {
		super(msg);
	}
}
