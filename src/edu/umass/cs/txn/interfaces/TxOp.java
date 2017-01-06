package edu.umass.cs.txn.interfaces;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;

/**
 * @author arun
 * 
 *         Specifies an interface for an individual transaction operation. A
 *         TxOp request must either be {@link ClientRequest} or a
 *         {@link ClientReconfigurationPacket}.
 */
public interface TxOp extends Request {

	/**
	 * @param response
	 * @return True if the response indicates that this operation was executed
	 *         successfully and the transaction can proceed to the next
	 *         operation; else false meaning that the transaction must be
	 *         aborted with a rollback.
	 */
	public boolean handleResponse(Request response);

	/**
	 * @return True if this operation must block until finished.
	 */
	default boolean isBlocking() {
		return true;
	}

	/**
	 * @return True if executing it multiple times has the same effect as
	 *         executing it exactly once.
	 */
	default boolean isIdempotent() {
		return false;
	}
}
