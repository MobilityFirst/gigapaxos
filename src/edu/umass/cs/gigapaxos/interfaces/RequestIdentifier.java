package edu.umass.cs.gigapaxos.interfaces;


/**
 * @author arun
 *
 */
public interface RequestIdentifier {
	/**
	 * The uniqueness of this identifier across all requests to a replica group
	 * is important for safety when
	 * {@link edu.umass.cs.gigapaxos.PaxosConfig.PC#PREVENT_DOUBLE_EXECUTION} is
	 * enabled as gigapaxos may otherwise assume that the duplicate request
	 * identifier corresponds to a retransmission and will send back the
	 * response if any corresponding to the first execution. With
	 * {@link edu.umass.cs.gigapaxos.PaxosConfig.PC#PREVENT_DOUBLE_EXECUTION}
	 * disabled, the uniqueness is important for liveness as duplicate request
	 * identifiers may result in the client not receiving a response for the
	 * latter request; this is because gigapaxos stores request execution
	 * callbacks indexed by the request identifier, so there must be at most one
	 * outstanding request with a given identifier for a given replica group.
	 * 
	 * @return A unique request identifier for this request.
	 */
	public long getRequestID();
}
