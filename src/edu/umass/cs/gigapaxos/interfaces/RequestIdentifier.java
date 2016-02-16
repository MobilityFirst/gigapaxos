package edu.umass.cs.gigapaxos.interfaces;

/**
 * @author arun
 *
 */
public interface RequestIdentifier {
	/**
	 * The uniqueness of this identifier across all requests to a replica group
	 * is important for safety.
	 * 
	 * @return Unique request identifier.
	 */
	public long getRequestID();
}
