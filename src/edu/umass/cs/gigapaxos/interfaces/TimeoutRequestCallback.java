package edu.umass.cs.gigapaxos.interfaces;

/**
 * @author arun
 *
 */
public interface TimeoutRequestCallback extends RequestCallback {
	/**
	 * @return Timeout after which this callback will expire.
	 */
	public long getTimeout();

}
