package edu.umass.cs.gigapaxos.interfaces;

/**
 * @author arun
 * 
 * @param <R>
 * @param <V>
 *
 */
public interface Callback<R, V> {
	/**
	 * @param response
	 * @return Value returned by processing {@code response}.
	 */
	public V processResponse(R response);
}
