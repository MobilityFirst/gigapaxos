package edu.umass.cs.gigapaxos.interfaces;

/**
 * @author arun
 *
 * @param <T>
 */
public interface Callback<T> {
	/**
	 * @param response
	 */
	public void handleResponse(T response);
}
