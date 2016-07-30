package edu.umass.cs.gigapaxos.interfaces;


/**
 * @author arun
 *
 */
public interface RequestCallback extends Callback<Request> {
	/**
	 * @param response
	 */
	public void handleResponse(Request response);
}
