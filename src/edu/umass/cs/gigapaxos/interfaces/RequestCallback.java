package edu.umass.cs.gigapaxos.interfaces;

/**
 * @author arun
 *
 */
public interface RequestCallback extends Callback<Request, Request> {
	/**
	 * @param response
	 */
	public void handleResponse(Request response);

	@Override
	default Request processResponse(Request response) {
		this.handleResponse(response);
		return null;
	}
}
