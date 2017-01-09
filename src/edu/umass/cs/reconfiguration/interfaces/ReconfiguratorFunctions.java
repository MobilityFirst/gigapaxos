package edu.umass.cs.reconfiguration.interfaces;

import edu.umass.cs.gigapaxos.async.RequestCallbackFuture;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.Reconfigurator;

/**
 * @author arun
 * 
 *         A minimal interface defining reconfigurator server functions. This
 *         interface is implemented by {@link Reconfigurator} and must be
 *         implemented by any client or proxy for reconfigurators.
 *
 */
public interface ReconfiguratorFunctions {

	/**
	 * @param request
	 * @param callback
	 * 
	 * @return A {@link RequestCallbackFuture} object that can be used by the
	 *         caller to retrieve the processed response.
	 */
	public RequestCallbackFuture<ReconfiguratorRequest> sendRequest(
			ReconfiguratorRequest request,
			Callback<Request, ReconfiguratorRequest> callback);

	/**
	 * @param request
	 * @return The response to {@code request} that will have the same type as
	 *         the request.
	 */
	public ReconfiguratorRequest sendRequest(ReconfiguratorRequest request);

}
