package edu.umass.cs.reconfiguration.interfaces;

import java.io.IOException;
import java.net.InetSocketAddress;

import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.NearestServerSelector;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;

/**
 * @author arun
 * 
 *         This interface should be implemented by any gigapaxos client
 *         implementation.
 * @param <V> 
 */
public interface GigaPaxosClient<V> {
	/**
	 * A blocking method to retrieve the result of executing {@code request}.
	 * 
	 * @param request
	 * @return The response obtained by executing {@code request}.
	 * @throws IOException
	 */
	public Request sendRequest(Request request) throws IOException;

	/**
	 * This method will automatically convert {@code request} to 
	 * {@link ClientRequest} via {@link ReplicableClientRequest} if necessary.
	 * 
	 * @param request
	 * @param callback
	 * @return Refer {@link #sendRequest(ClientRequest, Callback)}.
	 * @throws IOException
	 */
	public RequestFuture<V> sendRequest(Request request, Callback<Request, V> callback)
			throws IOException;

	/**
	 * @param request
	 * @param callback
	 * @return The long request identifier of the request if sent successfully;
	 *         null otherwise.
	 * @throws IOException
	 */
	public RequestFuture<V> sendRequest(ClientRequest request, Callback<Request, V> callback)
			throws IOException;

	/**
	 * Sends {@code request} to the nearest server as determined by
	 * {@code redirector}, an interface that returns the nearest server from a
	 * set of server addresses.
	 * 
	 * @param request
	 * @param callback
	 * @param redirector
	 * @return Refer {@link #sendRequest(ClientRequest, Callback)}.
	 * @throws IOException
	 */
	public RequestFuture<V> sendRequest(ClientRequest request,
			Callback<Request,V> callback, NearestServerSelector redirector)
			throws IOException;

	/**
	 * Sends {@code request} to the specified {@code server}.
	 * 
	 * @param request
	 * @param server
	 * @param callback
	 * @return Refer {@link #sendRequest(ClientRequest, Callback)}.
	 * @throws IOException
	 */
	public RequestFuture<V> sendRequest(ClientRequest request, InetSocketAddress server,
			Callback<Request,V> callback) throws IOException;
}
