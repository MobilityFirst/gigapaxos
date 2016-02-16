package edu.umass.cs.gigapaxos.interfaces;

import java.net.InetSocketAddress;

/**
 * @author arun
 *
 *         An interface that simplifies messaging of responses back to clients
 *         that the corresponding requests.
 */
public interface ClientRequest extends Request, RequestIdentifier {
	/**
	 * @return The socket address of the client that sent this request.
	 */
	@Deprecated
	public InetSocketAddress getClientAddress();

	/**
	 * @return The response to be sent back to the client that issued this
	 *         request.
	 */
	public ClientRequest getResponse();
}
