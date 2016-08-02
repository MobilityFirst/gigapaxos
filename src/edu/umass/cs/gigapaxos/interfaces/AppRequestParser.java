package edu.umass.cs.gigapaxos.interfaces;

import java.io.UnsupportedEncodingException;
import java.util.Set;

import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author arun
 *
 */
public interface AppRequestParser extends AppRequestParserBytes {
	/**
	 * An application must support string-to-{@link Request} conversion and
	 * back. Furthermore, the conversion to a string and back must preserve the
	 * return values of all {@link Request} methods, i.e.,
	 * {@link Application#getRequest(String) Application.getRequest}
	 * {@code (request.toString())).equals(request)} must be true.
	 * 
	 * @param stringified
	 * @return {@link Request} corresponding to {@code stringified}.
	 * @throws RequestParseException
	 */
	public Request getRequest(String stringified) throws RequestParseException;

	/**
	 * @return The set of request types that the application expects to process.
	 */
	public Set<IntegerPacketType> getRequestTypes();

	/**
	 * @return The set of request types that the application expects to process
	 *         iff received over SERVER_AUTH SSL.
	 */
	default Set<IntegerPacketType> getServerAuthRequestTypes() {
		return getRequestTypes();
	}

	/**
	 * @return The set of request types that the application expects to process
	 *         iff received only over MUTUAL_AUTH SSL. These request types will
	 *         not be processed if received over the CLEAR or SERVER_AUTH
	 *         client-facing ports.
	 */
	default Set<IntegerPacketType> getMutualAuthRequestTypes() {
		return null;
	}

	/**
	 * The implementation of this method is tied to and must be the inverse of
	 * {@link Request#toBytes()}, , i.e., invoking getRequest(request.toBytes(),
	 * header).equals(request).
	 */
	default Request getRequest(byte[] message, NIOHeader header)
			throws RequestParseException {
		try {
			return this.getRequest(new String(message,
					MessageNIOTransport.NIO_CHARSET_ENCODING));
		} catch (UnsupportedEncodingException | RequestParseException e) {
			throw new RequestParseException(e);
		}
	}
}
