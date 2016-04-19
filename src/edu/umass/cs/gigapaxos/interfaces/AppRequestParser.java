package edu.umass.cs.gigapaxos.interfaces;

import java.util.Set;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author arun
 *
 */
public interface AppRequestParser {
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

}
