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
	 * App must support string-to-InterfaceRequest conversion and back.
	 * Furthermore, the conversion to a string and back must preserve the return
	 * values of all InterfaceRequest methods, i.e.,
	 * {@code InterfaceApplication.getRequest(request.toString())).getRequestType =
	 * request.getRequestType()} ... and so on
	 * 
	 * @param stringified
	 * @return InterfaceRequest corresponding to {@code stringified}.
	 * @throws RequestParseException
	 */
	public Request getRequest(String stringified) throws RequestParseException;
	
	/**
	 * @return The set of request types that the application expects to process.
	 */
	public Set<IntegerPacketType> getRequestTypes();

}
