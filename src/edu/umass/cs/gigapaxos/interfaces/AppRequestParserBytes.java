package edu.umass.cs.gigapaxos.interfaces;

import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author arun
 *
 */
public interface AppRequestParserBytes {
	/**
	 * @param message
	 * @param header
	 * @return Request parsed from {@code message}.
	 * @throws RequestParseException 
	 */
	public Request getRequest(byte[] message, NIOHeader header) throws RequestParseException;
}
