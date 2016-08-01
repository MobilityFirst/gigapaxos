package edu.umass.cs.reconfiguration.http;

import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;

/**
 * @author arun
 *
 */
public class HTTPException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8942725299426579324L;
	final ClientReconfigurationPacket.ResponseCodes code;

	HTTPException(ClientReconfigurationPacket.ResponseCodes code, String message) {
		super(message);
		this.code = code;
	}

	HTTPException(String message) {
		super(message);
		this.code = null;
	}
}
