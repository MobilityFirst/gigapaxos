package edu.umass.cs.txn.exceptions;

/**
 * @author arun
 *
 */
public class TXException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6241599955379004041L;

	private final ResponseCode code;

	/**
	 * @param code
	 * @param message
	 */
	public TXException(ResponseCode code, String message) {
		super(message);
		this.code = code;
	}

	/**
	 * @param code
	 * @param message
	 * @param e
	 */
	public TXException(ResponseCode code, String message, Throwable e) {
		super(message, e);
		this.code = code;
	}

	/**
	 * @param code
	 * @param e
	 */
	public TXException(ResponseCode code, Throwable e) {
		super(e);
		this.code = code;
	}

	/**
	 * @param e
	 */
	public TXException(TXException e) {
		this(e.code, e);
	}

	/**
	 * @return Response code.
	 */
	public ResponseCode getCode() {
		return this.code;
	}

}
