package edu.umass.cs.txn.interfaces;

/**
 * @author arun
 *
 */
public interface Transactor {
	/**
	 * @param request
	 */
	public void transact(TXRequest request);
}
