package edu.umass.cs.txn.interfaces;

import java.util.SortedSet;

/**
 * @author arun
 *
 */
public interface TXRequest extends Iterable<TxOp> {
	/**
	 * @return Sorted list of lock IDs.
	 */
	public SortedSet<String> getLockList();
}
