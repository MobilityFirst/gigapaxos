package edu.umass.cs.txn.interfaces;

import edu.umass.cs.txn.exceptions.TXException;

/**
 * @author arun
 *
 */
public interface TXLocker {
	/**
	 * A blocking call that returns upon successfully locking {@code lockID}
	 * or throws a {@link TXException}. Locking a group involves synchronously
	 * checkpointing its state and maintaining in memory its locked status.
	 * 
	 * @param lockID
	 * @throws TXException
	 */
	public void lock(String lockID) throws TXException;

}
