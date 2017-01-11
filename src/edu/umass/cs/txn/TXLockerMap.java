package edu.umass.cs.txn;

import edu.umass.cs.txn.exceptions.TXException;
import edu.umass.cs.txn.interfaces.TXLocker;

/**
 * @author arun
 *
 */
public class TXLockerMap implements TXLocker {

	/**
	 * A blocking call that returns upon successfully locking {@code lockID} or
	 * throws a {@link TXException}. Locking a group involves synchronously
	 * checkpointing its state and maintaining in memory its locked status.
	 * 
	 * @param lockID
	 * @throws TXException
	 */
	@Override
	public void lock(String lockID) throws TXException {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * Acquires the locks in the order specified by {@code lockIDs}.
	 * 
	 * @param lockIDs
	 * @throws TXException
	 */
	public void lock(String[] lockIDs) throws TXException {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * A blocking call that returns upon successfully release {@code lockID} or
	 * throws a {@link TXException} .
	 * 
	 * @param lockID
	 * @throws TXException
	 */
	public void unlock(String lockID) throws TXException {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * Releases the locks in the order specified by {@code lockIDs}.
	 * 
	 * @param lockIDs
	 * @throws TXException
	 */
	public void unlock(String[] lockIDs) throws TXException {
		throw new RuntimeException("Unimplemented");
	}

}
