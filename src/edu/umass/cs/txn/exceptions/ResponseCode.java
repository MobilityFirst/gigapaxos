package edu.umass.cs.txn.exceptions;

/**
 * @author arun
 * 
 *         Response codes for transaction operations.
 */
public enum ResponseCode {

	/**
	 * Indicates that a lock acquisition attempt failed.
	 */
	LOCK_FAILURE(11),

	/**
	 * Indicates that a lock release attempt failed.
	 * 
	 */
	UNLOCK_FAILURE(12),

	/**
	 * 
	 */
	IOEXCEPTION(13),

	/**
	 * Indicates that an individual transaction operation failed.
	 */
	TXOP_FAILURE(14),

	/**
	 * A commit failed either because of IO or other failures or because the
	 * transaction was already aborted. Attempting either a commit (again) or an
	 * abort in response to a commit failure is safe. If the commit failed
	 * because the transaction was already aborted, it is best to do nothing.
	 */
	COMMIT_FAILURE(15),

	/**
	 * An abort failed either because of IO or other failures or because the
	 * transaction was already committed. Attempting either a commit (again) or
	 * an abort in response to a commit failure is safe. If the abort failed
	 * because the transaction was already committed, it is best to do nothing.
	 */
	ABORT_FAILURE(16),

	;

	final int code;

	ResponseCode(int code) {
		this.code = code;
	}
}
