package edu.umass.cs.gigapaxos.interfaces;

/**
 * @author arun
 *
 */
public interface Summarizable {
	/**
	 * Implementations are encouraged to return an inline created new Object()
	 * whose toString() method will return the actual String instead of just
	 * returning the String directly. This helps loggers be more efficient as
	 * the String won't be generated until the log level demands it.
	 * 
	 * @return An object whose toString method will return a summary of
	 *         {@code this} object.
	 */
	public Object getSummary();
}
