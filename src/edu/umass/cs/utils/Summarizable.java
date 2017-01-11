package edu.umass.cs.utils;

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
	
	/**
	 * @param log
	 * @return Summary if {@code log} is true.
	 */
	default Object getSummary(boolean log) {
		if(log) return getSummary();
		else return null;
	}
}
