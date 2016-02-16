package edu.umass.cs.gigapaxos.interfaces;

/**
 * @author arun
 *
 */
public interface ExecutedCallback {
	/**
	 * @param request
	 * @param handled
	 */
	public void executed(Request request, boolean handled);
}
