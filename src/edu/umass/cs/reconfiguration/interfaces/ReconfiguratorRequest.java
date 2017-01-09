package edu.umass.cs.reconfiguration.interfaces;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ServerReconfigurationPacket;

/**
 * @author arun
 * 
 *         An interface for methods common to
 *         {@link ClientReconfigurationPacket} and
 *         {@link ServerReconfigurationPacket}.
 *
 */
public interface ReconfiguratorRequest extends Request {

	/**
	 * @return {@code this}
	 */
	public abstract ReconfiguratorRequest setFailed();

	/**
	 * @return True if failed.
	 */
	public abstract boolean isFailed();

	/**
	 * @param msg
	 * @return {@code this}
	 */
	public abstract ReconfiguratorRequest setResponseMessage(String msg);

	/**
	 * @return The success or failure message.
	 */
	public abstract String getResponseMessage();

}