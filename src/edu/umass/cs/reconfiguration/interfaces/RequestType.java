package edu.umass.cs.reconfiguration.interfaces;

import edu.umass.cs.nio.interfaces.IntegerPacketType;

/**
 * @author arun
 *
 */
public interface RequestType extends IntegerPacketType {
	/**
	 * @return True if requests of this type, by default, need to be coordinated
	 *         or are meant to be executed locally.
	 */
	default public boolean needsCoordination() {
		return false;
	}

	/**
	 * @return True if this request needs to contact another active replica in
	 *         order to complete execution. This flag is different from
	 *         {@link #needsCoordination()} above in that the former implies a
	 *         statically determined replica coordination protocol while this
	 *         method may involve, say, looking up or writing to a some remote
	 *         data in a manner specific to executing this request.
	 */
	default public boolean isNonLocal() {
		return false;
	}

	/**
	 * @return The array of requests that are executed (in order) in order to
	 *         execute this request. A member of the returned array may itself
	 *         return a non-null set when subRequests() is invoked upon it. A
	 *         primitive request, i.e., one with no sub-requests, may return
	 *         either null or an empty array.
	 */
	default public RequestType[] subRequests() {
		return null;
	}

	/**
	 * @return True if {@link #subRequests()} returns a nonempty array of
	 *         requests all of which must be executed as a transaction, i.e.,
	 *         with atomicity and isolation. Note that the "C" and "D" in ACID
	 *         are properties of the replica coordination protocol, so
	 *         {@link #isTransaction()} does not imply durability.
	 */
	default public boolean isTransaction() {
		return false;
	}
}
