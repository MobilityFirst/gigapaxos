package edu.umass.cs.gigapaxos.paxosutil;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.PaxosManager.RequestAndCallback;
import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class PendingDigests {

	final ConcurrentHashMap<Long, AcceptPacket> accepts;
	final Map<Long, RequestAndCallback> requests;
	final PendingDigestCallback callback;

	private static final MessageDigest[] mds = new MessageDigest[Config.getGlobalInt(PC.NUM_MESSAGE_DIGESTS)];
    static {
        for (int i = 0; i < mds.length; i++)
            try {
                mds[i] = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                Util.suicide("Unable to initialize MessageDigest; exiting");
            }
    }


	private static final long ACCEPT_TIMEOUT = Config.getGlobalLong(PC.ACCEPT_TIMEOUT);
	
	/**
	 *
	 */
	public static abstract class PendingDigestCallback {
		/**
		 * @param accept
		 */
		abstract public void callback(AcceptPacket accept);
	}
	/**
	 * @param rcs 
	 * @param numMDs
	 * @param callback 
	 */
	public PendingDigests(Map<Long, RequestAndCallback> rcs, int numMDs, PendingDigestCallback callback) {
		this.requests = rcs;
		//this.mds = new MessageDigest[numMDs];
		this.callback = callback;
		this.accepts = (ACCEPT_TIMEOUT == Integer.MAX_VALUE ? new ConcurrentHashMap<Long, AcceptPacket>()
				: new GCConcurrentHashMap<Long, AcceptPacket>(
						new GCConcurrentHashMapCallback() {

							@Override
							public void callbackGC(Object key, Object value) {
								PendingDigests.this.callback.callback((AcceptPacket) value);
							}

						}, ACCEPT_TIMEOUT));
		try {
			for (int i = 0; i < mds.length; i++)
				mds[i] = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			Util.suicide("Unable to initialize MessageDigest; exiting");
		}
	}

	/**
	 * @param accept
	 * @return Accept packet constructed from matching request if any.
	 */
	public AcceptPacket match(AcceptPacket accept) {
		RequestAndCallback rc = null;

		synchronized (this.requests) {
			if ((rc = requests.get(accept.requestID)) == null) 
				this.accepts.put(accept.requestID, accept);
		}

		if (rc != null && rc.getRequestPacket().getPaxosID().equals(accept.getPaxosID())) {
			if (rc.getRequestPacket().digestEquals(accept,
					mds[(int) (Math.random() * mds.length)])) {
				accept = accept.undigest(rc.getRequestPacket());
				assert (accept.hasRequestValue());
				return accept;
			} else
				logAnomaly(rc.getRequestPacket(), accept);
		}

		return null;
	}

	/**
	 * @param request
	 * @param remove
	 * @return Released accept.
	 */
	public AcceptPacket release(RequestPacket request, boolean remove) {
		return this.release(request, remove, true);
	}

	/**
	 * @param request
	 * @param remove 
	 * @param assertion 
	 * @return AcceptPacket if any released by request.
	 */
	public AcceptPacket release(RequestPacket request, boolean remove, boolean assertion) {
		AcceptPacket accept = null;
		if(assertion && !this.requests.containsKey(request.requestID)) {
			PaxosConfig.getLogger().log(Level.SEVERE,
					"PendingDigests trying to release unqueued request {0}",
					new Object[] { request.getSummary() });
					assert(false) : request;
		}
		
		synchronized (this.requests) {
			accept = this.accepts.get(request.requestID);
		}
		assert (request != null && request.getPaxosID() != null && (accept == null || accept
				.getPaxosID() != null));

		if (accept != null && accept.getPaxosID().equals(request.getPaxosID())) {
			if (request.digestEquals(accept,
					mds[(int) (Math.random() * mds.length)])) {
				accept = accept.undigest(request);
				if(remove)
				synchronized(this.requests) {
					this.accepts.remove(request.requestID);
				}
				return accept;
			} else
				this.logAnomaly(request, accept);
		}
		return null;
	}

	private void logAnomaly(RequestPacket request, AcceptPacket accept) {
		PaxosConfig.getLogger().warning(
				"Mismatched digests for matching requestIDs: "
						+ request.getSummary() + " != " + accept.getSummary());
	}

	private String truncatedSummary(int size) {
		String s = "[";
		int count = 0;
		AcceptPacket[] array = this.accepts.values().toArray(new AcceptPacket[0]);
		for (AcceptPacket accept : array) {
			s += (count>0 ? ", " : "") + accept.requestID + ":" + accept.getSummary();
			if ((++count) == size)
				break;
		}
		assert(count==size || count==array.length);
		return s + "]";
	}

	public String toString() {
		return this.accepts.size() + ":"+ this.truncatedSummary(16);
	}

	/**
	 * @return Size of pending accepts.
	 */
	public int size() {
		return this.accepts.size();
	}

	/**
	 * @param request
	 * @return Removed accept if any.
	 */
	public AcceptPacket remove(RequestPacket request) {
		return this.accepts.remove(request.requestID);
	}
}
