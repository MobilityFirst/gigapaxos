package edu.umass.cs.txn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.GigaPaxosClient;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.txn.interfaces.TXRequest;
import edu.umass.cs.txn.interfaces.Transactor;
import edu.umass.cs.txn.txpackets.TXPacket;
import edu.umass.cs.txn.txpackets.TxStateRequest;

/**
 * @author arun
 * 
 *         A class with static utility methods related to transactions.
 * 
 */
public class TXUtils implements Transactor {

	private static final long RTO = ReconfigurableAppClientAsync.DEFAULT_GC_TIMEOUT;
	private static final int DEFAULT_NUM_ATTEMPTS = 3;

	protected static Request[] tryFinishAsyncTasks(
			GigaPaxosClient<Request> gpClient, ArrayList<Request> txRequests) {
		return tryFinishAsyncTasks(gpClient, txRequests, DEFAULT_NUM_ATTEMPTS);
	}

	/**
	 * Blocks and retries in parallel until all of a set of transaction steps
	 * have successfully completed. The steps have to be idempotent, i.e.,
	 * executing them multiple times should have the same effect as executing
	 * them exactly once.
	 * 
	 * @param gpClient
	 * @param txRequests
	 *            The list of operations comprising the transaction.
	 * @param numAttempts
	 *            Maximum number of times each task will be retransmitted before
	 *            timing out.
	 * @return An array of responses where the i'th element of the array is the
	 *         response for the i'th element in {@code txRequests}.
	 */
	protected static Request[] tryFinishAsyncTasks(
			GigaPaxosClient<Request> gpClient, ArrayList<Request> txRequests,
			int numAttempts) {
		RequestCallback[] callbacks = new RequestCallback[txRequests.size()];
		boolean[] monitor = new boolean[txRequests.size()];
		Request[] responses = new Request[txRequests.size()];

		// create callbacks
		for (int i = 0; i < callbacks.length; i++) {
			final int j = i;
			callbacks[i] = new RequestCallback() {
				@Override
				public void handleResponse(Request response) {
					if (response instanceof TXPacket
							&& !((TXPacket) response).isFailed()
							|| response instanceof RequestActiveReplicas
							&& !((RequestActiveReplicas) response).isFailed())
						synchronized (monitor) {
							monitor[j] = true;
							responses[j] = response;
							monitor.notify();
						}
				}
			};
		}
		/* Wait for monitor success and try a limited number of retransmissions.
		 * It is okay for this method to fail to finish all of the tasks. An
		 * unfinished transaction will eventually be attempted to be aborted
		 * upon a node recovery or after a sufficiently long timeout and it is
		 * safe to do so as the abort attempt can succeed if and only if */
		synchronized (monitor) {
			for (int j = 0; j < numAttempts; j++) {
				// async send undone requests
				for (int i = 0; i < monitor.length; i++) {
					if (!monitor[i])
						try {
							gpClient.sendRequest(txRequests.get(i),
									callbacks[i]);
						} catch (IOException e) {
							e.printStackTrace();
							// just continue
						}
				}
				// wait on undone requests
				boolean allDone = true;
				for (int i = 0; i < monitor.length; i++) {
					if (!monitor[i] && !(allDone = false))
						try {
							monitor.wait(RTO);
						} catch (InterruptedException e) {
							e.printStackTrace();
							// continue
						}
				}
				if (allDone)
					break;
			}
		}
		return (responses);
	}

	/**
	 * This method fetches the union set of the set of active replica addresses
	 * for all participant groups in a transaction by issuing an async task for
	 * the lock list of the transaction.
	 * 
	 * @param gpClient
	 * @param tx
	 * @return
	 */
	protected static Set<InetSocketAddress> requestActiveReplicas(
			GigaPaxosClient<Request> gpClient, Transaction tx) {
		ArrayList<Request> rars = new ArrayList<Request>();
		for (String name : tx.getLockList())
			rars.add(new RequestActiveReplicas(name));
		Request[] responses = tryFinishAsyncTasks(gpClient, rars);
		Set<InetSocketAddress> addresses = new HashSet<InetSocketAddress>();
		for (Request response : responses)
			// response must be RequestActiveReplicas
			if (response != null
					&& !((RequestActiveReplicas) response).isFailed())
				addresses.addAll(((RequestActiveReplicas) response)
						.getActives());
		addresses.add(tx.getEntryServer());
		return addresses;
	}

	protected TxStateRequest.State getTxState(
			GigaPaxosClient<Request> gpClient, Transaction tx)
			throws IOException {
		Request response = gpClient.sendRequest(new TxStateRequest(tx
				.getTxGroupName()));
		return (((TxStateRequest) response).getState());
	}

	/**
	 * Creates an async gigapaxos client without checking connectivity (because
	 * the reconfigurators may not yet have been initialized.
	 * 
	 * @param app
	 * @return The created client.
	 * @throws IOException
	 */
	protected static GigaPaxosClient<Request> getGPClient(AppRequestParser app)
			throws IOException {
		return new ReconfigurableAppClientAsync<Request>(
				ReconfigurationConfig.getReconfiguratorAddresses(), false) {

			@Override
			public Request getRequest(String stringified)
					throws RequestParseException {
				return app.getRequest(stringified);
			}

			@Override
			public Request getRequest(byte[] message, NIOHeader header)
					throws RequestParseException {
				return app.getRequest(message, header);
			}

			@Override
			public Set<IntegerPacketType> getRequestTypes() {
				return app.getRequestTypes();
			}
			
			public String getLabel() {
				return TXUtils.class.getSimpleName() + "."+GigaPaxosClient.class.getSimpleName();
			}
		};
	}

	@Override
	public void transact(TXRequest request) {
		// TODO Auto-generated method stub
		
	}

}
