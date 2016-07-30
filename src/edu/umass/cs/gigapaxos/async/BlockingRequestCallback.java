package edu.umass.cs.gigapaxos.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;

/**
 * @author arun
 * @param <V>
 *
 */
public class BlockingRequestCallback<V> implements RequestCallback,
		RequestFuture<V> {
	private final Request request;
	Request response;

	/**
	 * @param request
	 *            The request until whose execution this callback will block.
	 */
	public BlockingRequestCallback(Request request) {
		this.request = request;
	}

	@Override
	public void handleResponse(
			edu.umass.cs.gigapaxos.interfaces.Request response) {
		synchronized (this) {
			this.response = response;
			this.notify();
		}
	}

	/**
	 * @return Response corresponding to the execution of the request supplied
	 *         in this callback's constructor.
	 */
	@SuppressWarnings("unchecked")
	// class cast exception otherwise is fine
	private V waitResponse(Long timeout, TimeUnit unit) {
		synchronized (this) {
			while (this.response == null)
				try {
					if (timeout != null)
						this.wait(unit.toMillis(timeout));
					else
						this.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
					// continue to wait
				}
		}
		assert (this.response != null);
		return (V) this.response;
	}

	private V waitResponse() {
		return (V) this.waitResponse(null, TimeUnit.MILLISECONDS);
	}

	@Override
	public boolean isDone() {
		return this.waitResponse() != null;
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		return this.waitResponse();
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		return this.waitResponse(timeout, unit);
	}

	/**
	 * @return The request corresponding to this callback.
	 */
	public Request getRequest() {
		return this.request;
	}
}
