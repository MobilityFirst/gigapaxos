package edu.umass.cs.gigapaxos.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;

/**
 * @author arun
 * @param <V>
 *
 */
public class RequestCallbackFuture<V> implements Callback<Request, V>,
// RequestCallback,
		RequestFuture<V> {
	private final Request request;
	private final Callback<Request, V> callback;
	private Request response;
	private Exception exception;
	private boolean processResponseInvoked=false;

	/**
	 * @param request
	 *            The request until whose execution this callback will block.
	 * @param callback
	 */
	public RequestCallbackFuture(Request request, Callback<Request, V> callback) {
		this.request = request;
		this.callback = callback;
	}

	/**
	 * All get/wait roads lead here.
	 * 
	 * @return Response corresponding to the execution of the request supplied
	 *         in this callback's constructor.
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	// class cast exception otherwise is fine
	private V waitResponse(Long timeout, TimeUnit unit) throws ExecutionException {
		if(this.exception!=null) throw new ExecutionException(this.exception);
		synchronized (this) {
			if (this.response == null)
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
		return (V) this.response;
	}

	// same as get() but private
	private V waitResponse() throws ExecutionException {
		return (V) this.waitResponse(null, TimeUnit.MILLISECONDS);
	}

	@Override
	public boolean isDone() {
		try {
			return this.waitResponse() != null;
		} catch(ExecutionException e) {
			return true;
		}
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		return this.waitResponse();
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws ExecutionException,
			TimeoutException {
		V response = this.waitResponse(timeout, unit);
		if(!this.processResponseInvoked)
			throw new TimeoutException("Request " + request.getSummary() + " timed out after " + unit.toMillis(timeout) + "ms");
		return response;
	}

	/**
	 * @return The request corresponding to this callback.
	 */
	public Request getRequest() {
		return this.request;
	}

	@Override
	public V processResponse(Request response) {
		V retval = null;
		this.processResponseInvoked=true;
		if (this.callback != null)
			try {
				retval = this.callback.processResponse(response);
			} catch (Exception e) {
				this.exception = e;
			}
		synchronized (this) {
			this.response = response;
			this.notify();
		}
		return retval;
	}
}
