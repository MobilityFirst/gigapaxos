package edu.umass.cs.gigapaxos.interfaces;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author arun
 *
 * @param <V>
 */
public interface RequestFuture<V> {

	/**
	 * @return Refer {@link Future#isDone()}.
	 */
	public boolean isDone();

	/**
	 * @return Refer {@link Future#get()}.
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public V get() throws InterruptedException, ExecutionException;

	/**
	 * @param timeout
	 * @param unit
	 * @return Refer {@link Future#get(long, TimeUnit)}.
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	public V get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException;

}
