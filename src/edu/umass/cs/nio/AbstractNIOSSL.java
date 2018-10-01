/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun */
package edu.umass.cs.nio;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import edu.umass.cs.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public abstract class AbstractNIOSSL implements Runnable {
	private final static int DEFAULT_BUFFER_SIZE = NIOTransport.WRITE_BUFFER_SIZE;
	/**
	 * There isn't much benefit to increasing the buffer size and it does
	 * introduce a speed bump, so best to leave MAX_FACTOR at 1, effectively
	 * disabling dynamic buffer expansion.
	 */
	private final static int MAX_FACTOR = 1;
	private final static int MAX_BUFFER_SIZE = MAX_FACTOR
			* NIOTransport.WRITE_BUFFER_SIZE;
	private final static int MAX_DST_BUFFER_SIZE = MAX_FACTOR
			* NIOTransport.WRITE_BUFFER_SIZE;

	ByteBuffer wrapSrc, unwrapSrc;
	ByteBuffer wrapDst, unwrapDst;

	final SSLEngine engine;
	final Executor taskWorkers;
	
	final String myID;

	final SelectionKey key;
	private static final Logger log = Logger.getLogger(NIOTransport.class
			.getName());

	/**
	 * @param key
	 * @param engine
	 * @param taskWorkers
	 * @param myID 
	 */
	public AbstractNIOSSL(SelectionKey key, SSLEngine engine,
			Executor taskWorkers, String myID) {
		this.wrapSrc = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
		this.wrapDst = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
		this.unwrapSrc = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
		this.unwrapDst = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
		this.engine = engine;
		this.taskWorkers = taskWorkers;
		this.key = key;
		this.myID = myID;

		run();
	}

	/**
	 * @param decrypted
	 */
	public abstract void onInboundData(ByteBuffer decrypted);

	/**
	 * @param encrypted
	 */
	public abstract void onOutboundData(ByteBuffer encrypted);

	/**
	 * @param cause
	 */
	public abstract void onHandshakeFailure(Exception cause);

	/**
	 * 
	 */
	public abstract void onHandshakeSuccess();

	/**
	 * 
	 */
	public abstract void onClosed();

	/**
	 * To wrap encrypt-and-send outgoing data.
	 * 
	 * @param unencrypted
	 */
	public synchronized void nioSend(final ByteBuffer unencrypted) {
		try {
			Util.put(wrapSrc, unencrypted);
			// wrapSrc.put(unencrypted);
		} catch (BufferOverflowException boe) {
			// will never come here
			wrapSrc = getBiggerBuffer(wrapSrc, unencrypted);
			log.log(Level.INFO, "{0} increased wrapSrc buffer size to {1}",
					new Object[] { this, wrapSrc.capacity() });
		}
		run();
	}

	/**
	 * To unwrap (decrypt) data received from the network.
	 * 
	 * @param encrypted
	 */
	public synchronized void notifyReceived(ByteBuffer encrypted) {
		int original = encrypted.remaining();
		try {
			Util.put(unwrapSrc, encrypted);
			// unwrapSrc.put(encrypted);
		} catch (BufferOverflowException boe) {
			// will never come here
			unwrapSrc = getBiggerBuffer(unwrapSrc, encrypted);
			log.log(Level.INFO, "{0} increased unwrapSrc buffer size to {1}",
					new Object[] { this, unwrapSrc.capacity() });

		}
		NIOInstrumenter.incrEncrBytesRcvd(original - encrypted.remaining());
		run();
	}

	/**
	 * Trying to put buf2 into buf1 by creating a new buffer with enough space.
	 * Using this method seems to cause nio to get stuck at high sending rates.
	 * Unclear why. This method currently does not get invoked during runtime.
	 * 
	 * @param buf1
	 * @param buf2
	 * @return
	 */
	@Deprecated
	private ByteBuffer getBiggerBuffer(ByteBuffer buf1, ByteBuffer buf2) {
		int biggerSize = buf1.position() + buf2.remaining();
		if (biggerSize > MAX_BUFFER_SIZE) {
			log.log(Level.WARNING,
					"{0} reached maximum allowed buffer size limit",
					new Object[] { this });
			throw new BufferOverflowException();
		}
		ByteBuffer biggerBuf = ByteBuffer.allocate(biggerSize);
		buf1.flip();
		biggerBuf.put(buf1);
		biggerBuf.put(buf2);
		buf1.compact(); // not really needed
		return biggerBuf;

	}

	private static ByteBuffer getBiggerBuffer(ByteBuffer buf, int size) {
		ByteBuffer b = ByteBuffer.allocate(size);
		buf.flip();
		b.put(buf);
		return b;
	}

	public synchronized void run() {
		// executes non-blocking tasks on the IO-Worker
		while (this.step())
			continue;
	}

	private boolean step() {
		switch (engine.getHandshakeStatus()) {
		case NOT_HANDSHAKING:
			boolean anything = false;
			{
				if (wrapSrc.position() > 0)
					anything |= this.wrap();
				if (unwrapSrc.position() > 0)
					anything |= this.unwrap();
			}
			return anything;

		case NEED_WRAP:

			if (!this.wrap())
				return false;
			break;

		case NEED_UNWRAP:
			if (!this.unwrap())
				return false;
			break;

		case NEED_TASK:
			final Runnable sslTask = engine.getDelegatedTask();
			if (sslTask == null)
				return false;
			Runnable wrappedTask = new Runnable() {
				@Override
				public void run() {
					try {
						long t0 = System.nanoTime();
						sslTask.run();
						log.log(Level.FINEST,
								"{0} async SSL task {1} took {2}ms",
								new Object[] { this, sslTask,
										(System.nanoTime() - t0) / 1000000 });

						// continue handling I/O
						AbstractNIOSSL.this.run();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			taskWorkers.execute(wrappedTask);
			return false;
			/* We could also run delegated tasks in blocking mode and return
			 * true here, but that would probably defeat the purpose of NIO as
			 * some delegated tasks may require remote calls. */

		case FINISHED:
			throw new IllegalStateException("FINISHED");
		}

		return true;
	}

	private synchronized boolean wrap() {
		SSLEngineResult wrapResult;

		try {
			wrapSrc.flip();
			wrapResult = engine.wrap(wrapSrc, wrapDst);
			wrapSrc.compact();
		} catch (SSLException exc) {
			this.onHandshakeFailure(exc);
			return false;
		}

		switch (wrapResult.getStatus()) {
		case OK:
			if (wrapDst.position() > 0)
				this.drainOutbound();
			break;

		case BUFFER_UNDERFLOW:
			log.log(Level.FINEST, "{0} wrap BUFFER_UNDERFLOW",new Object[] { this });
			// try again later
			break;

		case BUFFER_OVERFLOW:
			log.log(Level.INFO,
					"{0} wrap BUFFER_OVERFLOW: Wrapped data is coming faster than the network can send it out.",
					new Object[] { this });
			int biggerSize = engine.getSession().getApplicationBufferSize()
					+ wrapDst.capacity();
			//if (biggerSize > MAX_DST_BUFFER_SIZE)
				// throw new IllegalStateException("failed to wrap")
			//	;
			// try increasing buffer size up to maximum limit
			if (biggerSize < MAX_DST_BUFFER_SIZE) {
				wrapDst = getBiggerBuffer(wrapDst, biggerSize);
				log.log(Level.INFO, "{0} increased wrapDst buffer size to {1}",
						new Object[] { this, wrapDst.capacity() });
				// retry the operation.
			}
			// drain outbound buffer
			this.drainOutbound();
			break;

		case CLOSED:
			this.onClosed();
			return false;
		}

		// inform server of handshake success
		switch (wrapResult.getHandshakeStatus()) {
		case FINISHED:
			this.onHandshakeSuccess();
			return false;
		default:
			break;
		}

		return true;
	}

	/**
	 * Push once and spawn task if all outbound data is not fully pushed out.
	 * 
	 * @param
	 */
	private void drainOutbound() {
		if (wrapDst.position() > 0) {
			pushOutbound();
			if (wrapDst.position() == 0)
				return;
			// else spawn task
			Runnable pushTask = new Runnable() {
				@Override
				public void run() {
					while (wrapDst.position() > 0) {
						int prev = wrapDst.position();
						synchronized (AbstractNIOSSL.this) {
							wrapDst.flip();
							AbstractNIOSSL.this.onOutboundData(wrapDst);
							wrapDst.compact();
						}
						if (wrapDst.position() == prev)
							Thread.yield();
					}
				}
			};
			this.taskWorkers.execute(pushTask);
		}
	}

	private void pushOutbound() {
		wrapDst.flip();
		AbstractNIOSSL.this.onOutboundData(wrapDst);
		wrapDst.compact();
	}

	private void pullInbound() {
		unwrapDst.flip();
		this.onInboundData(unwrapDst);
		unwrapDst.compact();
	}

	private synchronized boolean unwrap() {
		SSLEngineResult unwrapResult;

		try {
			unwrapSrc.flip();
			unwrapResult = engine.unwrap(unwrapSrc, unwrapDst);
			unwrapSrc.compact();
		} catch (SSLException exc) {
			this.onHandshakeFailure(exc);
			return false;
		}

		switch (unwrapResult.getStatus()) {
		case OK:
			if (unwrapDst.position() > 0)
				this.pullInbound();
			break;

		case CLOSED:
			this.onClosed();
			return false;

		case BUFFER_OVERFLOW:
			log.log(Level.INFO,
					"{0} unwrap BUFFER_OVERFLOW: Network data is coming in faster than can be unwrapped",
					new Object[] { this });
			int biggerSize = engine.getSession().getApplicationBufferSize()
					+ unwrapDst.capacity();
			//iOS translation does not allow empty body "if" statements
			//if (biggerSize > MAX_BUFFER_SIZE)
				// throw new IllegalStateException("failed to unwrap")
			//	;
			// try increasing size first
			if (biggerSize < MAX_BUFFER_SIZE) {
				unwrapDst = getBiggerBuffer(unwrapDst, biggerSize);
				log.log(Level.INFO,
						"{0} increased unwrapDst buffer size to {1}",
						new Object[] { this, unwrapDst.capacity() });
				// retry the operation.
			}
			// try draining inbound buffer
			this.pullInbound();
			break;

		case BUFFER_UNDERFLOW:
			log.log(Level.FINEST, "{0} unwrap BUFFER_UNDERFLOW",
					new Object[] { this });
			return false;
		}

		// inform client of handshake success
		switch (unwrapResult.getHandshakeStatus()) {
		case FINISHED:
			this.onHandshakeSuccess();
			return false;
		default:
			break;
		}

		return true;
	}

	public String toString() {
		return AbstractNIOSSL.class.getSimpleName()
				+":"+this.myID+":"+ (this.key != null ? this.key.channel() : "");
	}

	/**
	 * To flush stuck data if any.
	 */
	public void poke() {
		run();
	}

	/* We need to clean bytebuffers, otherwise they don't get garbage-collected
	 * quickly enough. It seems like no matter how much memory the JVM is given,
	 * the GC waits to clean up bytebuffers until it ends up using all that
	 * memory. This default behavior doesn't seem to affect performance, but
	 * makes the server look like a memory hog if many ssl connections get
	 * created over time. */
	protected void clean() {
		clean(this.unwrapDst);
		clean(this.unwrapSrc);
		clean(this.wrapDst);
		clean(this.wrapSrc);
	}

	private static void clean(ByteBuffer bbuf) {
		// Solve Compatible problem by using reflection
		Object cleaner = null;

		String version = System.getProperty("java.version");
		if (version.startsWith("1.8")) {
			// Java 8
			try {
				Class c = Class.forName("sun.misc.Cleaner");

				/* java 9 apparently may not support sun.nio.ch.DirectBuffer; if so,
				 * just comment the line below. The code will default to using the
				 * reflective approach below. NOTE: Code commented now.*/
				/*
				if (bbuf instanceof DirectBuffer)
					cleaner = ((DirectBuffer) bbuf).cleaner();
				*/

				if (cleaner == null)
					try {
						Field cleanerField = bbuf.getClass()
								.getDeclaredField("cleaner");
						cleanerField.setAccessible(true);
						cleaner = cleanerField.get(bbuf);
						//cleaner = (Cleaner) cleanerField.get(bbuf);
					} catch (NoSuchFieldException | SecurityException
							| IllegalArgumentException | IllegalAccessException e) {
						// best-effort GC purposes only, so ignore
						e.printStackTrace();
					}
				if (cleaner != null) {
					Method meth = c.getMethod("clean");
					meth.invoke(cleaner);
					//cleaner.clean();
				}
			} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
				e.printStackTrace();
			}


		} else {
			// Java 9+
			try {
				Class c = Class.forName("java.lang.ref.Cleaner");

				Method meth = c.getMethod("create");
				// a static method to create a cleaner
				cleaner = meth.invoke(null);
				Class clazz = Class.forName("java.lang.ref.Cleaner.Cleanable");
				meth = c.getMethod("register", Object.class, Runnable.class);
				Object cleanable = meth.invoke(cleaner, bbuf, new Runnable(){
					@Override
					public void run() {
					}
				});

				if (cleanable != null) {
					meth = clazz.getMethod("clean");
					meth.invoke(cleanable);
				}
			} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
				e.printStackTrace();
			}
		}
	}
}