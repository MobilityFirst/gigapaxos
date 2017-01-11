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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.minidev.json.JSONValue;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.InterfaceMessageExtractor;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.utils.Stringer;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         This class is a legacy of an old design and currently does not do
 *         very much except for taking bytes from NIO and handing them over to a
 *         suitable packet demultiplexer. Earlier, this class used to have
 *         functionality for parsing the header and payload by buffering the
 *         incoming bytestream by converting it first into a character stream.
 *         This was ugly and the header functionality has now been integrated
 *         into NIOTransport for all messages including byte[].
 */
public class MessageExtractor implements InterfaceMessageExtractor {

	/**
	 * 
	 */
	public static final String STRINGIFIED = "_STRINGIFIED_";

	private ArrayList<AbstractPacketDemultiplexer<?>> packetDemuxes;
	private final ScheduledExecutorService executor = Executors
			.newScheduledThreadPool(1); // only for delay emulation

	private static final Logger log = NIOTransport.getLogger();

	protected MessageExtractor(AbstractPacketDemultiplexer<?> pd) {
		packetDemuxes = new ArrayList<AbstractPacketDemultiplexer<?>>();
		packetDemuxes.add(pd);
	}

	protected MessageExtractor() { // default packet demux returns false
		this(new PacketDemultiplexerDefault());
	}

	/**
	 * Note: Use with care. This will change demultiplexing behavior midway,
	 * which is usually not what you want to do. This is typically useful to set
	 * in the beginning.
	 */
	public synchronized void addPacketDemultiplexer(
			AbstractPacketDemultiplexer<?> pd) {
		// we update tmp to not have to lock this structure
		ArrayList<AbstractPacketDemultiplexer<?>> tmp = new ArrayList<AbstractPacketDemultiplexer<?>>(
				this.packetDemuxes);
		tmp.add(pd);
		this.packetDemuxes = tmp;
	}

	/**
	 * Note: Use with care. This will change demultiplexing behavior midway,
	 * which is usually not what you want to do. This is typically useful to set
	 * in the beginning.
	 */
	public synchronized void precedePacketDemultiplexer(
			AbstractPacketDemultiplexer<?> pd) {
		// we update tmp to not have to lock this structure
		ArrayList<AbstractPacketDemultiplexer<?>> tmp = new ArrayList<AbstractPacketDemultiplexer<?>>();
		tmp.add(pd);
		tmp.addAll(packetDemuxes);
		this.packetDemuxes = tmp;
	}

	/**
	 * Incoming data has to be associated with a socket channel, not a nodeID,
	 * because the sending node's id is not known until the message is parsed.
	 * This means that, if the the socket channel changes in the middle of the
	 * transmission, that message will **definitely** be lost.
	 */
	@Override
	public void processData(SocketChannel socket, ByteBuffer incoming) {
		try {
			this.processMessageInternal(socket, incoming);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void stop() {
		for (AbstractPacketDemultiplexer<?> pd : this.packetDemuxes)
			if (pd != null)
				pd.stop();
		this.executor.shutdownNow();
	}

	// called only for loopback receives or by SSL worker
	@Override
	public void processLocalMessage(InetSocketAddress sockAddr, byte[] msg) {
		try {
			this.demultiplexLocalMessage(new NIOHeader(sockAddr, sockAddr), msg);
		} catch (UnsupportedEncodingException e) {
			fatalExit(e);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * String to JSON conversion
	 * 
	 * @param msg
	 * @return
	 */
	protected static final JSONObject parseJSON(String msg, boolean cacheStringified) {
		JSONObject jsonData = null;
		try {
			if (msg.length() > 0 && JSONPacket.couldBeJSON(msg))
				jsonData = new JSONObject(msg);
			if (cacheStringified)
				jsonData.put(MessageExtractor.STRINGIFIED, msg);
			// Util.toJSONObject(msg);
		} catch (JSONException e) {
			log.severe("Received incorrectly formatted JSON message: " + msg);
			e.printStackTrace();
		}
		return jsonData;
	}

	protected static final JSONObject parseJSON(String msg) {
		return parseJSON(msg);
	}

	/**
	 * @param msg
	 * @return Parsed JSON.
	 */
	public static final net.minidev.json.JSONObject parseJSONSmart(String msg) {
		net.minidev.json.JSONObject jsonData = null;
		try {
			if (msg.length() > 0)
				jsonData = (net.minidev.json.JSONObject) JSONValue.parse(msg);
		} catch (Exception e) {
			log.severe("Received incorrectly formatted JSON message: " + msg);
			e.printStackTrace();
		}
		return jsonData;
	}

	/* *************** Start of private methods **************************** */

	protected static final void fatalExit(UnsupportedEncodingException e) {
		e.printStackTrace();
		System.err.println("NIO failed because the charset encoding "
				+ JSONNIOTransport.NIO_CHARSET_ENCODING
				+ " is not supported; exiting");
		System.exit(1);
	}

	// exists only to support delay emulation
	private void processMessageInternal(SocketChannel socket,
			ByteBuffer incoming) throws IOException {
		/* The emulated delay value is in the message, so we need to read all
		 * bytes off incoming and stringify right away. */
		long delay = -1;
		if (JSONDelayEmulator.isDelayEmulated()) {
			byte[] msg = new byte[incoming.remaining()];
			incoming.get(msg);
			String message = new String(msg,
					MessageNIOTransport.NIO_CHARSET_ENCODING);
			// always true because of max(0,delay)
			if ((delay = Math.max(0, JSONDelayEmulator.getEmulatedDelay(message))) >= 0)
				// run in a separate thread after scheduled delay
				executor.schedule(
						new MessageWorker(socket, msg, packetDemuxes), delay,
						TimeUnit.MILLISECONDS);
		} else
			// run it immediately
			this.demultiplexMessage(
					new NIOHeader(
							(InetSocketAddress) socket.getRemoteAddress(),
							(InetSocketAddress) socket.getLocalAddress()),
					incoming);
	}

	/**
	 * @param bytes
	 * @return String decoded from bytes.
	 * @throws UnsupportedEncodingException
	 */
	public static final String decode(byte[] bytes)
			throws UnsupportedEncodingException {
		return new String(bytes, MessageNIOTransport.NIO_CHARSET_ENCODING);
	}

	/**
	 * @param bytes
	 * @param offset
	 * @param length
	 * @return String decoded from bytes.
	 * @throws UnsupportedEncodingException
	 */
	public static final String decode(byte[] bytes, int offset, int length)
			throws UnsupportedEncodingException {
		return new String(bytes, offset, length,
				MessageNIOTransport.NIO_CHARSET_ENCODING);
	}

	private void demultiplexMessage(NIOHeader header, ByteBuffer incoming)
			throws IOException {
		boolean extracted = false;
		byte[] msg = null;
		// synchronized (this.packetDemuxes)
		{
			for (final AbstractPacketDemultiplexer<?> pd : this.packetDemuxes) {
				if (pd instanceof PacketDemultiplexerDefault
				// if congested, don't process
						|| pd.isCongested(header))
					continue;

				if (!extracted) { // extract at most once
					msg = new byte[incoming.remaining()];
					incoming.get(msg);
					extracted = true;
					NIOInstrumenter.incrBytesRcvd(msg.length + 8);
				}

				// String message = (new String(msg,
				// MessageNIOTransport.NIO_CHARSET_ENCODING));
				if (this.callDemultiplexerHandler(header, msg, pd))
					return;
			}
		}
	}

	// called only for loopback receives or emulated delays
	private void demultiplexLocalMessage(NIOHeader header, byte[] message)
			throws IOException {
		// synchronized (this.packetDemuxes)
		{
			for (final AbstractPacketDemultiplexer<?> pd : this.packetDemuxes) {
				if (pd instanceof PacketDemultiplexerDefault)
					// no congestion check
					continue;

				if (this.callDemultiplexerHandler(header, message, pd))
					return;
			}
		}
	}

	// finally called for all receives
	private boolean callDemultiplexerHandler(NIOHeader header, byte[] message,
			AbstractPacketDemultiplexer<?> pd) {
		try {
			Level level = Level.FINEST;
			log.log(level, "{0} calling {1}.handleMessageSuper({2}:{3})",
					new Object[] {
							this,
							pd,
							header,
							log.isLoggable(level) ? Util.truncate(new Stringer(message), 32, 32)
									: message });
			// the handler turns true if it handled the message
			if (pd.handleMessageSuper(message, header))
				return true;

		} catch (JSONException je) {
			je.printStackTrace();
		}
		return false;
	}

	public String toString() {
		return this.getClass().getSimpleName();
	}

	@Override
	public void demultiplexMessage(Object message) {
		for (final AbstractPacketDemultiplexer<?> pd : this.packetDemuxes) {
			if (pd.loopback(message))
				break;
		}
	}

	private class MessageWorker extends TimerTask {

		private final SocketChannel socket;
		private final byte[] msg;

		MessageWorker(SocketChannel socket, byte[] msg,
				ArrayList<AbstractPacketDemultiplexer<?>> pdemuxes) {
			this.msg = msg;
			this.socket = socket;
		}

		@Override
		public void run() {
			try {
				MessageExtractor.this.demultiplexLocalMessage(new NIOHeader(
						(InetSocketAddress) socket.getRemoteAddress(),
						(InetSocketAddress) socket.getLocalAddress()), msg);
			} catch (UnsupportedEncodingException e) {
				fatalExit(e);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param sndrAddress
	 * @param rcvrAddress
	 * @param json
	 * @return JSONObject with addresses stamped.
	 */
	@SuppressWarnings("deprecation")
	// for backwards compatibility
	public static final JSONObject stampAddressIntoJSONObject(
			InetSocketAddress sndrAddress, InetSocketAddress rcvrAddress,
			JSONObject json) {
		// only put the IP field in if it doesn't exist already
		try {
			// put sender address
			if (!json.has(MessageNIOTransport.SNDR_ADDRESS_FIELD))
				json.put(MessageNIOTransport.SNDR_ADDRESS_FIELD,
						sndrAddress.getAddress().getHostAddress()+":"+sndrAddress.getPort());

			// TODO: remove the deprecated lines bel
			if (!json.has(JSONNIOTransport.SNDR_IP_FIELD))
				json.put(JSONNIOTransport.SNDR_IP_FIELD, sndrAddress
						.getAddress().getHostAddress());
			if (!json.has(JSONNIOTransport.SNDR_PORT_FIELD))
				json.put(JSONNIOTransport.SNDR_PORT_FIELD,
						sndrAddress.getPort());

			// put receiver address
			if (!json.has(MessageNIOTransport.RCVR_ADDRESS_FIELD))
				json.put(MessageNIOTransport.RCVR_ADDRESS_FIELD,
						rcvrAddress.getAddress().getHostAddress()+":"+rcvrAddress.getPort());

		} catch (JSONException e) {
			log.severe("Encountered JSONException while stamping sender address and port at receiver: ");
			e.printStackTrace();
		}
		return json;
	}

	/**
	 * For comparing json-smart with org.json.
	 * 
	 * @param sndrAddress
	 * @param rcvrAddress
	 * @param json
	 * @return Parsed JSON object.
	 */
	public static final net.minidev.json.JSONObject stampAddressIntoJSONObject(
			InetSocketAddress sndrAddress, InetSocketAddress rcvrAddress,
			net.minidev.json.JSONObject json) {
		// only put the IP field in if it doesn't exist already
		try {
			// put sender address
			if (!json.containsKey(MessageNIOTransport.SNDR_ADDRESS_FIELD))
				json.put(MessageNIOTransport.SNDR_ADDRESS_FIELD,
						sndrAddress.toString());

			// put receiver socket address
			if (!json.containsKey(MessageNIOTransport.RCVR_ADDRESS_FIELD))
				json.put(MessageNIOTransport.RCVR_ADDRESS_FIELD,
						rcvrAddress.toString());

		} catch (Exception e) {
			log.severe("Encountered JSONException while stamping sender address and port at receiver: ");
			e.printStackTrace();
		}
		return json;
	}
}
