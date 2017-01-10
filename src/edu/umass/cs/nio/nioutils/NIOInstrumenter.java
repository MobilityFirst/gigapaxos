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
package edu.umass.cs.nio.nioutils;

import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         Helps instrument read/write stats in NIOTransport. Used for testing
 *         and instrumentation purposes only.
 */

public class NIOInstrumenter {
	private static Logger log = NIOTransport.getLogger();
	private static int totalSent = 0; // Sent by NIOTransport
	private static int totalRcvd = 0; // Received by NIOTransport

	private static int totalBytesSent = 0;
	private static int totalBytesRcvd = 0;

	private static int totalEncrBytesSent = 0;
	private static int totalEncrBytesRcvd = 0;

	private static int totalConnAccepted = 0;
	private static int totalConnInitiated = 0;
	private static int totalJSONRcvd = 0;
	private static double averageDelay = 0;
	private static boolean enabled = false;

	private static long PERIOD = 5000;
	private static long lastUpdated = System.currentTimeMillis();
	private static Timer timer = new Timer(true);

	private static final long THRESHOLD = 8000;
	static {
		if (enabled)
			timer.scheduleAtFixedRate(new TimerTask() {
				public void run() {
					if (System.currentTimeMillis() - lastUpdated < PERIOD) {
						String stats = NIOInstrumenter.getJSONStats();
						System.out.println(stats);
						if (log != null)
							log.log(Level.WARNING, "{0}",
									new Object[] { stats });
					}
				}
			}, 0, PERIOD);
	}

	/**
	 * @param l
	 */
	public static void setLogger(Logger l) {
		log = l;
	}

	/**
	 * 
	 */
	public static void incrSent() {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				lastUpdated = System.currentTimeMillis();
				totalSent++;
			}
	}

	/**
	 * 
	 */
	public static void incrRcvd() {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				lastUpdated = System.currentTimeMillis();
				totalRcvd++;
			}
	}

	/**
	 * @param sent
	 * @return total bytes sent
	 */
	public static int incrBytesSent(int sent) {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				lastUpdated = System.currentTimeMillis();
				totalBytesSent += sent;
			}
		return totalBytesSent;
	}

	/**
	 * @param sent
	 * @return total encrypted bytes sent
	 */
	public static int incrEncrBytesSent(int sent) {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				lastUpdated = System.currentTimeMillis();
				totalEncrBytesSent += sent;
			}
		return totalEncrBytesSent;
	}

	/**
	 * @param rcvd
	 * @return total bytes received
	 */
	public static int incrBytesRcvd(int rcvd) {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				lastUpdated = System.currentTimeMillis();
				totalBytesRcvd += rcvd;
			}
		return totalBytesRcvd;
	}

	/**
	 * @param rcvd
	 * @return total encrypted bytes received
	 */
	public static int incrEncrBytesRcvd(int rcvd) {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				lastUpdated = System.currentTimeMillis();
				totalEncrBytesRcvd += rcvd;
			}
		return totalEncrBytesRcvd;
	}

	/**
	 * 
	 */
	public static void incrAccepted() {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				lastUpdated = System.currentTimeMillis();
				totalConnAccepted++;
			}
	}

	/**
	 * 
	 */
	public static void incrInitiated() {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				lastUpdated = System.currentTimeMillis();
				totalConnInitiated++;
			}
	}

	/**
	 * 
	 */
	public static void incrJSONRcvd() {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				lastUpdated = System.currentTimeMillis();
				totalJSONRcvd++;
			}
	}

	/**
	 * @param msg
	 * @throws JSONException
	 */
	public static void rcvdJSONPacket(JSONObject msg) throws JSONException {
		if (enabled)
			synchronized (NIOInstrumenter.class) {

				if (msg.has(JSONMessenger.SENT_TIME)) {
					averageDelay = Util.movingAverage(
							System.currentTimeMillis()
									- msg.getLong(JSONMessenger.SENT_TIME),
							averageDelay);
					lastUpdated = System.currentTimeMillis();
				}
			}
	}

	/**
	 * @return number of missing packets
	 */
	public static int getMissing() {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				return totalSent - totalJSONRcvd;
			}
		return 0;
	}

	/**
	 * 
	 */
	public void disable() {
		enabled = (enabled ? false : false);
	}

	/**
	 * 
	 */
	public void enable() {
		enabled = true;
	}

	/**
	 * @return monitorHandleMessageEnabled
	 */
	public static boolean monitorHandleMessageEnabled() {
		return monitorHandleMessageEnabled;
	}

	private static boolean monitorHandleMessageEnabled = false;

	/**
	 * Will monitor for long
	 * {@link AbstractPacketDemultiplexer#handleMessage(Object, NIOHeader)}
	 * instances.
	 */
	public static void monitorHandleMessage() {
		{
			log.log(Level.INFO,
					"Initializing handleMessageMonitor with threshold "
							+ THRESHOLD + " period " + PERIOD);
			monitorHandleMessageEnabled = true;
			timer.scheduleAtFixedRate(new TimerTask() {
				public void run() {
					String stats = AbstractPacketDemultiplexer
							.getHandleMessageReport(THRESHOLD);
					if (stats != null) {
						System.out.println(stats);
						if (log != null)
							log.log(Level.WARNING, "{0}",
									new Object[] { stats });
					}
				}
			}, 0, PERIOD);
		}
	}

	/**
	 * @return Pretty printed stats
	 */
	public static String getJSONStats() {
		if (enabled)
			synchronized (NIOInstrumenter.class) {
				return "[NIO stats: [ #sent=" + totalSent + " | #rcvd="
						+ totalRcvd + " | bytesSent=" + totalBytesSent
						+ " | bytesRcvd=" + totalBytesRcvd
						+ " | totalEncrBytesSent=" + totalEncrBytesSent
						+ " | totalEncrBytesRcvd=" + totalEncrBytesRcvd + "]]";
			}
		return null;
	}

	public String toString() {
		String s = "";
		return s
				+ "NIO stats: [totalSent = "
				+ totalSent
				+ ", totalRcvd = "
				+ totalRcvd
				+ (totalSent != totalRcvd ? ", missing-or-batched = "
						+ (totalSent - totalRcvd) : "") + "]"
				+ "\n\t [totalConnInitiated = " + totalConnInitiated
				+ ", totalConnAccepted = " + totalConnAccepted + "]"
				+ "\nJSONMessageWorker: [totalJSONRcvd = " + totalJSONRcvd
				+ "]" + "]";
	}
}
