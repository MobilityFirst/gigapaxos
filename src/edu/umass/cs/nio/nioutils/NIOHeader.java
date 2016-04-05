package edu.umass.cs.nio.nioutils;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * @author arun
 *
 */
public class NIOHeader {
	/**
	 * Size in bytes.
	 */
	public static final int BYTES = 12;
	/**
	 * 
	 */
	public final InetSocketAddress sndr;
	/**
	 * 
	 */
	public final InetSocketAddress rcvr;

	/**
	 * @param sndr
	 * @param rcvr
	 */
	public NIOHeader(InetSocketAddress sndr, InetSocketAddress rcvr) {
		this.sndr = sndr;
		this.rcvr = rcvr;
	}

	public String toString() {
		return sndr + "->" + rcvr;
	}

	/**
	 * @return {@code this} as a 12 byte array with 6 bytes for each IP, port
	 *         pair.
	 */
	public byte[] toBytes() {
		ByteBuffer bbuf = ByteBuffer.wrap(new byte[12]);
		bbuf.put(this.sndr != null ? this.sndr.getAddress().getAddress()
				: new byte[4]);
		bbuf.putShort(this.sndr != null ? (short) this.sndr.getPort() : 0);
		bbuf.put(this.rcvr != null ? this.rcvr.getAddress().getAddress()
				: new byte[4]);
		bbuf.putShort(this.rcvr != null ? (short) this.rcvr.getPort() : 0);
		return bbuf.array();
	}
}
