package edu.umass.cs.nio.nioutils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import edu.umass.cs.nio.MessageNIOTransport;

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
	 * Character set used by NIO for encoding bytes to String and back.
	 */
	public static final String CHARSET = MessageNIOTransport.NIO_CHARSET_ENCODING;

	/**
	 * @param sndr
	 * @param rcvr
	 */
	public NIOHeader(InetSocketAddress sndr, InetSocketAddress rcvr) {
		this.sndr = sndr;
		this.rcvr = rcvr;
	}
	
	/**
	 * @param bytes
	 * @return NIOHeader constructed from the first 12 bytes.
	 * @throws UnknownHostException
	 */
	public static NIOHeader getNIOHeader(byte[] bytes)
			throws UnknownHostException {
		ByteBuffer bbuf = ByteBuffer.wrap(bytes, 0, 12);
		byte[] sip = new byte[4];
		bbuf.get(sip, 0, 4);
		int sport = (int)bbuf.getShort();
		if (sport < 0)
			sport += 2 * (Short.MAX_VALUE + 1);

		byte[] dip = new byte[4];
		bbuf.get(dip, 0, 4);
		int dport = (int)bbuf.getShort();
		if (dport < 0)
			dport += 2 * (Short.MAX_VALUE + 1);
		return new NIOHeader(new InetSocketAddress(
				InetAddress.getByAddress(sip), sport), new InetSocketAddress(
				InetAddress.getByAddress(dip), dport));
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
