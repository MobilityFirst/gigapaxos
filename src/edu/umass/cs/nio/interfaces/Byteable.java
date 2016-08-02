package edu.umass.cs.nio.interfaces;

/**
 * @author arun
 *
 *         A simple interface to serialize an object to bytes. There is no
 *         "fromBytes(byte[])" method to deserialize as that generally needs to
 *         be implemented using the object's constructor (unlike Java
 *         serialization that is object-agnostic but is also slow).
 */
public interface Byteable {
	/**
	 * The recommendation that the first four bytes contain the integer packet
	 * type is for bring able to write efficient demultiplexers that will not
	 * attempt to decode an incoming packet unless the type matches one of the
	 * types they expect to receive. Not following this requirement will
	 * will not break applications but can result in wasted decoding
	 * attempts if the inital bytes happen to accidentally match the header
	 * patterns used by other demultiplexers.
	 * 
	 * @return byte[] serialized form of this object. It is recommended that the
	 *         first four bytes contain the integer packet type of this packet
	 *         in network byte order, i.e., ByteBuffer.wrap(bytes,0,4).getInt()
	 *         should return the expected type on the bytes being returned.
	 */
	public byte[] toBytes();
}
