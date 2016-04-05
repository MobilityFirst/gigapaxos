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
	 * @return byte[] serialized form of this object with the first four bytes
	 *         containing the integer packet type of this packet in network byte
	 *         order; ensure that ByteBuffer.wrap(bytes,0,4).getInt() returns
	 *         the expected type on the bytes being returned.
	 */
	public byte[] toBytes();
}
