package edu.umass.cs.nio.interfaces;

/**
 * @author arun
 *
 *         A simple interface to serialize an object to bytes. There is no
 *         "fromBytes(byte[])" method to deserialize as that needs to be
 *         implemented using the object's constructor.
 */
public interface Byteable {
	/**
	 * @return byte[] serialized form of this object.
	 */
	public byte[] toBytes();
}
