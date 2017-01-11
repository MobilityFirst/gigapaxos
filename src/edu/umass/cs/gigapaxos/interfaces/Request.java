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
package edu.umass.cs.gigapaxos.interfaces;

import java.io.UnsupportedEncodingException;

import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;

/**
 * @author V. Arun
 */
public interface Request extends Summarizable{
	/**
	 * A string representing a special no-op request.
	 */
	public static String NO_OP = "NO_OP";

	/**
	 * @return The IntegerPacketType type corresponding to this request.
	 */
	public IntegerPacketType getRequestType();

	/**
	 * @return Returns the unique app-level replica-group ID.
	 */
	public String getServiceName();

	/**
	 * Serializes the request to a String. The default toString() method must be
	 * overridden.
	 * 
	 * @return Returns this request serialized as a String.
	 */
	public String toString(); // must be explicitly overridden

	/**
	 * @return Summary
	 */
	default Object getSummary() {
		return new Object() {
			public String toString() {
				return Request.this.getRequestType()
						+ ":"
						+ Request.this.getServiceName()
						+ (Request.this instanceof RequestIdentifier ? ":"
								+ ((RequestIdentifier) Request.this)
										.getRequestID() : "");
			}
		};
	}

	/**
	 * The implementation of this method is tied to and must be the inverse of
	 * {@link AppRequestParser#getRequest(byte[], edu.umass.cs.nio.nioutils.NIOHeader)}
	 * , i.e., invoking getRequest(request.toBytes(), header).equals(request).
	 * 
	 * @return byte[] serialized form of this request
	 */
	default byte[] toBytes() {
		try {
			return this.toString().getBytes(
					MessageNIOTransport.NIO_CHARSET_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
}
