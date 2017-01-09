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
package edu.umass.cs.reconfiguration.interfaces;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestIdentifier;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

/**
 * @author V. Arun
 *         <p>
 *         A reconfigurable request can also be a stop request and comes with an
 *         epoch number. The application must be aware of both of these if it
 *         uses any coordination protocol other than paxos.
 */
public interface ReconfigurableRequest extends Request {
	/**
	 * Used internally.
	 */
	public static final IntegerPacketType STOP = ReconfigurationPacket.PacketType.NO_TYPE;
	
	/**
	 * @return The epoch number.
	 */
	public int getEpochNumber();

	/**
	 * @return True if this request is a stop request.
	 */
	public boolean isStop();

	@Override
	default Object getSummary() {
		return new Object() {
			public String toString() {
				return ReconfigurableRequest.this.getRequestType()
						+ ":"
						+ ReconfigurableRequest.this.getServiceName()
						+ ReconfigurableRequest.this.getEpochNumber()
						+ (ReconfigurableRequest.this instanceof RequestIdentifier ? ":"
								+ ((RequestIdentifier) ReconfigurableRequest.this)
										.getRequestID()
								: "")
						+ (ReconfigurableRequest.this.isStop() ? ":[STOP]" : "");
			}
		};
	}
}
