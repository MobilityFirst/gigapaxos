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

/**
 * @author V. Arun
 */
public interface ReplicableRequest extends Request, RequestIdentifier {

	/**
	 * @return True if this request needs to be coordinated, false otherwise.
	 */
	public boolean needsCoordination();

	/**
	 * After this method returns, {@link #needsCoordination()} should
	 * subsequently return {@code b}. This method is invoked by
	 * {@link edu.umass.cs.reconfiguration.AbstractReplicaCoordinator} with a {@code false} argument just
	 * before coordinating the request so that coordinated packets are not
	 * coordinated again infinitely. For example, if a replica coordinator's
	 * coordination strategy is to simply flood the request to all replicas,
	 * there needs to be a way for a recipient of a copy of this already once
	 * coordinated request to know that it should not coordinate it again. This
	 * method provides {@link edu.umass.cs.reconfiguration.AbstractReplicaCoordinator} a placeholder in the
	 * application request to prevent such infinite coordination loops.
	 * <p>
	 *
	 * This method is now deprecated and does not need to be overridden by
	 * implementing classes. It's original purpose was really only to be able to
	 * easily do lazy propagation replica coordination without introducing an
	 * additional type (just to distinguish between the original request at the
	 * entry replica and the replicated request so as to avoid an infinite
	 * propagation loop), but introducing an additional type is the clean way to
	 * do this. Paxos for example already has its own types, so it doesn't need
	 * this method.
	 * 
	 * @param b
	 *            True if subsequent invocations of {@link #needsCoordination()}
	 *            must return true, false otherwise.
	 */
	@Deprecated
	default public void setNeedsCoordination(boolean b) {

	}
}
