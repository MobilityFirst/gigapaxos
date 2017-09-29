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

import java.net.InetSocketAddress;

/**
 * @author arun
 *         <p>
 *
 *         An interface that simplifies messaging of responses back to clients
 *         by allowing the app to delegate response messaging to gigapaxos.
 */
public interface ClientRequest extends Request, RequestIdentifier {
	/**
	 * @return The socket address of the client that sent this request. This
	 *         method is now deprecated and exists only for backwards
	 *         compatibility, so it is okay for applications to not explicitly
	 *         override the default implementation of this method (that simply
	 *         returns null); {@link edu.umass.cs.reconfiguration.AbstractReplicaCoordinator
	 *         AbstractReplicaCoordinator} already knows the sending client's
	 *         address anyway.
	 */
	@Deprecated
	default public InetSocketAddress getClientAddress() {
		return null;
	}

	/**
	 * @return The response to be sent back to the client that issued this
	 *         request. {@link edu.umass.cs.reconfiguration.AbstractReplicaCoordinator}, e.g.,
	 *         {@link edu.umass.cs.gigapaxos.PaxosManager}, will invoke this method immediately after
	 *         {@link Replicable#execute(Request)} and, if the returned response
	 *         is non-null, will send it back to the client that issued that
	 *         request. Applications are expected to internally implement logic
	 *         that sets the response at the end of
	 *         {@link Replicable#execute(Request)} as follows:
	 *         <p>
	 *         public boolean {@link Replicable}.execute(Request request)} { <br>
	 *         // execute request to obtain response <br>
	 *         request.setResponse(response); <br>
	 *         }
	 */
	public ClientRequest getResponse();
}
