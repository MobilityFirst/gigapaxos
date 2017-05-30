/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.interfaces;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;

/**
 * @author V. Arun
 *         <p>
 *         The method executed(.) in this interface is called only by
 *         AbstractReplicaCoordinator that is an internal class. This interface
 *         is not (yet) expected to be implemented by a third-party class like
 *         an instance of Application.
 */
public interface ReconfiguratorCallback extends ExecutedCallback {
	/**
	 * @param request
	 * @return True if execution complete.
	 */
	public boolean preExecuted(Request request);
	
	/**
	 * @param name
	 * @param state
	 * @return True if app's restore method doesn't have to be called.
	 */
	default boolean preRestore(String name, String state) {
		return false;
	}

	/**
	 * @param name
	 * @return Checkpoint state.
	 */
	default String preCheckpoint(String name) {
		return null;
	}
}
