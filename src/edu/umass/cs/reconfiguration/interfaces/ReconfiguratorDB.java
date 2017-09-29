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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.ReconfigureUponActivesChange;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public interface ReconfiguratorDB<NodeIDType> {
	/* ******** Methods for individual names below **************** */

	/**
	 * @param name
	 * @return ReconfigurationRecord for {@code name}.
	 */
	public ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name);

	/**
	 * @param record
	 * @return ReconfigurationRecord created.
	 */
	public ReconfigurationRecord<NodeIDType> createReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> record);

	/**
	 * @param newActives
	 * @param nameStates
	 * @param reconfigureUponActivesChange 
	 * @return True if created successfully.
	 */
	public boolean createReconfigurationRecords(
			Map<String,String> nameStates, Set<NodeIDType> newActives, ReconfigureUponActivesChange reconfigureUponActivesChange);

	/**
	 * @param name
	 * @param epoch
	 * @return True if record existed and was successfully deleted.
	 */
	public boolean deleteReconfigurationRecord(String name, int epoch);

	/**
	 * @param name
	 * @param epoch
	 * @return True if record existed and was successfully marked as pending a
	 *         deletion.
	 */
	public boolean markDeleteReconfigurationRecord(String name, int epoch);

	/**
	 * Update demand statistics.
	 * 
	 * @param report
	 * @return True if updated successfully.
	 */
	public boolean updateDemandStats(DemandReport<NodeIDType> report);

	/**
	 * @param name
	 * @return Demand statistics as JSON string.
	 */
	public String getDemandStats(String name);

	/**
	 * Set epoch and state as specified.
	 * 
	 * @param name
	 * @param epoch
	 * @param state
	 * @return True if successfully set; false if the state transition is not
	 *         legitimate.
	 */
	public boolean setState(String name, int epoch,
			ReconfigurationRecord.RCStates state);

	/**
	 * Set epoch, state, newActives, primary only if current state is READY
	 * 
	 * @param name
	 * @param epoch
	 * @param state
	 * @param newActives
	 * @return True if successfully set; false if the state transition is not
	 *         legitimate.
	 */
	public boolean setStateInitReconfiguration(String name, int epoch,
			ReconfigurationRecord.RCStates state, Set<NodeIDType> newActives);
	
	/**
	 * @param nameStates
	 * @param epoch
	 * @param state
	 * @param newActives
	 * @return True if successfully set; false if any of the constituent state
	 *         transitions are not legitimate.
	 */
	public boolean setStateInitReconfiguration(Map<String,String> nameStates, int epoch,
			ReconfigurationRecord.RCStates state, Set<NodeIDType> newActives);

	/**
	 * Names for which reconfiguration is incomplete, needed for recovery
	 * 
	 * @return Array of names.
	 */
	public String[] getPendingReconfigurations();
	
	/**
	 * @param name
	 */
	public void removePending(String name);

	/**
	 * Get current RC group names from the DB
	 * 
	 * @return Map of RC group names and their group members.
	 */
	public Map<String, Set<NodeIDType>> getRCGroups();

	/**
	 * @return Set of all RC group names maintained locally.
	 */
	public Set<String> getRCGroupNames();

	/**
	 * @param node
	 * @param sockAddr
	 * @param version
	 * @return True if successfully added.
	 */
	public boolean addReconfigurator(NodeIDType node,
			InetSocketAddress sockAddr, int version);

	/**
	 * @param node
	 * @param sockAddr
	 * @param version
	 * @return True if successfully added.
	 */
	public boolean addActiveReplica(NodeIDType node,
			InetSocketAddress sockAddr, int version);

	
	/**
	 * @param version
	 * @return Delete deleted reconfigurators.
	 */
	public boolean garbageCollectOldReconfigurators(int version);


	/**
	 * Merge state, i.e., append instead of replacing state, exactly once. The
	 * tricky part is to ensure exactly-once semantics, which we ensure by
	 * maintaining a "merged" set in ReconfigurationRecord.
	 * 
	 * @param name
	 * @param epoch
	 * @param mergee
	 * @param mergeeEpoch 
	 * @param state
	 * @return True if merged successfully.
	 */
	public boolean mergeState(String name, int epoch, String mergee, int mergeeEpoch,
			String state);

	/**
	 * @param name
	 * @param epoch
	 * @param mergee
	 * @return True if intent committed successfully.
	 */
	public boolean mergeIntent(String name, int epoch, String mergee);

	/**
	 * @param name
	 * @param epoch
	 * @param state
	 * @param newActives 
	 * @param mergees
	 * @return True if state and mergees set successfully.
	 */
	public boolean setStateMerge(String name, int epoch, ReconfigurationRecord.RCStates state, Set<NodeIDType> newActives, Set<String> mergees);

	/**
	 * @param nameStates
	 * @param epoch
	 * @param state
	 * @param newActives
	 * @return True is state set successfully.
	 */
	public boolean setStateMerge(Map<String,String> nameStates, int epoch, ReconfigurationRecord.RCStates state, Set<NodeIDType> newActives);
	
	/**
	 * 
	 */
	public void delayedDeleteComplete();
	
	/**
	 * Clear all merged state in RC record. Invoked when a stop request is
	 * executed just before transitioning to the next epoch.
	 * 
	 * @param name
	 * @param epoch
	 */
	public void clearMerged(String name, int epoch);

	/**
	 * Sets RC epochs so that we know the epoch numbers for each reconfigurator
	 * group.
	 * 
	 * @param ncRecord
	 */
	public void setRCEpochs(ReconfigurationRecord<NodeIDType> ncRecord);

	/**
	 * @param node
	 */
	public void garbageCollectedDeletedNode(NodeIDType node);
		
	/**
	 * @param active
	 * @return Whether successfully opened.
	 */
	public boolean initiateReadActiveRecords(NodeIDType active);
	/**
	 * @param add 
	 * @return Next name
	 */
	public ReconfigurationRecord<NodeIDType> readNextActiveRecord(boolean add);
	/**
	 * @return Whether successfully closed.
	 */
	public boolean closeReadActiveRecords();
	
	/**
	 * @param finalStates
	 * @param mergerGroup
	 * @param epoch
	 * @return True if successfully merged.
	 */
	public String fetchAndAggregateMergeeStates(Map<String, String> finalStates,
			String mergerGroup, int epoch);
	/**
	 * @param callback
	 */
	public void setCallback(ReconfiguratorCallback callback);
	
	/**
	 * Close gracefully.
	 */
	public void close();
}
