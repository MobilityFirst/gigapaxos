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
package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         This sample implementation of {@link AbstractDemandProfile} maintains
 *         the demand profile for a single name and returns it as a JSONObject
 *         via its getStats() method. The specification of its methods is in the
 *         documentation of {@link AbstractDemandProfile}.
 */
public class DemandProfile extends AbstractDemandProfile {
	protected enum Keys {
		NAME, STATS, RATE, NREQS, NTOTREQS
	};

	/**
	 * The minimum number of requests after which a demand report will be sent
	 * to reconfigurators.
	 */
	protected static int minRequestsBeforeDemandReport = 1;
	/**
	 * The minimum amount of time (ms) that must elapse since the previous
	 * reconfiguration before the next reconfiguration can happen.
	 */
	protected static long minReconfigurationInterval = 000;
	/**
	 * The minimum number of requests between two successive reconfigurations.
	 */
	protected static long minRequestsBeforeReconfiguration = minRequestsBeforeDemandReport;

	protected double interArrivalTime = 0.0;
	protected long lastRequestTime = 0;
	protected int numRequests = 0;
	protected int numTotalRequests = 0;

	// Needed only at reconfigurators, so we don't need to serialize this.
	protected DemandProfile lastReconfiguredProfile = null;

	/**
	 * The string argument {@code name} is the service name for which this
	 * demand profile is being maintained.
	 * 
	 * @param name
	 */
	public DemandProfile(String name) {
		super(name);
	}

	/**
	 * Deep copy constructor. This constructor should create a copy of the
	 * supplied DemandProfile argument {@code dp} such that the newly
	 * constructed DemandProfile instance dpCopy != dp but dpCopy.equals(dp).
	 * 
	 * @param dp
	 */
	public DemandProfile(DemandProfile dp) {
		super(dp.name);
		this.interArrivalTime = dp.interArrivalTime;
		this.lastRequestTime = dp.lastRequestTime;
		this.numRequests = dp.numRequests;
		this.numTotalRequests = dp.numTotalRequests;
	}

	/**
	 * All {@link AbstractDemandProfile} instances must be constructible from a
	 * JSONObject.
	 * 
	 * @param json
	 * @throws JSONException
	 */
	public DemandProfile(JSONObject json) throws JSONException {
		super(json.getString(Keys.NAME.toString()));
		this.interArrivalTime = 1.0 / json.getDouble(Keys.RATE.toString());
		this.numRequests = json.getInt(Keys.NREQS.toString());
		this.numTotalRequests = json.getInt(Keys.NTOTREQS.toString());
	}

	/**
	 * 
	 * @param name
	 * @return All {@link AbstractDemandProfile} instances must support a single
	 *         String argument constructor that is the underlying service name.
	 */
	public static DemandProfile createDemandProfile(String name) {
		return new DemandProfile(name);
	}

	/**
	 * This method is used to inform the reconfiguration policy that
	 * {@code request} received from a client at IP address {@code sender}. The
	 * parameter {@code nodeConfig} provides the list of all active replica
	 * locations. The reconfiguration policy may use this information to
	 * assimilate a demand distribution and use that to determine whether and
	 * how to reconfigure the current set of replicas.
	 * 
	 * The simplistic example below ignores the {@code sender} information that
	 * in general is needed to determine the geo-distribution of demand.
	 */
	@Override
	public boolean shouldReportDemandStats(Request request, InetAddress sender,
			ReconfigurableAppInfo nodeConfig) {
		// incorporate request
		if (!request.getServiceName().equals(this.name))
			return false;
		this.numRequests++;
		this.numTotalRequests++;
		long iaTime = 0;
		if (lastRequestTime > 0) {
			iaTime = System.nanoTime() - this.lastRequestTime;
			this.interArrivalTime = Util
					.movingAverage(iaTime, interArrivalTime);
		} else
			lastRequestTime = System.nanoTime(); // initialization

		// determine whether to send demand report
		if (DISABLE_RECONFIGURATION)
			return false;
		if (getNumRequests() >= minRequestsBeforeDemandReport)
			return true;
		return false;

	}

	/**
	 * @return Request rate for the service name.
	 */
	public double getRequestRate() {
		return this.interArrivalTime > 0 ? 1.0 / this.interArrivalTime
				: 1.0 / (1000 * 1000 * 1000);
	}

	/**
	 * @return Number of requests for this service name since the most recent
	 *         demand report was sent to reconfigurators.
	 */
	public double getNumRequests() {
		return this.numRequests;
	}

	/**
	 * @return Total number of requests for this service name.
	 */
	public double getNumTotalRequests() {
		return this.numTotalRequests;
	}

	private static final boolean DISABLE_RECONFIGURATION = Config
			.getGlobalBoolean(RC.DISABLE_RECONFIGURATION);

	@Override
	public JSONObject getDemandStats() {
		JSONObject json = new JSONObject();
		try {
			json.put(Keys.NAME.toString(), this.name);
			json.put(Keys.RATE.toString(), getRequestRate());
			json.put(Keys.NREQS.toString(), getNumRequests());
			json.put(Keys.NTOTREQS.toString(), getNumTotalRequests());
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return json;
	}

	@Override
	public void combine(AbstractDemandProfile dp) {
		DemandProfile update = (DemandProfile) dp;
		this.lastRequestTime = Math.max(this.lastRequestTime,
				update.lastRequestTime);
		this.interArrivalTime = Util.movingAverage(update.interArrivalTime,
				this.interArrivalTime, update.getNumRequests());
		this.numRequests += update.numRequests;
		this.numTotalRequests += update.numTotalRequests;
	}

	@Override
	public Set<String> reconfigure(Set<String> curActives,
			ReconfigurableAppInfo nodeConfig) {
		if (this.lastReconfiguredProfile != null) {
			if (System.nanoTime()
					- this.lastReconfiguredProfile.lastRequestTime < minReconfigurationInterval)
				return null;
			if (this.numTotalRequests
					- this.lastReconfiguredProfile.numTotalRequests < minRequestsBeforeReconfiguration)
				return null;
		}
		/**
		 * This example simply returns curActives as this policy trivially
		 * reconfigures to the same set of locations after every call. In
		 * general, AbstractDemandProfile implementations should return a new
		 * list different from curActives.
		 */
		return curActives;
	}

	@Override
	public void justReconfigured() {
		// deep copy
		this.lastReconfiguredProfile = new DemandProfile(this); // this.clone();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ReconfigurationPolicyTest.testPolicyImplementation(DemandProfile.class);
	}
}
