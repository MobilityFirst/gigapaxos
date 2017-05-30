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

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;

/**
 * @author V. Arun
 * 
 *         An implementation, X, of this abstract class must satisfy the
 *         following requirements in addition to those enforced by the abstract
 *         methods explicitly listed herein:
 * 
 *         (1) X must have a constructor that takes a single JSONObject as an
 *         argument. Otherwise, the static method
 *         {@link #createDemandProfile(JSONObject)} that reflectively creates an
 *         instance of this class from a JSONObject will fail.
 * 
 *         (2) X must have a constructor that takes a single String as an
 *         argument. Otherwise, the static method
 *         {@link #createDemandProfile(String)} that reflectively creates an
 *         instance of this class from a JSONObject will fail.
 * 
 *         (3) For any instance x of X, x.equals(new X(x.getStats()) must return
 *         true.
 * 
 *         (4) It is slightly more efficient (but not necessary) for X to
 *         override the methods {@link #createDemandProfile(String)} and
 *         {@link #createDemandProfile(JSONObject)} method to create an instance
 *         of X rather than having the default implementation do so via
 *         reflection.
 * 
 *         Refer to reconfiguration.reconfigurationutils.DemandProfile for an
 *         example implementation.
 * 
 *         Use {@link ReconfigurationPolicyTest#testPolicyImplementation(Class)}
 *         to test an implementation of this class.
 */

@SuppressWarnings("javadoc")
public abstract class AbstractDemandProfile {
	static final Class<?> C = ReconfigurationConfig.getDemandProfile();

	protected static enum Keys {
		SERVICE_NAME
	};

	protected final String name;

	/**
	 * @param name
	 *            The service name of the reconfiguree replica group.
	 */
	public AbstractDemandProfile(String name) {
		this.name = name;
	}

	/*********************** Start of abstract methods ***************/

	/**
	 * This method incorporates new information about client requests, i.e.,
	 * {@code request} was received from {@code sender}. The list of all active
	 * replicas can be obtained from
	 * {@link ReconfigurableAppInfo#getAllActiveReplicas()} ; this may be useful
	 * to determine whether other active replicas might be better suited to
	 * service requests from this sender.
	 * 
	 * This method must implement a policy that tells whether it is time to
	 * report from an active replica to a reconfigurator. Active replicas in
	 * general should not report upon every request, but at some coarser
	 * frequency to limit overhead.
	 * 
	 * @return Whether the active replica should send a demand report to the
	 *         reconfigurator.
	 * 
	 * @param request
	 * @param sender
	 * @param nodeConfig
	 */
	public abstract boolean shouldReportDemandStats(Request request,
			InetAddress sender, ReconfigurableAppInfo nodeConfig);

	/**
	 * All relevant stats must be serializable into JSONObject. Any information
	 * not included in the returned JSONObject will not be available for use in
	 * the reconfiguration policy in
	 * {@link AbstractDemandProfile#reconfigure(Set, ReconfigurableAppInfo)}
	 * that is invoked at reconfigurators (not active replicas).
	 * 
	 * @return Demand statistics as JSON.
	 */
	public abstract JSONObject getDemandStats();

	/**
	 * Combine the new information in {@code update} into {@code this}. This
	 * method is used at reconfigurators to combine a newly received demand
	 * report with an existing demand report.
	 * 
	 * @param update
	 */
	public abstract void combine(AbstractDemandProfile update);

	/**
	 * The main reconfiguration policy invoked at reconfigurators (not active
	 * replicas).
	 * 
	 * @param curActives
	 * @param nodeConfig
	 * 
	 * @return The list of reconfigured actives for the new placement. Simply
	 *         returning null means no reconfiguration will happen. Returning a
	 *         list that is the same as curActives means that a trivial
	 *         reconfiguration will happen unless
	 *         {@link RC#RECONFIGURE_IN_PLACE} is set to false.
	 */
	public abstract Set<String> reconfigure(Set<String> curActives,
			ReconfigurableAppInfo appInfo);

	/**
	 * Tells {@code this} that the current demand profile was just used to
	 * perform reconfiguration. This information is useful for implementing
	 * policies based on the difference between the current demand profile and
	 * the one at the time of the most recent reconfiguration, e.g., reconfigure
	 * only if the demand from some region has changed by more than 10%.
	 */
	public abstract void justReconfigured();

	/* ********************** End of abstract methods ************** */

	/**
	 * @return Name of reconfiguree.
	 */
	public final String getName() {
		return this.name;
	}

	/**
	 * Creates a deep copy of this object. So, it must be the case that the
	 * return value != this, but the return value.equals(this). You may also
	 * need to override equals(.) and hashCode() methods accordingly. If an
	 * implementation of this class consists of non-primitive types, e.g., a
	 * {@link Map}, then a new Map has to be created inside the clone() method.
	 * 
	 */
	@Deprecated
	public AbstractDemandProfile clone() {
		return this;
	}

	/**
	 * Clear all info, i.e., forget all previous stats.
	 */
	@Deprecated
	public void reset() {
	}

	protected static AbstractDemandProfile createDemandProfile(String name) {
		return createDemandProfile(C, name);
	}

	protected static AbstractDemandProfile createDemandProfile(Class<?> clazz,
			String name) {
		try {
			assert (clazz != null);
			return (AbstractDemandProfile) clazz.getConstructor(String.class)
					.newInstance(name);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param json
	 * @return Reflection-based constructor to create demand profile.
	 */
	public static AbstractDemandProfile createDemandProfile(JSONObject json) {
		return createDemandProfile(C, json);
	}

	static AbstractDemandProfile createDemandProfile(Class<?> clazz,
			JSONObject json) {
		try {
			return (AbstractDemandProfile) clazz.getConstructor(
					JSONObject.class).newInstance(json);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			ReconfigurationConfig.getLogger().severe(
					e.getClass().getSimpleName() + " while creating " + clazz
							+ " with JSONObject " + json);
			e.printStackTrace();
		}
		return null;
	}
}
