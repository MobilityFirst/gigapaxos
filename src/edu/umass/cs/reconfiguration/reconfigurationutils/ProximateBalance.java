package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.nioutils.RTTEstimator;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class ProximateBalance extends DemandProfile {

	/* A map of the number of requests such that the InetAddress key is the
	 * closest active replica to the origin of that request. */
	private HashMap<InetSocketAddress, Integer> demandDistribution = new HashMap<InetSocketAddress, Integer>();

	private static enum PPKeys {
		DEM_DIST, NREADS, NTOTREADS,
	}

	static {
		DemandProfile.minReconfigurationInterval = 1000;
		DemandProfile.minRequestsBeforeDemandReport = 50;
		DemandProfile.minRequestsBeforeReconfiguration = 250;
	}

	// serialized into demand reports
	private int numReads = 0;
	private int numTotalReads = 0;

	// not serialized, and used only at reconfigurators
	private static long aggregateStartTime = System.nanoTime();
	private static int aggregateNumReads = 0;
	private static int aggregateNumRequests = 0;

	/**
	 * Required String constructor.
	 * 
	 * @param name
	 */
	public ProximateBalance(String name) {
		super(name);
	}

	/**
	 * Required JSONObject constructor.
	 * 
	 * @param json
	 * @throws JSONException
	 */
	public ProximateBalance(JSONObject json) throws JSONException {
		super(json);
		this.demandDistribution = unjsonify(json.getJSONObject(PPKeys.DEM_DIST
				.toString()));
		this.numReads = json.getInt(PPKeys.NREADS.toString());
		this.numTotalReads = json.getInt(PPKeys.NTOTREADS.toString());
		// No aggregate information is serialized
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
		super.shouldReportDemandStats(request, sender, nodeConfig);

		// isRead really means uncoordinated
		boolean isRead = !(request instanceof ReplicableRequest && ((ReplicableRequest) request)
				.needsCoordination());
		if (isRead)
			this.numReads++;
		this.numTotalReads++;
		// No aggregate information is serialized

		// update demand distribution
		synchronized (this.demandDistribution) {
			Set<InetAddress> closest = RTTEstimator.getClosest(sender);
			if (closest != null)
				for (InetAddress active : closest)
					for (InetSocketAddress isa : nodeConfig
							.getAllActiveReplicas().values())
						if (isa.getAddress().equals(active))
							this.demandDistribution
									.put(isa,
											!this.demandDistribution
													.containsKey(isa) ? 0
													: this.demandDistribution
															.get(isa) + 1);
		}

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
		JSONObject json = super.getDemandStats();
		try {

			// demand distribution map
			json.put(PPKeys.DEM_DIST.toString(),
					jsonify(this.demandDistribution));

			// to distinsguish reads and writes
			json.put(PPKeys.NREADS.toString(), this.numReads);
			json.put(PPKeys.NTOTREADS.toString(), this.numTotalReads);

			// No aggregate information is serialized
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return json;
	}

	@Override
	public DemandProfile clone() {
		return new DemandProfile(this);
	}

	@Override
	public void combine(AbstractDemandProfile dp) {
		super.combine(dp);
		ProximateBalance update = (ProximateBalance) dp;
		{
			this.lastRequestTime = Math.max(this.lastRequestTime,
					update.lastRequestTime);
			this.interArrivalTime = Util.movingAverage(update.interArrivalTime,
					this.interArrivalTime, update.getNumRequests());
			// this number is not meaningful at reconfigurators
			this.numRequests += update.numRequests;
			this.numTotalRequests += update.numTotalRequests;
		}
		this.numReads += update.numReads;
		this.numTotalReads += update.numTotalReads;

		aggregateNumReads += update.numReads;
		aggregateNumRequests += update.numRequests;

		resetIfNeeded();
	}

	/* This interval is rather arbitrary and is probably high enough for just
	 * about any service to not forget the past too easily, but it also means
	 * that smaller-scale services will not easily forget the past. */
	private static final int REFRESH_INTERVAL = 16 * 1024 * 1024;

	private static synchronized void resetIfNeeded() {
		if (aggregateNumRequests > REFRESH_INTERVAL) {
			aggregateStartTime = (System.nanoTime() + aggregateStartTime) / 2;
			aggregateNumReads /= 2;
			aggregateNumRequests /= 2;
		}
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
		// else determine closest actives
		return auspice(curActives, nodeConfig);
	}

	private static final double MIN_SERVER_LOAD = 1000.0; // /s

	/* Policy described in the Auspice Sigcomm'14 paper roughly with some
	 * differences.
	 * 
	 * (1) As described, Auspice uses the read/write ratio without taking read
	 * demand geo-distribution into account; for example, if the read/write
	 * ratio is very high but all reads came from just a single region, Auspice
	 * will choose the same number of replicas as it would for a name with all
	 * else identical but with a more geo-distributed read demand. The policy
	 * here takes read geo-distribution into account by choosing only as many
	 * replicas as needed to cover 75% of the recorded demand.
	 * 
	 * (2) Auspice used the parameter beta to enable replication to use
	 * available resources aggressively. The value of beta needs knowledge of
	 * the server capacities. We assume that each server's utilization is at
	 * least MIN_SERVER_LOAD=1000 requests/second and determine the replication
	 * factor accordingly. That is, this policy will replicate aggressively
	 * enough so as to try to ensure that the average load per server is at
	 * least that much. */
	private Set<String> auspice(Set<String> curActives,
			ReconfigurableAppInfo nodeConfig) {

		Map<String, InetSocketAddress> activesMap = nodeConfig
				.getAllActiveReplicas();
		Collection<InetSocketAddress> actives = (activesMap.values());
		// ensure minimum utilization
		int avgNumReplicas = (int) Math.max(
				Config.getGlobalInt(RC.DEFAULT_NUM_REPLICAS),

				/* The total load actives.size() * MIN_SERVER_CAPACITY should be
				 * equal to (1 + k/aggregateRWRatio)*aggregate_read_rate, where
				 * we have assumed that each write counts as a load equivalent
				 * of k reads if there are k replicas. */
				(actives.size() * MIN_SERVER_LOAD / getAggregateReadRate() - 1)
						* getAggregateRWRatio());

		// replicate proportional to rwRatio
		int numReplicas = (int) Math.max(
				Config.getGlobalInt(RC.DEFAULT_NUM_REPLICAS), avgNumReplicas
						* this.getRWRatio() / getAggregateRWRatio());

		// sort by increasing demand
		ArrayList<InetSocketAddress> newActives = new ArrayList<InetSocketAddress>(
				Util.sortByValue(this.demandDistribution).keySet());
		/* The block below picks as many actives as are needed to cover 75% of
		 * the total demand seen by this node. */
		{
			int totalDemand = 0;
			for (int j = newActives.size() - 1; j >= 0; j--)
				totalDemand += this.demandDistribution.get(newActives.get(j));

			// only include requests accounting for 75% of the demand
			int unnecessary = newActives.size() - 1;
			for (int curTotal = 0; unnecessary >= 0; unnecessary--)
				if ((curTotal += this.demandDistribution.get(newActives
						.get(unnecessary))) > (int) (0.75 * totalDemand))
					break;
			for (int j = 0; j < unnecessary; j++)
				newActives.remove(0);
		}

		// trim down to numReplicas
		for (int j = 0; j < newActives.size() - numReplicas; j++)
			newActives.remove(0);
		// have at least minimum number of replicas
		if (newActives.size() < Config.getGlobalInt(RC.DEFAULT_NUM_REPLICAS))
			for (String curActive : curActives)
				if (newActives.size() < Config
						.getGlobalInt(RC.DEFAULT_NUM_REPLICAS)
						&& !newActives.contains(curActive))
					newActives.add(activesMap.get(curActive));

		// reverse so that closest is first
		Collections.reverse(newActives);
		Set<String> retval = new HashSet<String>();
		for (String active : activesMap.keySet())
			if (newActives.contains(activesMap.get(active)))
				retval.add(active);
		return retval;
	}

	private static double getAggregateRequestRate() {
		return aggregateNumRequests * 1000.0
				/ (System.nanoTime() - aggregateStartTime) * 1000 * 1000;
		// aggregateIAT / 1000.0 / 1000.0 / 1000.0;
	}

	private static double getAggregateRWRatio() {
		return (aggregateNumReads * 1.0 / Math.max(1, aggregateNumRequests
				- aggregateNumReads));
	}

	private static double getAggregateReadRate() {
		return getAggregateRequestRate() * getAggregateRWRatio()
				/ (1 + getAggregateRWRatio());
	}

	private double getRWRatio() {
		return (this.numTotalReads * 1.0 / Math.max(1, this.numTotalRequests
				- this.numTotalReads));
	}

	@Override
	public void justReconfigured() {
		this.lastReconfiguredProfile = this.clone();
	}

	private static JSONObject jsonify(Map<InetSocketAddress, Integer> map)
			throws JSONException {
		JSONObject json = new JSONObject();
		for (InetSocketAddress addr : map.keySet())
			json.put(addr.getAddress().getHostAddress() + ":" + addr.getPort(),
					map.get(addr));
		return json;
	}

	private static HashMap<InetSocketAddress, Integer> unjsonify(JSONObject json)
			throws JSONException {
		String[] keys = JSONObject.getNames(json);
		HashMap<InetSocketAddress, Integer> map = new HashMap<InetSocketAddress, Integer>();
		if (keys != null)
			for (String addrStr : keys)
				map.put(Util.getInetSocketAddressFromString(addrStr),
						json.getInt(addrStr));
		return map;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ReconfigurationPolicyTest
				.testPolicyImplementation(ProximateBalance.class);
	}
}
