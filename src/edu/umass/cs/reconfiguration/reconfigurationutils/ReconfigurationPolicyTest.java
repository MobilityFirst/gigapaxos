package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;

import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.utils.DefaultTest;

/**
 * @author arun
 * 
 *         A utility class to sanity check the reconfiguration policy
 *         implementation.
 *
 */
public class ReconfigurationPolicyTest extends DefaultTest {
	private static Class<?> C = AbstractDemandProfile.C;

	/**
	 * @param clazz
	 *            The class being tested.
	 */
	static void setClass(Class<?> clazz) {
		C = clazz;
	}

	/**
	 *
	 */
	@Before
	public void beforeMethod() {
		if(!C.getName().equals(ReconfigurationConfig.RC.DEMAND_PROFILE_TYPE.getDefaultValue()
				.toString())) {
			System.out.print(C.getSimpleName() + ":");
			super.beforeMethod();
		}
	}

	/**
	 *
	 */
	@After
	public void afterMethod() {
		verbose=1;
	}
	/**
	 */
	@Test
	public void testStringConstructor() {
		AbstractDemandProfile profile1 = AbstractDemandProfile
				.createDemandProfile(C, name1);
		Assert.assertEquals(name1, profile1.getName());
		AbstractDemandProfile profile2 = AbstractDemandProfile
				.createDemandProfile(C, name2);
		Assert.assertEquals(name2, profile2.getName());
	}

	private String name1 = "somename";
	private String name2 = "someothername";
	private int numRequests = 10;

	private Request getRequest(final String name) {
		return new Request() {

			@Override
			public IntegerPacketType getRequestType() {
				return new IntegerPacketType() {

					@Override
					public int getInt() {
						return 0;
					}
				};
			}

			@Override
			public String getServiceName() {
				return name;
			}
		};
	}

	/**
	 * @throws UnknownHostException
	 */
	@Test
	public void testJSONConstructor() throws UnknownHostException {
		AbstractDemandProfile profile = AbstractDemandProfile
				.createDemandProfile(C, name1);
		for (int i = 0; i < numRequests; i++)
			profile.shouldReportDemandStats(getRequest(name1),
					InetAddress.getByName("64.46.77." + i), getAppInfo());
		Assert.assertEquals(profile.getDemandStats().toString(),
				AbstractDemandProfile
						.createDemandProfile(C, profile.getDemandStats()).getDemandStats()
						.toString());
	}

	private static ReconfigurableAppInfo getAppInfo() {

		return new ReconfigurableAppInfo() {

			@Override
			public Set<String> getReplicaGroup(
					String serviceName) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String snapshot(String serviceName) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Map<String, InetSocketAddress> getAllActiveReplicas() {
				return null;
			}
		};
	}

	/**
	 * Sanity checks the implementation of a reconfiguration policy using a
	 * trivial default Request implementation.
	 * 
	 * @param clazz
	 */
	public static void testPolicyImplementation(Class<?> clazz) {
		setClass(clazz);
		Result result = JUnitCore.runClasses(ReconfigurationPolicyTest.class);
		for (Failure failure : result.getFailures()) 
			failure.getException().printStackTrace();
	}
}
