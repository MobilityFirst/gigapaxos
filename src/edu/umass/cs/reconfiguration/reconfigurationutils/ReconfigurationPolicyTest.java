package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationMain;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

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

	@Before
	public void beforeMethod() {
		super.beforeMethod();
		System.out.print(C.getSimpleName());
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
			profile.register(getRequest(name1),
					InetAddress.getByName("64.46.77." + i), getActives());
		Assert.assertEquals(profile.getStats().toString(),
				AbstractDemandProfile
						.createDemandProfile(C, profile.getStats()).getStats()
						.toString());
	}

	private static int numAddrs = 10;

	private static InterfaceGetActiveIPs getActives() {

		return new InterfaceGetActiveIPs() {
			@Override
			public ArrayList<InetAddress> getActiveIPs() {
				ArrayList<InetAddress> list = new ArrayList<InetAddress>();
				for (int i = 0; i < numAddrs; i++)
					try {
						list.add(InetAddress.getByName("43.22.67." + i));
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				return list;

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
