package edu.umass.cs.utils;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.paxosutil.E2ELatencyAwareRedirector;

/**
 *
 */
public class DefaultTest {
	/**
	 * 
	 */
	public DefaultTest() {
	}

	/**
	 * 
	 */
	@Rule
	public TestName testName = new TestName();

	/**
	 * 
	 */
	@Rule
	public TestWatcher watcher = new TestWatcher() {
		@Override
		protected void failed(Throwable e, Description description) {
			System.out.println(" FAILED!!!!!!!!!!!!! " + e);
			e.printStackTrace();
			System.exit(1);
		}

		@Override
		protected void succeeded(Description description) {
			if (!testName.getMethodName().equals("testPackage"))
				System.out.println(" succeeded");
		}
	};

	/**
	 * 
	 */
	@Before
	public void beforeMethod() {
		if (!testName.getMethodName().equals("testPackage"))
			System.out.print(testName.getMethodName() + " ");
	}

	/**
	 * @param classes
	 * @throws ClassNotFoundException
	 */
	public static void testAll(List<Class<?>> classes)
			throws ClassNotFoundException {
		testAll(classes.toArray(new Class<?>[0]));
	}

	private static final String MARKER = "--------------------";

	/**
	 * @param classes
	 */
	public static void testAll(Class<?>... classes) {
		for (Class<?> clazz : classes) {
			if (!clazz.getName().endsWith("Test"))
				continue;
			System.out.println("\n\n" + MARKER + "Testing "
					+ clazz.getSimpleName() + MARKER);
			Result result = JUnitCore.runClasses((clazz));
			System.out
					.println("Completed "
							+ result.getRunCount()
							+ " tests"
							+ (result.getFailureCount() > 0 ? " and found the following failures"
									: ""));
			for (Failure failure : result.getFailures()) {
				System.out.println(failure.toString());
				failure.getException().printStackTrace();
			}
		}
	}

	private static boolean called = false;

	/**
	 * 
	 */
	@Test
	public void test() {
		
	}
	/**
	 * @throws ClassNotFoundException
	 */
	//@Test
	public void testPackage() throws ClassNotFoundException {
		if (called)
			return;
		else
			called = true;
		String packageRegex = System.getProperty("pkg");
		if (packageRegex != null) {
			System.out.println("packageRgegex=" + packageRegex);
			testAll((ClassFinder.findRegex(".*" + packageRegex + ".*")));
		}
	}
}
