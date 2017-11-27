package edu.umass.cs.utils;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

//import edu.umass.cs.gigapaxos.paxosutil.RequestInstrumenter;
//import edu.umass.cs.gigapaxos.PaxosConfig.PC;

/**
 *
 */
public class DefaultTest {
	protected int verbose = 0;
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
	 * To repeat a test a given number of times.
	 */
	@Rule
	public RepeatRule repeatRule = new RepeatRule();

	/**
	 * 
	 */
	@Rule
	public TestWatcher watcher = new TestWatcher() {
		@Override
		protected void failed(Throwable e, Description description) {
			System.out.println(testName.getMethodName() + ":"+(repeatIndex)
					+ " FAILED!!!!!!!!!!!!! " + e);
			e.printStackTrace();
//			if (Config.getGlobalBoolean(PC.DEBUG))
//				System.out.println(RequestInstrumenter.getLog());
			System.exit(1);
		}

		@Override
		protected void succeeded(Description description) {
			if(verbose==0) System.out.println(" succeeded");
		}
	};

	/**
	 * To repeat a test a given number of times.
	 */

	int repeatIndex = 0, lastPrinted = 1;

	/**
	 * 
	 */
	@Before
	public void beforeMethod() {
		if (repeatRule.times() > 1)
			System.out
					.print(repeatIndex++ == 0 ? testName.getMethodName() + " "
							+ repeatIndex + " "

							: (repeatIndex == lastPrinted * 2 && (lastPrinted *= 2) > 0) ? (repeatIndex + " ")
									: "");
		else if(verbose==0)
			System.out.print(testName.getMethodName() + " ");
	}

	/**
	 * 
	 */
	@After
	public void afterMethod() {
		if (repeatIndex == repeatRule.times() && repeatIndex > lastPrinted)
			System.out.print(repeatIndex + " ");
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
	// @Test
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
