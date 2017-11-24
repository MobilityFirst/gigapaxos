package edu.umass.cs.reconfiguration.reconfigurationpackets;

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.utils.Util;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author arun
 *
 * This class is not used.
 */
public class BatchedCreateServiceNameTest  {

	public static void main(String[] args) {
		try {
			Util.assertAssertionsEnabled();
			InetSocketAddress isa = new InetSocketAddress(
					InetAddress.getByName("localhost"), 2345);
			int numNames = 1000;
			String[] reconfigurators = { "RC43", "RC22", "RC78", "RC21",
					"RC143" };
			String namePrefix = "someName";
			String defaultState = "default_initial_state";
			String[] names = new String[numNames];
			String[] states = new String[numNames];
			for (int i = 0; i < numNames; i++) {
				names[i] = namePrefix + i;
				states[i] = defaultState + i;
			}
			BatchedCreateServiceName bcreate1 = new BatchedCreateServiceName(
					isa, "random0", 0, "hello");
			BatchedCreateServiceName bcreate2 = new BatchedCreateServiceName(
					isa, names, 0, states);
			System.out.println(bcreate1.toString());
			System.out.println(bcreate2.toString());

			// translate a batch into consistent constituent batches
			Collection<Set<String>> batches = ConsistentReconfigurableNodeConfig
					.splitIntoRCGroups(
							new HashSet<String>(Arrays.asList(names)),
							new HashSet<String>(Arrays.asList(reconfigurators)));
			int totalSize = 0;
			int numBatches = 0;
			for (Set<String> batch : batches)
				System.out.println("batch#" + numBatches++ + " of size "
						+ batch.size() + " (totalSize = "
						+ (totalSize += batch.size()) + ")" + " = " + batch);
			assert(totalSize==numNames);
			System.out.println(bcreate2.getSummary());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
