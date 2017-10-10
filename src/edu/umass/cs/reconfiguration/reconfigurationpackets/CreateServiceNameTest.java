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
package edu.umass.cs.reconfiguration.reconfigurationpackets;

import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.utils.Util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * @author V. Arun
 * 
 *         This class has a field to specify the initial state in addition to
 *         the default fields in ClientReconfigurationPacket.
 */
public class CreateServiceNameTest {
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
			CreateServiceName bcreate1 = new CreateServiceName(isa, "random0",
					0, "hello");
			HashMap<String, String> nameStates = new HashMap<String, String>();
			for (int i = 0; i < names.length; i++)
				nameStates.put(names[i], states[i]);
			CreateServiceName bcreate2 = new CreateServiceName(isa, names[0],
					0, states[0], nameStates);
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
			assert (totalSize == numNames);
			System.out.println(bcreate2.getSummary());

			CreateServiceName c1 = new CreateServiceName("somename",
					"somestate", new HashSet<InetSocketAddress>(Arrays.asList(
					new InetSocketAddress(InetAddress
							.getLoopbackAddress(), 1234),
					new InetSocketAddress(InetAddress
							.getLoopbackAddress(), 1235))));
			assert (c1.toString().equals(new CreateServiceName(c1
					.toJSONObject()).toString())) : "\n" + c1 + " != \n"
					+ new CreateServiceName(c1.toJSONObject());
			System.out.println(c1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
