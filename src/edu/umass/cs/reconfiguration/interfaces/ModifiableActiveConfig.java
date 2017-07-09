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
package edu.umass.cs.reconfiguration.interfaces;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author arun
 *
 * @param <Node>
 */
public interface ModifiableActiveConfig<Node> extends
		ReconfigurableNodeConfig<Node> {

	/**
	 * @param id
	 * @param sockAddr
	 * @return Socket address previously mapped to this id. But we really should
	 *         not allow mappings to be changed via an add method.
	 */
	public InetSocketAddress addActiveReplica(Node id,
			InetSocketAddress sockAddr);

	/**
	 * @param id
	 * @return Socket address to which {@code id} was mapped.
	 */
	public InetSocketAddress removeActiveReplica(Node id);

	/**
	 * Unused. 
	 * 
	 * @return Version number of node config.
	 */
	public long getVersion();

	/**
	 * @return Active replicas as map.
	 */
	default Map<String, InetSocketAddress> getActiveReplicasMap() {
		return new Map<String, InetSocketAddress>() {

			@Override
			public int size() {
				return getActiveReplicasReadOnly().size();
			}

			@Override
			public boolean isEmpty() {
				return getActiveReplicasReadOnly().isEmpty();
			}

			// convert key to NodeIDType
			@Override
			public boolean containsKey(Object key) {
				return getActiveReplicasReadOnly().containsKey(
						valueOf(key.toString()));
			}

			@Override
			public boolean containsValue(Object value) {
				return getActiveReplicasReadOnly().containsValue(value);
			}

			// convert key to NodeIDType
			@Override
			public InetSocketAddress get(Object key) {
				return getActiveReplicasReadOnly().get(valueOf(key.toString()));
			}

			@Override
			public InetSocketAddress put(String key, InetSocketAddress value) {
				throw new UnsupportedOperationException();
			}

			@Override
			public InetSocketAddress remove(Object key) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void putAll(
					Map<? extends String, ? extends InetSocketAddress> m) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void clear() {
				throw new UnsupportedOperationException();
			}

			@Override
			public Set<String> keySet() {
				Set<String> keys = new HashSet<String>();
				for (Node node : getActiveReplicasReadOnly().keySet())
					keys.add(node.toString());
				return keys;
			}

			@Override
			public Collection<InetSocketAddress> values() {
				return getActiveReplicasReadOnly().values();
			}

			@Override
			public Set<java.util.Map.Entry<String, InetSocketAddress>> entrySet() {
				Set<java.util.Map.Entry<String, InetSocketAddress>> set = new HashSet<java.util.Map.Entry<String, InetSocketAddress>>();
				for (Node node : getActiveReplicasReadOnly().keySet())
					set.add(new java.util.Map.Entry<String, InetSocketAddress>() {

						@Override
						public String getKey() {
							return node.toString();
						}

						@Override
						public InetSocketAddress getValue() {
							return getActiveReplicasReadOnly().get(node);
						}

						@Override
						public InetSocketAddress setValue(
								InetSocketAddress value) {
							throw new UnsupportedOperationException();
						}
					});
				return set;
			}
		};
	}
}
