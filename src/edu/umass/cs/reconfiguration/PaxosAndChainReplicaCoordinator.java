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
 * Initial developer(s): Z. Gao */

package edu.umass.cs.reconfiguration;

import edu.umass.cs.chainreplication.ChainManager;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

public class PaxosAndChainReplicaCoordinator<NodeIDType>
        extends AbstractReplicaCoordinator<NodeIDType> {

    private final PaxosManager<NodeIDType> paxosManager;
    private final ChainManager<NodeIDType> chainManager;

    private final static String chainServiceNameSuffix = "_ch";

    // used to keep track of service names that use Paxos as replication algorithm
    // private static HashSet<String> paxosNames = new HashSet<>();

    /**
     *
     * @param app
     * @param myID
     * @param unstringer
     * @param niot
     */
    public PaxosAndChainReplicaCoordinator(Replicable app, NodeIDType myID,
                                           Stringifiable<NodeIDType> unstringer,
                                           Messenger<NodeIDType, ?> niot) {
        super(app, niot);
        assert (niot instanceof JSONMessenger);
        // initialize Managers
        this.paxosManager = new PaxosManager<NodeIDType>(myID, unstringer,
                (JSONMessenger<NodeIDType>) niot, this)
                .initClientMessenger(new InetSocketAddress(niot.getNodeConfig()
                        .getNodeAddress(myID), niot.getNodeConfig()
                        .getNodePort(myID)), niot);
        this.chainManager = null;
    }

    @Override
    public boolean coordinateRequest(Request request,
                                     ExecutedCallback callback)
            throws IOException, RequestParseException {

        if (!isChainReplicationName(request.getServiceName())){
            // use PaxosManager for coordination
        } else {
            // use ChainManager for coordination
        }

        return false;
    }

    private boolean isChainReplicationName(String serviceName) {
        // FIXME: the information to indicate that chain replication should be used is embedded in serviceName
        return serviceName.endsWith(chainServiceNameSuffix);
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
        assert (state != null);
        boolean created = false;
        if (!isChainReplicationName(serviceName)){
            // TODO: create chain
            // created = this.chainManager
        } else {
            // create the group as a Paxos group
            created = this.paxosManager.createPaxosInstanceForcibly(
                    serviceName, epoch, nodes, this, state, 0);
            // FIXME: this is always true, not sure why it did this
            boolean createdOrExistsOrHigher = (created || this.paxosManager
                    .equalOrHigherVersionExists(serviceName, epoch));

            if (!createdOrExistsOrHigher)
                throw new PaxosInstanceCreationException((this
                        + " failed to create " + serviceName + ":" + epoch
                        + " with state [" + state + "]") + "; existing_version=" +
                        this.paxosManager.getVersion(serviceName));
            return createdOrExistsOrHigher;
        }
        return created;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        return false;
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return null;
    }

    private static Set<IntegerPacketType> requestTypes = null;

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        if(requestTypes!=null) return requestTypes;
        // FIXME: get request types from a proper app
        Set<IntegerPacketType> types = this.app.getRequestTypes();
        if (types==null) types= new HashSet<IntegerPacketType>();
        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        return requestTypes = types;
    }

}
