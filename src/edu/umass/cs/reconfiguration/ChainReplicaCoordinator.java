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
import edu.umass.cs.chainreplication.chainpackets.ChainPacket;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.ReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;

public class ChainReplicaCoordinator<NodeIDType>
        extends AbstractReplicaCoordinator<NodeIDType> {

    private final ChainManager<NodeIDType> chainManager;

    public ChainReplicaCoordinator(Replicable app, NodeIDType myID,
                                   Stringifiable<NodeIDType> unstringer,
                                   Messenger<NodeIDType, ?> niot) {
        super(app, niot);
        assert (niot instanceof JSONMessenger);
        this.chainManager = new ChainManager<>(myID, unstringer,
                (JSONMessenger<NodeIDType>) niot, this, null,
                true);
    }

    private static Set<IntegerPacketType> requestTypes = null;

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        if(requestTypes!=null) return requestTypes;
        // FIXME: get request types from a proper app
        Set<IntegerPacketType> types = this.app.getRequestTypes();

        if (types==null) types= new HashSet<IntegerPacketType>();
        /* Need to add this separately because paxos won't initClientMessenger
         * automatically with ReconfigurableNode unlike PaxosServer.
         */

        for (IntegerPacketType type: ChainPacket.ChainPacketType.values())
            types.add(type);

        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        return requestTypes = types;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback) throws IOException, RequestParseException {
        // coordinate the request by forwarding to the next node in the chain
        // System.out.println(">>>>> Coordinate: "+request);
        // return true;
        return this.chainManager.propose(request.getServiceName(), request, callback)!= null;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
        // System.out.println(">>>>> Create: "+serviceName+", on "+this.getMyID());

        return this.chainManager.createReplicatedChainForcibly(
                serviceName, epoch, nodes, this, state);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        return this.chainManager.deleteReplicatedChain(serviceName, epoch);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return this.chainManager.getReplicaGroup(serviceName);
    }

    @Override
    public Integer getEpoch(String name) {
        return this.chainManager.getVersion(name);
    }

    @Override
    public String getFinalState(String name, int epoch) {
        return null;
    }

    public static void main(String[] args){
        String coordinatorClassName = "edu.umass.cs.reconfiguration.PaxosReplicaCoordinator";
        Class<?> c;
        ReplicaCoordinator<?> coordinator = null;
        try {
            c = Class.forName(coordinatorClassName);
            // System.out.println(c.getConstructors());
            Constructor<?>[] cons = c.getConstructors();
            for (int i=0; i<cons.length; i++){
                System.out.println(cons[i]);
            }
            ;
            coordinator = (ReplicaCoordinator<?>) c.getDeclaredConstructor(
                    Replicable.class, Object.class, Stringifiable.class, JSONMessenger.class)
                    .newInstance(null, new Object(), null, null);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }

    }

}
