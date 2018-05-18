package edu.umass.cs.reconfiguration.interfaces;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;

import java.util.Set;

public interface CoordinatorCallback<NodeIDType> extends ReconfiguratorCallback,AppRequestParser{

    void setCoordinator(AbstractReplicaCoordinator<NodeIDType> coordinator,Messenger messenger);

    Set<IntegerPacketType> getRequestTypes() ;

}
