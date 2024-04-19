package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

public abstract class PrimaryBackupPacket implements ReplicableRequest {

    public static final String SERIALIZED_PREFIX = "pb:";

}
