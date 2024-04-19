package edu.umass.cs.primarybackup;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.primarybackup.packets.RequestPacket;

public record RequestAndCallback(RequestPacket requestPacket, ExecutedCallback callback) {
}
