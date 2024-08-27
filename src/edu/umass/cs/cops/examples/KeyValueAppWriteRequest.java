package edu.umass.cs.cops.examples;

import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import edu.umass.cs.xdn.interfaces.behavior.RequestBehaviorType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KeyValueAppWriteRequest extends KeyValueAppRequest implements BehavioralRequest {
    public KeyValueAppWriteRequest(String serviceName, long clientID, long requestID, String key, String value) {
        super(serviceName, clientID, requestID, key, value);
    }

    @Override
    public Set<RequestBehaviorType> getBehaviors() {
        return new HashSet<>(List.of(RequestBehaviorType.WRITE_ONLY));
    }
}
