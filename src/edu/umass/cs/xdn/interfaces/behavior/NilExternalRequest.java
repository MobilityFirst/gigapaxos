package edu.umass.cs.xdn.interfaces.behavior;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NilExternalRequest implements BehavioralRequest {
    @Override
    public Set<RequestBehaviorType> getBehaviors() {
        return new HashSet<>(List.of(RequestBehaviorType.NIL_EXTERNAL));
    }
}
