package edu.umass.cs.xdn.interfaces.behavior;

import java.util.Set;

/**
 * BehavioralRequest is end-user request that has certain behaviors. Having the request's behavior,
 * XDN can provide efficient coordination. For example, given {@link RequestBehaviorType#READ_ONLY}
 * request, then for certain coordination protocol XDN can serve the request locally, without
 * coordination with other replicas.
 */
public interface BehavioralRequest {

    /**
     * Provides the behaviors of end-user request that implements the {@link BehavioralRequest}
     * interface.
     *
     * @return Set of behaviors for this end-user's request.
     */
    Set<RequestBehaviorType> getBehaviors();

    // helper methods to check the behavior of a request
    public default boolean isReadOnlyRequest() {
        return this.getBehaviors().contains(RequestBehaviorType.READ_ONLY);
    }
    public default boolean isWriteOnlyRequest() {
        return this.getBehaviors().contains(RequestBehaviorType.WRITE_ONLY);
    }
    public default boolean isReadModifyWriteRequest() {
        return this.getBehaviors().contains(RequestBehaviorType.READ_MODIFY_WRITE);
    }
    public default boolean isMonotonicRequest() {
        return this.getBehaviors().contains(RequestBehaviorType.MONOTONIC);
    }
    public default boolean isNilExternal() {
        return this.getBehaviors().contains(RequestBehaviorType.NIL_EXTERNAL);
    }
}
