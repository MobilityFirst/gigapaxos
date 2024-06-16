package edu.umass.cs.reconfiguration.interfaces;

public interface InitialStateValidator {

    /**
     * validateInitialState validates the given initialState provided to start a Replicable
     * instance. The method must throw InvalidInitialStateException if the provided initialState is
     * incorrect. Otherwise, the initialState is assumed to be correct and will be used to create a
     * Replica Group. Doing the validation before Replica Group creation is preferred so all
     * Replicas will be successfully instantiated.
     *
     * <p> Note that the validation *must* be deterministic since it will be executed independently
     * in all replicas and must yield the same result.
     *
     * @param initialState the initialState provided to start Replicable instance
     * @throws InvalidInitialStateException when the given initialState is invalid
     */
    public void validateInitialState(String initialState) throws InvalidInitialStateException;

    class InvalidInitialStateException extends Exception {
        public InvalidInitialStateException(String message) {
            super(message);
        }
    }
}
