package edu.umass.cs.xdn.service;

public enum ConsistencyModel {
    LINEARIZABILITY,
    SEQUENTIAL,
    CAUSAL,
    EVENTUAL,
    READ,
    WRITES_FOLLOW_READS,
    MONOTONIC_READS,
    MONOTONIC_WRITES
}
