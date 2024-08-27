# Flexible consistency in XDN

!!! tip
    Developer can specify the intended consistency model, or even implement custom replication protocol with the
    provided API.

## Predefined consistency models

### Linearizable
TBD

### Sequential
TBD

### Causal
TBD

### Eventual
TBD

### PRAM
TBD

### Read your writes
TBD

### Writes follow reads
TBD

### Monotonic reads
TBD

### Monotonic writes
TBD

## Custom replication protocol

!!! note
    Informally, a **consistency model** provides gurantees on what are valid values observable from the read requests. 
    **Replication protocol** is the one that ensures the guarantee is satisfied and never broken by
    preventing invalid observations (i.e., upholding the safety property).

TBD

!!! note
    Replication protocol is commonly also referred as Coordination or Synchronization protocol. It is a class of 
    Distributed Protocol that manage the state in all the replicas, ensuring convergence.   