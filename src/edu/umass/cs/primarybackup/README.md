# Primary Backup Manager

Primary Backup Manager (`PBManager`) is a manager for Primary Backup 
Replica Coordinator used in Gigapaxos. The `PBManager` uses Paxos to 
agree on the order of statediff and primary changes, in all Nodes.
