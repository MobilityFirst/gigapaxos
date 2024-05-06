

## Architecture of Coordinator Wrapper in XDN

```
                 XDNReplicaCoordinator
          _________________|_________________
         |                                   |
PaxosReplicaCoordinator         PrimaryBackupReplicaCoordinator
         |                                   |        
    PaxosManager                    PrimaryBackupManager
         |                                   |
         |                              PaxosManager
         |                                   |
         |                           PaxosMiddlewareApp
         |___________________________________|
                           |           
                    XDNGigapaxosApp
```