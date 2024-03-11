package edu.umass.cs.primarybackup.interfaces;

/**
 * BackupApplication is intended for PrimaryBackupReplicaCoordinator so
 * that a backup application can apply statediffs. As with the execute()
 * method, applyStatediff() should be done atomically.
 */
public interface BackupableApplication {
    public boolean applyStatediff(String serviceName, String statediff);
}
