package edu.umass.cs.primarybackup.interfaces;

/**
 * BackupableApplication is intended for PrimaryBackupReplicaCoordinator so that a primary can
 * capture statediff of running application and a backup application can apply statediffs, and
 * a primary can capture stat. As with the execute() method, captureStatediff() and applyStatediff()
 * should be done atomically.
 *
 * TODO: design activate(.) and deactivate(.) methods.
 *
 */
public interface BackupableApplication {
    public String captureStatediff(String serviceName);
    public boolean applyStatediff(String serviceName, String statediff);
}
