package edu.umass.cs.xdn.recorder;

import edu.umass.cs.xdn.utils.Shell;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FuselogStateDiffRecorder extends AbstractStateDiffRecorder {

    private static final String FUSELOG_BIN_PATH = "/usr/local/bin/fuselog";
    private static final String FUSELOG_APPLY_BIN_PATH = "/usr/local/bin/fuselog-apply";

    private static final String defaultWorkingBasePath = "/tmp/xdn/state/fuselog/";

    private final String baseMountDirPath;
    private final String baseSocketDirPath;

    // important locations:
    // - /tmp/xdn/state/fuselog/<node-id>/                          the base directory
    // - /tmp/xdn/state/fuselog/<node-id>/mnt/                      the mount directory
    // - /tmp/xdn/state/fuselog/<node-id>/mnt/<service-name>/       mount dir of specific service
    // - /tmp/xdn/state/fuselog/<node-id>/sock/                     the socket directory
    // - /tmp/xdn/state/fuselog/<node-id>/sock/<service-name>.sock  socket to fs of specific service

    public FuselogStateDiffRecorder(String nodeID) {
        super(nodeID, defaultWorkingBasePath + nodeID + "/");

        // make sure that fuselog and fuselog-apply are exist
        File fuselog = new File(FUSELOG_BIN_PATH);
        assert fuselog.exists() : "fuselog binary does not exist at " + FUSELOG_BIN_PATH;
        File fuselogApplicator = new File(FUSELOG_APPLY_BIN_PATH);
        assert fuselogApplicator.exists() : "fuselog-apply binary does not exist at " +
                FUSELOG_APPLY_BIN_PATH;

        // create working mount dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/mnt/
        this.baseMountDirPath = this.baseDirectoryPath;
        try {
            Files.createDirectories(Paths.get(this.baseMountDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // create socket dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/sock/
        this.baseSocketDirPath = defaultWorkingBasePath + nodeID + "/sock/";
        try {
            Files.createDirectories(Paths.get(this.baseSocketDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public String getTargetDirectory(String serviceName) {
        return baseMountDirPath + serviceName + "/";
    }

    @Override
    public boolean preInitialization(String serviceName) {
        String targetDir = this.getTargetDirectory(serviceName);
        String socketFile = baseSocketDirPath + serviceName + ".sock";

        // create target mnt dir, if not exist
        // e.g., /tmp/xdn/state/rsync/node1/mnt/service1/
        try {
            Shell.runCommand("rm -rf " + targetDir);
            Files.createDirectory(Paths.get(targetDir));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // TODO: initialize file system in the mnt dir, with socket

        return true;
    }

    @Override
    public boolean postInitialization(String serviceName) {
        // TODO: read initialization stateDiff and discard it
        return false;
    }

    @Override
    public String captureStateDiff(String serviceName) {
        // TODO: use socket to read stateDiff captured by Fuselog
        return null;
    }

    @Override
    public boolean applyStateDiff(String serviceName, String encodedState) {
        // TODO: use fuselog-apply to apply stateDiff
        return false;
    }
}
