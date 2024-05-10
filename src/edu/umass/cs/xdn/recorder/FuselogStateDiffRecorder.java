package edu.umass.cs.xdn.recorder;

public class FuselogStateDiffRecorder extends AbstractStateDiffRecorder {

    private static final String FUSELOG_BIN_PATH = "/usr/local/bin/fuselog";
    private static final String FUSELOG_APPLY_BIN_PATH = "/usr/local/bin/fuselog_apply";

    private static final String defaultWorkingPath = "/tmp/xdn/state/fuselog/";

    public FuselogStateDiffRecorder(String nodeID) {
        super(nodeID, defaultWorkingPath + nodeID + "/");
    }

    @Override
    public String getTargetDirectory(String serviceName) {
        return this.baseDirectoryPath + serviceName + "/";
    }

    @Override
    public boolean initialize(String serviceName) {
        return false;
    }

    @Override
    public String captureStateDiff(String serviceName) {
        return null;
    }

    @Override
    public boolean applyStateDiff(String serviceName, String encodedState) {
        return false;
    }
}
