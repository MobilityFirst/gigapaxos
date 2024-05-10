package edu.umass.cs.xdn.recorder;

public class ZipStateDiffRecorder extends AbstractStateDiffRecorder {

    private static final String defaultWorkingPath = "/tmp/xdn/statediff/zip/";

    public ZipStateDiffRecorder(String nodeID) {
        super(nodeID, defaultWorkingPath + nodeID + "/mnt/");
    }

    @Override
    public String getTargetDirectory(String serviceName) {
        return this.baseDirectoryPath + serviceName + "/";
    }

    @Override
    public boolean initialize(String serviceName) {
        // do nothing
        return true;
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
