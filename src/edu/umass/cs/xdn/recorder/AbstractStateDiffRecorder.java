package edu.umass.cs.xdn.recorder;

public abstract class AbstractStateDiffRecorder {

    private final String nodeID;
    protected final String baseDirectoryPath;

    protected AbstractStateDiffRecorder(String nodeID, String basePath) {
        this.nodeID = nodeID;
        this.baseDirectoryPath = basePath;
    }

    abstract public String getTargetDirectory(String serviceName);

    abstract public boolean preInitialization(String serviceName);

    abstract public boolean postInitialization(String serviceName);

    abstract public String captureStateDiff(String serviceName);

    abstract public boolean applyStateDiff(String serviceName, String encodedState);

}
