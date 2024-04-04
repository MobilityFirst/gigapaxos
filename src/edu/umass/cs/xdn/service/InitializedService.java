package edu.umass.cs.xdn.service;

import java.util.List;

public class InitializedService {
    public final ServiceProperty property;
    public final String serviceName;
    public final String networkName;

    /** port in which this service receives HTTP request forwarded by XDN */
    public final int allocatedHttpPort;

    /** containerNames contains list of container names for each component in the service */
    public final List<String> containerNames;

    /** state directory inside the stateful component of this service */
    public final String stateDirectory;

    public InitializedService(ServiceProperty property, String serviceName, String networkName,
                              int allocatedHttpPort, List<String> containerNames) {
        this.property = property;
        this.serviceName = serviceName;
        this.networkName = networkName;
        this.allocatedHttpPort = allocatedHttpPort;
        this.containerNames = containerNames;

        // parse state directory which can be in these two forms:
        // - "/data/"
        // - "backend:/data/", where backend is a component name
        String finalStateDirectory = null;
        if (property.getStateDirectory() != null && !property.getStateDirectory().isEmpty()) {
            String[] componentAndStateDir = property.getStateDirectory().split(":");
            if (componentAndStateDir.length == 1) {
                finalStateDirectory = componentAndStateDir[0];
            }
            if (componentAndStateDir.length == 2) {
                finalStateDirectory = componentAndStateDir[1];
            }
        }
        this.stateDirectory = finalStateDirectory;
    }
}
