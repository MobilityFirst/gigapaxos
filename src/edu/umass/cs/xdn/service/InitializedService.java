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
    public final String entryContainer;
    public final String statefulContainer;

    public InitializedService(ServiceProperty property, String serviceName, String networkName,
                              int allocatedHttpPort, List<String> containerNames) {
        this.property = property;
        this.serviceName = serviceName;
        this.networkName = networkName;
        this.allocatedHttpPort = allocatedHttpPort;
        this.containerNames = containerNames;

        assert property.getComponents().size() == containerNames.size() :
                "container names must be provided for all service component";

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
                String statefulComponent = componentAndStateDir[0];
                finalStateDirectory = componentAndStateDir[1];

                boolean exist = false;
                for (ServiceComponent c : property.getComponents()) {
                    if (!c.isStateful()) continue;
                    if (c.getComponentName().equals(statefulComponent)) {
                        exist = true;
                        break;
                    }
                }
                assert exist : "component specified in the state directory must be exist";
            }
        }
        assert finalStateDirectory == null || finalStateDirectory.endsWith("/") :
                "specified state directory must end with '/";
        this.stateDirectory = finalStateDirectory;

        // get the entry container
        String finalEntryContainer = null; int idx = 0;
        for (ServiceComponent c : property.getComponents()) {
            if (c.isEntryComponent()) finalEntryContainer = containerNames.get(idx);
            idx++;
        }
        this.entryContainer = finalEntryContainer;

        // get the stateful container
        String finalStatefulContainer = null; idx = 0;
        for (ServiceComponent c : property.getComponents()) {
            if (c.isStateful()) finalStatefulContainer = containerNames.get(idx);
            idx++;
        }
        this.statefulContainer = finalStatefulContainer;

    }
}
