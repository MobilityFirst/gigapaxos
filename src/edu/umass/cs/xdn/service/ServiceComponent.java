package edu.umass.cs.xdn.service;

import java.util.Map;

public class ServiceComponent {
    private final String componentName;
    private final String imageName;

    /**
     * exposedPort is any tcp port this service component listen,
     * e.g. 3306 for database connection in MySQL, or 5432 for
     * database connection in PostgresSQL.
     */
    private final Integer exposedPort;
    private boolean isStateful;
    private final boolean isEntryComponent;

    /**
     * entryPort is the http port where this service component listen
     */
    private final Integer entryPort;
    private final Map<String, String> environmentVariables;

    protected ServiceComponent(String componentName, String imageName, Integer exposedPort,
                               boolean isStateful, boolean isEntryComponent, Integer entryPort,
                               Map<String, String> environmentVariables) {
        this.componentName = componentName;
        this.imageName = imageName;
        this.exposedPort = exposedPort;
        this.isStateful = isStateful;
        this.isEntryComponent = isEntryComponent;
        this.entryPort = entryPort;
        this.environmentVariables = environmentVariables;

        if (this.isEntryComponent && entryPort == null) {
            throw new RuntimeException("port is required for service's entry component");
        }
    }

    public String getComponentName() {
        return componentName;
    }

    public String getImageName() {
        return imageName;
    }

    public Integer getExposedPort() {
        return exposedPort;
    }

    public boolean isStateful() {
        return isStateful;
    }

    protected void setIsStateful(boolean isStateful) {
        this.isStateful = isStateful;
    }

    public boolean isEntryComponent() {
        return isEntryComponent;
    }

    public Integer getEntryPort() {
        return entryPort;
    }

    public Map<String, String> getEnvironmentVariables() {
        return environmentVariables;
    }
}
