package edu.umass.cs.xdn;

public class XDNServiceProperties {
    public String serviceName;
    public String stateDir;
    public Integer exposedPort;
    public Integer mappedPort; // TODO: should be hidden
    public String consistencyModel;
    public boolean isDeterministic;
    public String containerName;
    public String dockerImageName;
}
