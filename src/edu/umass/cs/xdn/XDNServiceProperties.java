package edu.umass.cs.xdn;

import java.util.ArrayList;
import java.util.List;

public class XDNServiceProperties {
    // the attributes below are provided by the service owner
    // using the initial_state=xdn:init:dockerimg1,dockerimg2:port:consistency_model:is_deterministic:state_dir
    public String serviceName;
    public List<String> dockerImages = new ArrayList<>();
    public Integer exposedPort;
    public String consistencyModel;
    public boolean isDeterministic;
    public String stateDir;

    public String dockerImageName;

    // the attributes below are assigned by XDN, not the service owner
    public Integer mappedPort; // TODO: should be hidden
    public List<String> containerNames;
}
