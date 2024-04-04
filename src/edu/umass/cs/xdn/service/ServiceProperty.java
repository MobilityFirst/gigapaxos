package edu.umass.cs.xdn.service;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.*;

@RunWith(Enclosed.class)
public class ServiceProperty {
    private final String serviceName;
    private final boolean isDeterministic;

    /**
     * either "/data/" or "datastore:/data/"
     */
    private final String stateDirectory;
    private final ConsistencyModel consistencyModel;
    private final List<ServiceComponent> components;

    private ServiceComponent entryComponent;
    private ServiceComponent statefulComponent;

    private ServiceProperty(String serviceName, boolean isDeterministic, String stateDirectory,
                            ConsistencyModel consistencyModel,
                            List<ServiceComponent> components) {
        this.serviceName = serviceName;
        this.isDeterministic = isDeterministic;
        this.stateDirectory = stateDirectory;
        this.consistencyModel = consistencyModel;
        this.components = components;
    }

    public ServiceComponent getEntryComponent() {
        if (entryComponent == null) {
            for (ServiceComponent c : components) {
                if (c.isEntryComponent()) {
                    entryComponent = c;
                    break;
                }
            }
        }
        return entryComponent;
    }

    public ServiceComponent getStatefulComponent() {
        if (statefulComponent == null) {
            for (ServiceComponent c : components) {
                if (c.isStateful()) {
                    statefulComponent = c;
                    break;
                }
            }
        }
        return statefulComponent;
    }

    public static ServiceProperty createFromJSONString(String jsonString) throws JSONException {
        JSONObject json = new JSONObject(jsonString);

        // parsing and validating service name
        String serviceName = json.getString("name");
        if (serviceName == null || serviceName.isEmpty()) {
            throw new RuntimeException("service name is required");
        }
        if (serviceName.length() > 256) {
            throw new RuntimeException("service name must be <= 256 characters");
        }

        // parsing is-deterministic
        boolean isDeterministic = false;
        if (json.has("deterministic")) {
            isDeterministic = json.getBoolean("deterministic");
        }

        // parsing and validating state directory
        String stateDirectory = json.getString("state");
        if (stateDirectory.isEmpty()) {
            stateDirectory = null;
        }
        if (stateDirectory != null) {
            validateStateDirectory(stateDirectory);
        }

        // parsing and validating consistency model, the default consistency
        // model is SEQUENTIAL_CONSISTENCY.
        ConsistencyModel consistencyModel;
        String consistencyModelString = json.getString("consistency");
        if (consistencyModelString == null) {
            consistencyModel = ConsistencyModel.SEQUENTIAL;
        } else {
            consistencyModel = parseConsistencyModel(consistencyModelString);
        }

        // parsing and validating service component(s)
        List<ServiceComponent> components = new ArrayList<>();
        if (json.has("image") && json.has("components")) {
            throw new RuntimeException("a service must either have a single component, " +
                    "declared with 'image', or have multiple components declared with 'components'");
        }
        // case-1: handle service with a single component
        if (json.has("image")) {
            String imageName = json.getString("image");
            if (imageName == null || imageName.isEmpty()) {
                throw new RuntimeException("docker image name is required");
            }

            // parse entry port with port 80 as the default
            int entryPort = json.getInt("port");
            if (entryPort == 0) {
                entryPort = 80;
            }

            // parse environment variables, if any
            Map<String, String> env = null;
            if (json.has("environments")) {
                JSONArray envJSON = json.getJSONArray("environments");
                env = parseEnvironmentVariables(envJSON);
            }
            if (json.has("env")) {
                JSONArray envJSON = json.getJSONArray("env");
                env = parseEnvironmentVariables(envJSON);
            }

            ServiceComponent c = new ServiceComponent(
                    serviceName,
                    imageName,
                    entryPort,
                    (stateDirectory != null),
                    true,
                    entryPort,
                    env
            );
            components.add(c);
        }
        // case-2: handle service with multiple components
        if (json.has("components")) {
            JSONArray componentsJSON = json.getJSONArray("components");
            components.addAll(parseServiceComponents(componentsJSON));
        }

        ServiceProperty prop = new ServiceProperty(
                serviceName,
                isDeterministic,
                stateDirectory,
                consistencyModel,
                components
        );

        // automatically infer is-stateful of component via the state directory
        if (stateDirectory != null && stateDirectory.split(":").length == 2) {
            String[] componentStateDir = stateDirectory.split(":");
            String statefulComponent = componentStateDir[0];
            ServiceComponent c = null;
            for (ServiceComponent sc : components)
                if (sc.getComponentName().equals(statefulComponent))
                    c = sc;
            if (c == null) {
                throw new RuntimeException("unknown service's component specified in the state dir");
            }
            c.setIsStateful(true);
        }

        // validation: the number of stateful and entry component
        int numStatefulComponent = 0;
        int numEntryComponent = 0;
        for (ServiceComponent c : components) {
            if (c.isStateful()) numStatefulComponent++;
            if (c.isEntryComponent()) numEntryComponent++;
        }
        if (numStatefulComponent > 1) {
            throw new RuntimeException("there is at most one stateful service's component");
        }
        if (numEntryComponent != 1) {
            throw new RuntimeException("there must be one entry component");
        }

        return prop;
    }

    private static void validateStateDirectory(String stateDirectory) {
        if (stateDirectory == null) {
            return;
        }

        String statePath = null;
        String[] componentAndStateDir = stateDirectory.split(":");
        if (componentAndStateDir.length > 2) {
            throw new RuntimeException("invalid format for state directory, " +
                    "expecting '<component>:<path>' or <path>.");
        }
        if (componentAndStateDir.length == 2) {
            statePath = componentAndStateDir[1];
        }
        if (componentAndStateDir.length == 1) {
            statePath = componentAndStateDir[0];
        }
        if (componentAndStateDir.length < 1) {
            throw new RuntimeException("invalid format for state directory");
        }

        if (statePath.isEmpty()) {
            throw new RuntimeException("empty path of state directory");
        }

        if (!statePath.startsWith("/")) {
            throw new RuntimeException("state directory must be in absolute path");
        }

        if (!stateDirectory.endsWith("/")) {
            throw new RuntimeException("state directory must be a directory, ending with '/'");
        }
    }

    private static ConsistencyModel parseConsistencyModel(String model) {
        if (model == null) {
            throw new RuntimeException("consistency model can not be null");
        }
        if (model.isEmpty()) {
            throw new RuntimeException("consistency model can not be empty");
        }

        // try to match the given consistency model with valid consistency model
        model = model.toUpperCase();
        for (ConsistencyModel cm : ConsistencyModel.values()) {
            if (cm.toString().equals(model)) {
                return cm;
            }
        }

        // invalid consistency model was given, prepare exception message
        StringBuilder b = new StringBuilder();
        int counter = 0;
        for (ConsistencyModel cm : ConsistencyModel.values()) {
            b.append(cm.toString());
            counter++;
            if (counter != ConsistencyModel.values().length) {
                b.append(", ");
            }
        }
        throw new RuntimeException("invalid consistency model, valid values are: " + b.toString());
    }

    private static Map<String, String> parseEnvironmentVariables(JSONArray envJSON) throws JSONException {
        Map<String, String> env = new HashMap<>();
        if (envJSON != null && envJSON.length() > 0) {
            for (int i = 0; i < envJSON.length(); i++) {
                JSONObject envItem = envJSON.getJSONObject(i);
                String envVarName = null;
                String envVarValue = null;

                Iterator keyIterator = envItem.keys();
                while (keyIterator.hasNext()) {
                    Object k = keyIterator.next();
                    envVarName = k.toString();
                }

                if (envVarName == null) {
                    continue;
                }

                envVarValue = envItem.getString(envVarName);
                env.put(envVarName, envVarValue);
            }
        }
        return env;
    }

    private static List<ServiceComponent> parseServiceComponents(JSONArray componentsJSON)
            throws JSONException {
        List<ServiceComponent> components = new ArrayList<>();
        int len = componentsJSON.length();
        for (int i = 0; i < len; i++) {
            JSONObject componentJSON = componentsJSON.getJSONObject(i);
            String componentName = null;

            Iterator it = componentJSON.keys();
            if (it.hasNext()) {
                componentName = it.next().toString();
            }
            JSONObject componentDetailJSON = componentJSON.getJSONObject(componentName);

            // parse image name
            String imageName = componentDetailJSON.getString("image");
            if (imageName == null || imageName.isEmpty()) {
                throw new RuntimeException("docker image name is required for service component '" +
                        componentName + "'");
            }

            // parse is-stateful
            boolean isStateful = false;
            if (componentDetailJSON.has("stateful")) {
                isStateful = componentDetailJSON.getBoolean("stateful");
            }

            // parse is-entry
            boolean isEntry = false;
            if (componentDetailJSON.has("entry")) {
                isEntry = componentDetailJSON.getBoolean("entry");
            }

            // parse exposed port
            int exposedPort = 0;
            if (componentDetailJSON.has("expose")) {
                exposedPort = componentDetailJSON.getInt("expose");
            }

            // parse entry port, note that ieEntry is set to true for component with
            // http port specified.
            int entryPort = 0;
            if (componentDetailJSON.has("port")) {
                entryPort = componentDetailJSON.getInt("port");
                isEntry = true;
            }

            // parse environments
            Map<String, String> env = null;
            if (componentDetailJSON.has("environments")) {
                JSONArray envJSON = componentDetailJSON.getJSONArray("environments");
                env = parseEnvironmentVariables(envJSON);
            }
            if (componentDetailJSON.has("env")) {
                JSONArray envJSON = componentDetailJSON.getJSONArray("env");
                env = parseEnvironmentVariables(envJSON);
            }

            components.add(new ServiceComponent(
                    componentName,
                    imageName,
                    exposedPort == 0 ? null : exposedPort,
                    isStateful,
                    isEntry,
                    entryPort == 0 ? null : entryPort,
                    env
            ));
        }

        return components;
    }

    public String getServiceName() {
        return serviceName;
    }

    public boolean isDeterministic() {
        return isDeterministic;
    }

    public String getStateDirectory() {
        return stateDirectory;
    }

    public ConsistencyModel getConsistencyModel() {
        return consistencyModel;
    }

    public List<ServiceComponent> getComponents() {
        return components;
    }

    public static class ServicePropertiesTest {
        @Test
        public void TEST_parseSingleComponentServiceProperties() {
            String serviceName = "alice-book-catalog";
            String prop = String.format("""
                    {
                      "name": "%s",
                      "image": "bookcatalog",
                      "port": 8000,
                      "state": "/data/",
                      "consistency": "linearizability",
                      "deterministic": true
                    }
                    """, serviceName);
            try {
                ServiceProperty sp = createFromJSONString(prop);
                assert Objects.equals(sp.serviceName, serviceName);
                assert Objects.equals(sp.components.size(), 1);
                assert Objects.equals(sp.consistencyModel, ConsistencyModel.LINEARIZABILITY);
                assert Objects.equals(sp.isDeterministic, true);

                ServiceComponent c = sp.components.getFirst();
                assert Objects.equals(c.getComponentName(), serviceName);
                assert Objects.equals(c.getExposedPort(), 8000);
                assert Objects.equals(c.getEntryPort(), 8000);
                assert Objects.equals(c.isStateful(), true);
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }

        @Test
        public void TEST_parseTwoComponentsServiceProperties() {
            String serviceName = "dave-note";
            String prop = String.format("""
                    {
                      "name": "%s",
                      "components": [
                        {
                          "backend": {
                            "image": "note-backend",
                            "expose": 8000,
                            "stateful": true
                          }
                        },
                        {
                          "frontend": {
                            "image": "note-frontend",
                            "port": 8080,
                            "entry": true,
                            "environments": [
                              {
                                "BACKEND_HOST": "localhost:8000"
                              }
                            ]
                          }
                        }
                      ],
                      "deterministic": false,
                      "state": "backend:/app/prisma/",
                      "consistency": "causal"
                    }
                    """, serviceName);
            try {
                ServiceProperty sp = createFromJSONString(prop);
                assert Objects.equals(sp.serviceName, serviceName);
                assert Objects.equals(sp.components.size(), 2);
                assert Objects.equals(sp.consistencyModel, ConsistencyModel.CAUSAL);
                assert Objects.equals(sp.isDeterministic, false);

                ServiceComponent c1 = sp.components.get(0);
                ServiceComponent c2 = sp.components.get(1);
                assert c1 != null;
                assert c2 != null;
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
