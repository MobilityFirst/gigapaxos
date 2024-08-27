package edu.umass.cs.cops.examples;

import edu.umass.cs.cops.packets.CopsPacket;
import edu.umass.cs.cops.packets.CopsPacketType;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class KeyValueApp implements Replicable, Reconfigurable {

    private String myID;
    private Map<String, String> datastore = new HashMap<>();
    private Map<String, AtomicLong> datastoreVersioning = new HashMap<>();

    public KeyValueApp(String[] args) {
        this.myID = args[args.length-1];
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        // FIXME: quick hack because we dont differentiate coordination packet and
        //  app request packet

        System.out.println(">>> getRequest() " + stringified);

        if (stringified != null && stringified.startsWith(KeyValueAppRequest.SERIALIZED_PREFIX)) {
            System.out.println(">>> createAppRequest");
            return KeyValueAppRequest.createFromString(stringified);
        }

        return CopsPacket.createFromString(stringified);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> types = new HashSet<>();
        types.add(KeyValueAppRequest.KeyValueAppRequestType);
        // FIXME: separate coordinator and app request
        // types.add(CopsPacketType.COPS_PACKET);
        return types;
    }

    @Override
    public boolean execute(Request request) {
        return execute(request, false);
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        if (!(request instanceof KeyValueAppRequest r)) {
            System.out.printf("Error, unknown request of class %s\n",
                    request.getClass().getSimpleName());
            return true;
        }

        if (!datastoreVersioning.containsKey(r.getKey())) {
            datastoreVersioning.put(r.getKey(), new AtomicLong());
        }

        // handle GET request
        if (r.getValue() == null || r.getValue().isEmpty()) {
            String storedValue = datastore.get(r.getKey());
            if (storedValue == null) storedValue = "";
            r.setResponseValue(storedValue);
            long currentVersion = datastoreVersioning.get(r.getKey()).get();
            r.responseVersion = currentVersion;
            r.dependencies.add(String.format("%s:%s:%d", r.getKey(), myID, currentVersion));
        }

        // handle PUT request
        // TODO use synchronized or lock for this critical section
        if (r.getValue() != null && !r.getValue().isEmpty()) {
            datastore.put(r.getKey(), r.getValue());
            r.setResponseValue("success");

            // update the versioning
            datastoreVersioning.put(r.getKey(), new AtomicLong(
                            datastoreVersioning.get(r.getKey()).incrementAndGet()));
            long currentVersion = datastoreVersioning.get(r.getKey()).get();
            r.responseVersion = currentVersion;
            r.dependencies.add(String.format("%s:%s:%d", r.getKey(), myID, currentVersion));
        }

        return true;
    }

    public void handlePutAfter() {
        // TODO: implement me
    }

    @Override
    public String checkpoint(String name) {
        StringBuilder stateSnapshot = new StringBuilder();
        for (Map.Entry<String, String> entry : this.datastore.entrySet()) {
            stateSnapshot.append(String.format("%s:%s,", entry.getKey(), entry.getValue()));
        }
        return stateSnapshot.toString();
    }

    @Override
    public boolean restore(String name, String state) {
        if (state == null || state.isEmpty()) {
            this.datastore.clear();
            return true;
        }

        this.datastore.clear();
        String[] keyValuePairs = state.split(",");
        for (String keyValuePair : keyValuePairs) {
            String[] keyAndValue = keyValuePair.split(":");
            assert keyAndValue.length == 2 : "KeyValueApp: Invalid app snapshot";
            String key = keyAndValue[0];
            String value = keyAndValue[1];
            this.datastore.put(key, value);
        }
        return true;
    }

    private record KeyValueAppStopRequest(String serviceName, int placementEpoch)
            implements ReconfigurableRequest {

        @Override
        public IntegerPacketType getRequestType() {
            return ReconfigurableRequest.STOP;
        }

        @Override
        public String getServiceName() {
            return this.serviceName;
        }

        @Override
        public int getEpochNumber() {
            return this.placementEpoch;
        }

        @Override
        public boolean isStop() {
            return true;
        }
    }

    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        return new KeyValueAppStopRequest(name, epoch);
    }

    @Override
    public String getFinalState(String name, int epoch) {
        return null;
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {

    }

    @Override
    public boolean deleteFinalState(String name, int epoch) {
        return false;
    }

    @Override
    public Integer getEpoch(String name) {
        return 0;
    }
}
