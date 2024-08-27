package edu.umass.cs.cops.examples;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import edu.umass.cs.xdn.interfaces.behavior.RequestBehaviorType;
import edu.umass.cs.xdn.interfaces.behavior.WriteOnlyRequest;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class KeyValueAppRequest implements ReplicableRequest, ClientRequest, BehavioralRequest {

    public static final String SERIALIZED_PREFIX = "app:";
    private static final int KEY_VALUE_APP_REQUEST_TYPE = 8888;
    public static final IntegerPacketType KeyValueAppRequestType = () -> KEY_VALUE_APP_REQUEST_TYPE;

    private final String serviceName;
    private final long requestID;

    private final String key;
    private final String value;

    private final long clientID;
    private final long version;
    public static final long LATEST_VERSION = -1;

    private String response;
    public long responseVersion;
    public List<String> dependencies = new ArrayList<>();

    public KeyValueAppRequest(String serviceName, long clientID, long requestID, String key,
                              String value, long version) {
        assert serviceName != null && !serviceName.isEmpty() :
                "Service name must not be null, nor empty";
        this.serviceName = serviceName;
        this.clientID = clientID;
        this.requestID = requestID;
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public KeyValueAppRequest(String serviceName, String key, String value) {
        this(serviceName,
                /*clientID=*/new Random().nextLong(),
                /*requestID=*/ System.currentTimeMillis(),
                key, value,
                LATEST_VERSION);
    }

    public KeyValueAppRequest(String serviceName, long clientID, long requestID, String key, String value) {
        this(serviceName,
                clientID,
                requestID,
                key, value,
                LATEST_VERSION);
    }

    public KeyValueAppRequest(String serviceName, long clientID, String key, String value,
                              long version) {
        this(serviceName,
                clientID,
                /*requestID=*/ System.currentTimeMillis(),
                key, value,
                version);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getClientID() {
        return clientID;
    }

    public long getVersion() {
        return version;
    }

    public boolean isWriteRequest() {
        return this.value != null && !this.value.isEmpty();
    }

    @Override
    public ClientRequest getResponse() {
        return this;
    }

    public void setResponseValue(String response) {
        this.response = response;
    }

    public String getResponseValue() {
        return this.response;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return KeyValueAppRequestType;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.requestID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValueAppRequest that = (KeyValueAppRequest) o;
        return requestID == that.requestID &&
                clientID == that.clientID &&
                version == that.version &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, requestID, key, value);
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("serviceName", this.serviceName);
            json.put("id", this.requestID);
            json.put("cid", this.clientID);
            json.put("key", this.key);
            json.put("value", this.value == null ? "" : this.value);
            json.put("version", this.version);
            json.put("response", this.response == null ? "" : this.response);
            json.put("responseVersion", this.responseVersion);
            json.put("dependencies", this.dependencies);
            json.put("write_only",
                    (this instanceof BehavioralRequest br && br.isWriteOnlyRequest()));
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static KeyValueAppRequest createFromString(String encodedRequest) {
        assert encodedRequest != null && !encodedRequest.isEmpty();
        assert encodedRequest.startsWith(SERIALIZED_PREFIX);

        encodedRequest = encodedRequest.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedRequest);
            String serviceName = json.getString("serviceName");
            long requestID = json.getLong("id");
            long clientID = json.getLong("cid");
            String key = json.getString("key");
            String value = json.getString("value");
            long version = json.getLong("version");
            String response = json.getString("response");
            long responseVersion = json.getLong("responseVersion");
            JSONArray arr = json.getJSONArray("dependencies");
            List<String> deps = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                deps.add(arr.getString(i));
            }
            boolean isWriteOnly = json.getBoolean("write_only");

            KeyValueAppRequest r;
            if (isWriteOnly) {
                r = new KeyValueAppWriteRequest(serviceName, clientID, requestID, key, value);
            } else {
                r = new KeyValueAppRequest(serviceName, clientID, requestID, key, value, version);
            }
            if (response != null && !response.isEmpty()) {
                r.response = response;
            }
            r.responseVersion = responseVersion;
            r.dependencies = deps;
            return r;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<RequestBehaviorType> getBehaviors() {
        return new HashSet<>(List.of(RequestBehaviorType.READ_ONLY));
    }
}
