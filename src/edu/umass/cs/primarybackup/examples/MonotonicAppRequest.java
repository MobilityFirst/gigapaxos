package edu.umass.cs.primarybackup.examples;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Objects;

@RunWith(Enclosed.class)
public class MonotonicAppRequest implements ReplicableRequest, ClientRequest {

    public static final String MONOTONIC_APP_GEN_NUMBER_COMMAND = "genNumber";
    public static final String MONOTONIC_APP_GET_SEQUENCE_COMMAND = "getSequence";
    public static final String MONOTONIC_APP_EXIT_COMMAND = "exit";

    private static final int MONOTONIC_APP_REQUEST_TYPE = 8888;
    public static final String SERIALIZED_PREFIX = "app:";
    public static final IntegerPacketType MonotonicAppRequestType =
            () -> MONOTONIC_APP_REQUEST_TYPE;

    private final String serviceName;
    private final String command;
    private final long requestID;

    private String response;

    public MonotonicAppRequest(String serviceName, String command) {
        assert MONOTONIC_APP_GEN_NUMBER_COMMAND.equals(command) ||
                MONOTONIC_APP_GET_SEQUENCE_COMMAND.equals(command);
        assert serviceName != null && !serviceName.isEmpty();

        this.serviceName = serviceName;
        this.command = command;
        this.requestID = System.currentTimeMillis();
    }

    private MonotonicAppRequest(String serviceName, String command, long requestID) {
        assert MONOTONIC_APP_GEN_NUMBER_COMMAND.equals(command) ||
                MONOTONIC_APP_GET_SEQUENCE_COMMAND.equals(command);
        assert serviceName != null && !serviceName.isEmpty();

        this.serviceName = serviceName;
        this.command = command;
        this.requestID = requestID;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return MonotonicAppRequestType;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    public String getCommand() {
        return command;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public ClientRequest getResponse() {
        return this;
    }

    public String getResponseValue() {
        return this.response;
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
        MonotonicAppRequest that = (MonotonicAppRequest) o;
        return requestID == that.requestID &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(command, that.command);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, command, requestID);
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("serviceName", this.serviceName);
            json.put("command", this.command);
            json.put("id", this.requestID);
            json.put("response", this.response == null ? "" : this.response);
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static MonotonicAppRequest createFromString(String encodedRequest) {
        assert encodedRequest != null && !encodedRequest.isEmpty();
        assert encodedRequest.startsWith(SERIALIZED_PREFIX);

        encodedRequest = encodedRequest.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedRequest);
            String serviceName = json.getString("serviceName");
            String command = json.getString("command");
            long requestID = json.getLong("id");
            String response = json.getString("response");

            MonotonicAppRequest r = new MonotonicAppRequest(serviceName, command, requestID);
            if (response != null && !response.isEmpty()) {
                r.response = response;
            }
            return r;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TestMonotonicAppRequest {
        @Test
        public void TestMonotonicAppRequestSerializationDeserialization() {
            String serviceName = "dummy-service-name";
            String command = "genNumber";
            MonotonicAppRequest r1 = new MonotonicAppRequest(serviceName, command);
            MonotonicAppRequest r2 = MonotonicAppRequest.createFromString(r1.toString());

            assert Objects.equals(r1, r2);

            serviceName = "dummy-service-name-2";
            command = "getSequence";
            MonotonicAppRequest r3 = new MonotonicAppRequest(serviceName, command);
            MonotonicAppRequest r4 = MonotonicAppRequest.createFromString(r3.toString());

            assert Objects.equals(r3, r4);
        }

        @Test
        public void TestMonotonicAppRequestWithResponse() {
            String serviceName = "dummy-service-name";
            String command = "genNumber";
            String response = "40";

            MonotonicAppRequest r1 = new MonotonicAppRequest(serviceName, command);
            r1.setResponse(response);
            MonotonicAppRequest r2 = MonotonicAppRequest.createFromString(r1.toString());

            assert r2.getResponse().equals(response);
        }
    }

}
