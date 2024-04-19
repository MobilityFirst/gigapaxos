package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Objects;

@RunWith(Enclosed.class)
public class ChangePrimaryPacket extends PrimaryBackupPacket {

    public static final String SERIALIZED_PREFIX = "pb:cp:";
    private final String serviceName;
    private final String nodeID;
    private final long packetID;

    public ChangePrimaryPacket(String serviceName, String nodeID) {
        this.serviceName = serviceName;
        this.nodeID = nodeID;
        this.packetID = System.currentTimeMillis();
    }

    private ChangePrimaryPacket(String serviceName, String nodeID, long packetID) {
        this.serviceName = serviceName;
        this.nodeID = nodeID;
        this.packetID = packetID;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_CHANGE_PRIMARY_PACKET;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    public String getNodeID() {
        return nodeID;
    }

    @Override
    public long getRequestID() {
        return packetID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChangePrimaryPacket that = (ChangePrimaryPacket) o;
        return Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(nodeID, that.nodeID) &&
                Objects.equals(packetID, that.packetID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, nodeID, packetID);
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("serviceName", this.serviceName);
            json.put("nodeID", this.nodeID);
            json.put("id", this.packetID);
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static ChangePrimaryPacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("serviceName");
            String nodeID = json.getString("nodeID");
            long packetID = json.getLong("id");

            return new ChangePrimaryPacket(serviceName, nodeID, packetID);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TestChangePrimaryPacket {
        @Test
        public void TestChangePrimaryPacketSerializationDeserialization() {
            String serviceName = "dummy-service-name";
            String nodeID = "AR0";
            ChangePrimaryPacket p1 = new ChangePrimaryPacket(serviceName, nodeID);
            ChangePrimaryPacket p2 = createFromString(p1.toString());

            assert p2 != null;
            assert Objects.equals(p1, p2);
        }
    }

}
