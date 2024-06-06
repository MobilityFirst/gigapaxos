package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.PrimaryEpoch;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Objects;

@RunWith(Enclosed.class)
public class StartEpochPacket extends PrimaryBackupPacket {

    public static final String SERIALIZED_PREFIX = "pb:se:";
    private final String serviceName;
    private final PrimaryEpoch<?> startingEpoch;

    public StartEpochPacket(String serviceName, PrimaryEpoch<?> epoch) {
        this.serviceName = serviceName;
        this.startingEpoch = epoch;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_START_EPOCH_PACKET;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    public String getStartingEpochString() {
        return startingEpoch.toString();
    }

    @Override
    public long getRequestID() {
        return 0;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StartEpochPacket that = (StartEpochPacket) o;
        return Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(startingEpoch.counter, that.startingEpoch.counter) &&
                Objects.equals(startingEpoch.nodeID, that.startingEpoch.nodeID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, startingEpoch.counter, startingEpoch.nodeID);
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("serviceName", this.serviceName);
            json.put("epochCounter", this.startingEpoch.counter);
            json.put("epochNode", this.startingEpoch.nodeID);
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static StartEpochPacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("serviceName");
            String epochNode = json.getString("epochNode");
            int epochCounter = json.getInt("epochCounter");

            return new StartEpochPacket(serviceName, new PrimaryEpoch(epochNode, epochCounter));
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TestStartEpochPacket {
        @Test
        public void TestStartEpochPacketSerializationDeserialization() {
            PrimaryEpoch e = new PrimaryEpoch("ar0", 0);
            StartEpochPacket p1 = new StartEpochPacket("dummy-service-name", e);
            StartEpochPacket p2 = createFromString(p1.toString());

            assert p2 != null;
            assert Objects.equals(p1, p2);
        }
    }

}
