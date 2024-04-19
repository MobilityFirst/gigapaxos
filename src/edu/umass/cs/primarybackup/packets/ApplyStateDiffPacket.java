package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.PrimaryEpoch;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Random;

@RunWith(Enclosed.class)
public class ApplyStateDiffPacket extends PrimaryBackupPacket {

    public static final String SERIALIZED_PREFIX = "pb:sd:";

    private final String serviceName;
    private final PrimaryEpoch primaryEpoch;
    private final String stateDiff;
    private final long requestID;

    public ApplyStateDiffPacket(String serviceName, PrimaryEpoch primaryEpoch, String stateDiff) {
        assert serviceName != null;
        assert primaryEpoch != null;
        assert stateDiff != null;

        this.serviceName = serviceName;
        this.primaryEpoch = primaryEpoch;
        this.stateDiff = stateDiff;
        this.requestID = System.currentTimeMillis();
    }

    private ApplyStateDiffPacket(String serviceName, PrimaryEpoch primaryEpoch, String stateDiff,
                                 long requestID) {
        this.serviceName = serviceName;
        this.primaryEpoch = primaryEpoch;
        this.stateDiff = stateDiff;
        this.requestID = requestID;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_STATE_DIFF_PACKET;
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

    public PrimaryEpoch getPrimaryEpoch() {
        return primaryEpoch;
    }

    public String getStateDiff() {
        return stateDiff;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplyStateDiffPacket that = (ApplyStateDiffPacket) o;
        return requestID == that.requestID &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(primaryEpoch, that.primaryEpoch) &&
                Objects.equals(stateDiff, that.stateDiff);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, primaryEpoch, stateDiff, requestID);
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("sn", this.serviceName);
            json.put("ep", this.primaryEpoch.toString());
            json.put("sd", this.stateDiff);
            json.put("id", this.requestID);
            return String.format("%s%s", SERIALIZED_PREFIX, json.toString());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static ApplyStateDiffPacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("sn");
            String primaryEpochStr = json.getString("ep");
            String stateDiff = json.getString("sd");
            long requestID = json.getLong("id");

            return new ApplyStateDiffPacket(
                    serviceName,
                    new PrimaryEpoch(primaryEpochStr),
                    stateDiff,
                    requestID);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static class TestApplyStateDiffPacket {
        @Test
        public void TestApplyStateDiffPacketSerializationDeserialization() {
            byte[] stateDiff = new byte[10240];
            new Random().nextBytes(stateDiff);

            String serviceName = "dummyServiceName";
            PrimaryEpoch zero = new PrimaryEpoch("0:0");
            String stateDiffString = new String(stateDiff, StandardCharsets.ISO_8859_1);

            ApplyStateDiffPacket p1 = new ApplyStateDiffPacket(serviceName, zero, stateDiffString);
            ApplyStateDiffPacket p2 = ApplyStateDiffPacket.createFromString(p1.toString());
            
            assert p2.equals(p1);
        }
    }

}
