package edu.umass.cs.xdn.request;

import edu.umass.cs.primarybackup.PrimaryEpoch;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class TestXDNStatediffApplyRequest {
    @Test
    public void TestXDNStatediffApplyRequestSerializationDeserialization() {
        byte[] statediff = new byte[10240];
        new Random().nextBytes(statediff);

        String serviceName = "dummyServiceName";
        PrimaryEpoch zero = new PrimaryEpoch("0:0");
        String statediffString = new String(statediff, StandardCharsets.ISO_8859_1);
        XDNStatediffApplyRequest request = new XDNStatediffApplyRequest(
                serviceName,
                zero,
                statediffString);

        String serialized = request.toString();
        XDNStatediffApplyRequest deserializedRequest = XDNStatediffApplyRequest.
                createFromString(serialized);

        System.out.println(request);
        System.out.println(deserializedRequest);
        assert deserializedRequest != null : "deserialized XDNStatediffApplyRequest is null";

        assert request.equals(deserializedRequest);
        assert serialized.equals(deserializedRequest.toString());
    }
}
