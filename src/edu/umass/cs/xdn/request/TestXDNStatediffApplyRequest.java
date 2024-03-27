package edu.umass.cs.xdn.request;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class TestXDNStatediffApplyRequest {
    @Test
    public void TestXDNStatediffApplyRequestSerializationDeserialization() {
        byte[] statediff = new byte[10240];
        new Random().nextBytes(statediff);

        String serviceName = "dummyServiceName";
        String statediffString = new String(statediff, StandardCharsets.ISO_8859_1);
        XDNStatediffApplyRequest request = new XDNStatediffApplyRequest(
                serviceName,
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
