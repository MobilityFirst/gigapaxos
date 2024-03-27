package edu.umass.cs.xdn.request;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class TestXDNHttpRequest {
    @Test
    public void TestXDNRequestSerializationDeserialization() {
        XDNHttpRequest dummyXDNHttpRequest = createDummyRequest();

        // the created XDNRequest from string should equal to the
        // original XDNRequest being used to generate the string.
        XDNHttpRequest deserializedXDNRequest = XDNHttpRequest.createFromString(
                dummyXDNHttpRequest.toString());
        System.out.println(dummyXDNHttpRequest);
        System.out.println(deserializedXDNRequest);
        assert deserializedXDNRequest != null : "deserialized XDNRequest is null";
        assert Objects.equals(
                dummyXDNHttpRequest,
                deserializedXDNRequest) : "deserialized XDNRequest is different";
    }

    public static XDNHttpRequest createDummyRequest() {
        String serviceName = "dummyServiceName";
        HttpRequest dummyHttpRequest = new DefaultHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.POST,
                "/?name=alice-book-catalog&qval=qwerty",
                new DefaultHttpHeaders()
                        .add("header-1", "value-1")
                        .add("header-1", "value-2")
                        .add("header-1", "value-3")
                        .add("header-a", "value-a")
                        .add("header-b", "value-b")
                        .add("Random-1", "a,b,c")
                        .add("Random-2", "a:b:c")
                        .add("XDN", serviceName)
                        .add("Random-Char", "=,;:\"'`")
                        .add("Content-Type", "multipart/mixed; boundary=gc0p4Jq0MYt08"));
        HttpContent dummyHttpContent = new DefaultHttpContent(
                Unpooled.copiedBuffer("somestringcontent".getBytes(StandardCharsets.UTF_8)));

        return new XDNHttpRequest(
                serviceName, dummyHttpRequest, dummyHttpContent);
    }
}
