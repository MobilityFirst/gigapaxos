package edu.umass.cs.gigapaxos.examples.xdn;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class XDNRequest implements ReplicableRequest {

    public enum PacketType implements IntegerPacketType {
        XDN_SERVICE_HTTP_REQUEST(31300);

        private static HashMap<Integer, XDNRequest.PacketType> numbers = new HashMap<>();

        /* *****BEGIN static code block to ensure correct initialization *********** */
        static {
            for (XDNRequest.PacketType type : XDNRequest.PacketType.values()) {
                if (!XDNRequest.PacketType.numbers.containsKey(type.number)) {
                    XDNRequest.PacketType.numbers.put(type.number, type);
                } else {
                    assert (false) : "Duplicate or inconsistent enum type";
                    throw new RuntimeException(
                            "Duplicate or inconsistent enum type");
                }
            }
        }
        /*  *************** END static code block to ensure correct initialization *********** */

        private final int number;

        PacketType(int t) {
            this.number = t;
        }

        @Override
        public int getInt() {
            return 313000;
        }
    }

    private HttpRequest httpRequest;
    private HttpContent httpContent;
    private HttpResponse httpResponse;
    private byte[] statediff;

    // SERIALIZED_PREFIX is used as prefix of the serialized (string) version of
    // XDNRequest, otherwise Gigapaxos will detect it as JSONPacket and handle it
    // incorrectly.
    private static final String SERIALIZED_PREFIX = "xdn:";
    private final long requestID;

    public XDNRequest() {
        this.requestID = System.currentTimeMillis();
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PacketType.XDN_SERVICE_HTTP_REQUEST;
    }

    // The service's name is embedded in the request header.
    // For example, the service name is 'hello' for these cases:
    // - request with "XDN: hello" in the header.
    // - request with "Host: hello.abc.xdn.io:80" in the header.
    @Override
    public String getServiceName() {
        if (httpRequest == null) {
            return null;
        }

        // case-1: embedded in the XDN header
        String xdnHeader = httpRequest.headers().get("XDN");
        if (xdnHeader != null && xdnHeader.length() > 0) {
            return xdnHeader;
        }

        // case-1: embedded in the required Host header
        String hostStr = httpRequest.headers().get(HttpHeaderNames.HOST);
        if (hostStr == null || hostStr.length() == 0) {
            return null;
        }
        String serviceName = hostStr.split("\\.")[0];
        if (serviceName.length() > 0) {
            return serviceName;
        }

        return null;
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public void setHttpRequest(HttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    public HttpContent getHttpContent() {
        return httpContent;
    }

    public void setHttpContent(HttpContent httpContent) {
        this.httpContent = httpContent;
    }

    @Override
    public long getRequestID() {
        // TODO: specify header field to be used as requestID
        return requestID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public String toString() {
        if (httpRequest == null) {
            return null;
        }
        try {
            JSONObject json = new JSONObject();
            json.put("protocolVersion", httpRequest.protocolVersion().toString());
            json.put("method", httpRequest.method().toString());
            json.put("uri", httpRequest.uri());
            JSONArray headerJsonArray = new JSONArray();
            Iterator<Map.Entry<String, String>> it = httpRequest.headers().iteratorAsString();
            while (it.hasNext()) {
                Map.Entry<String, String> entry = it.next();
                headerJsonArray.put(String.format("%s:%s", entry.getKey(), entry.getValue()));
            }
            json.put("headers", headerJsonArray);
            if (httpContent != null) {
                json.put("content", httpContent.content().toString(StandardCharsets.UTF_8));
            } else {
                json.put("content", "");
            }
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static XDNRequest createFromString(String stringified) {
        if (stringified == null || !stringified.startsWith(SERIALIZED_PREFIX)) {
            return null;
        }
        stringified = stringified.substring(SERIALIZED_PREFIX.length());
        try {
            JSONObject json = new JSONObject(stringified);

            // prepare the deserialized variables
            String httpProtocolVersion = json.getString("protocolVersion");
            String httpMethod = json.getString("method");
            String httpURI = json.getString("uri");
            String httpContent = json.getString("content");

            // handle array of header
            JSONArray headerJSONArr = json.getJSONArray("headers");
            HttpHeaders httpHeaders = new DefaultHttpHeaders();
            for (int i = 0; i < headerJSONArr.length(); i++) {
                String headerEntry = headerJSONArr.getString(i);
                String headerKey = headerEntry.split(":")[0];
                String headerVal = headerEntry.substring(headerKey.length() + 1);
                httpHeaders.add(headerKey, headerVal);
            }

            // init request and content, then combine them into XDNRequest
            HttpRequest req = new DefaultHttpRequest(
                    HttpVersion.valueOf(httpProtocolVersion),
                    HttpMethod.valueOf(httpMethod),
                    httpURI,
                    httpHeaders);
            HttpContent reqContent = new DefaultHttpContent(
                    Unpooled.copiedBuffer(httpContent, StandardCharsets.UTF_8));
            XDNRequest deserializedRequest = new XDNRequest();
            deserializedRequest.setHttpRequest(req);
            deserializedRequest.setHttpContent(reqContent);

            return deserializedRequest;
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void setHttpResponse(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XDNRequest that = (XDNRequest) o;
        return httpRequest.equals(that.httpRequest) && httpContent.equals(that.httpContent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(httpRequest, httpContent);
    }

    @Test
    public void testXDNRequestSerializationDeserialization() {
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
                        .add("Random-Char", "=,;:\"'`")
                        .add("Content-Type", "multipart/mixed; boundary=gc0p4Jq0M2Yt08j34"));
        HttpContent dummyHttpContent = new DefaultHttpContent(
                Unpooled.copiedBuffer("somestringcontent".getBytes(StandardCharsets.UTF_8)));

        XDNRequest dummyXDNRequest = new XDNRequest();
        dummyXDNRequest.setHttpRequest(dummyHttpRequest);
        dummyXDNRequest.setHttpContent(dummyHttpContent);

        // the created XDNRequest from string should equal to the
        // original XDNRequest being used to generate the string.
        XDNRequest deserializedXDNRequest = XDNRequest.createFromString(dummyXDNRequest.toString());
        System.out.println(dummyXDNRequest);
        System.out.println(deserializedXDNRequest);
        assert deserializedXDNRequest != null : "deserialized XDNRequest is null";
        assert Objects.equals(
                dummyXDNRequest,
                deserializedXDNRequest) : "deserialized XDNRequest is different";
    }

}
