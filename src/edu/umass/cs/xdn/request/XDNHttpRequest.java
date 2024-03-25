package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class XDNHttpRequest extends XDNRequest {

    /**
     * All the serialized XDNHttpRequest starts with "xdn:31300:"
     */
    public static final String SERIALIZED_PREFIX = String.format("%s%d:",
            XDNRequest.SERIALIZED_PREFIX, XDNRequestType.XDN_SERVICE_HTTP_REQUEST.getInt());

    private final HttpRequest httpRequest;
    private final HttpContent httpRequestContent;
    private final String serviceName;
    private HttpResponse httpResponse;

    public XDNHttpRequest(String serviceName, HttpRequest httpRequest, HttpContent httpRequestContent) {
        assert serviceName != null && httpRequest != null && httpRequestContent != null;
        this.serviceName = serviceName;
        this.httpRequest = httpRequest;
        this.httpRequestContent = httpRequestContent;

        // TODO: specify header field to be used as requestID
    }

    // The service's name is embedded in the request header.
    // For example, the service name is 'hello' for these cases:
    // - request with "XDN: hello" in the header.
    // - request with "Host: hello.abc.xdn.io:80" in the header.
    // return an empty string if service name cannot be inferred
    public static String inferServiceName(HttpRequest httpRequest) {
        assert httpRequest != null;

        // case-1: embedded in the XDN header (e.g., XDN: alice-book-catalog)
        String xdnHeader = httpRequest.headers().get("XDN");
        if (xdnHeader != null && xdnHeader.length() > 0) {
            return xdnHeader;
        }

        // case-2: embedded in the required Host header (e.g., Host: alice-book-catalog.xdn.io)
        String hostStr = httpRequest.headers().get(HttpHeaderNames.HOST);
        if (hostStr == null || hostStr.length() == 0) {
            return null;
        }
        String reqServiceName = hostStr.split("\\.")[0];
        if (reqServiceName.length() > 0) {
            return reqServiceName;
        }

        return null;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return XDNRequestType.XDN_SERVICE_HTTP_REQUEST;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
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
        XDNHttpRequest that = (XDNHttpRequest) o;
        return this.serviceName.equals(that.serviceName) &&
                this.httpRequest.equals(that.httpRequest) &&
                this.httpRequestContent.equals(that.httpRequestContent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.serviceName, this.httpRequest, this.httpRequestContent);
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public HttpContent getHttpRequestContent() {
        return httpRequestContent;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }

    public void setHttpResponse(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

    @Override
    public String toString() {
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
            if (httpRequestContent != null) {
                json.put("content", httpRequestContent.content().toString(StandardCharsets.UTF_8));
            } else {
                json.put("content", "");
            }
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static XDNHttpRequest createFromString(String stringified) {
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
            String serviceName = inferServiceName(req);
            XDNHttpRequest deserializedRequest = new XDNHttpRequest(serviceName, req, reqContent);
            return deserializedRequest;
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
    }


    public static class TestXDNHttpRequest {
        @Test
        public void TestXDNRequestSerializationDeserialization() {
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

            String serviceName = "dummyServiceName";
            XDNHttpRequest dummyXDNHttpRequest = new XDNHttpRequest(serviceName, dummyHttpRequest, dummyHttpContent);

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
    }

}
