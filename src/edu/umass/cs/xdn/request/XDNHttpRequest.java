package edu.umass.cs.xdn.request;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

@RunWith(Enclosed.class)
public class XDNHttpRequest extends XDNRequest implements ClientRequest {

    /**
     * All the serialized XDNHttpRequest starts with "xdn:31300:"
     */
    public static final String SERIALIZED_PREFIX = String.format("%s%d:",
            XDNRequest.SERIALIZED_PREFIX, XDNRequestType.XDN_SERVICE_HTTP_REQUEST.getInt());

    private static final String XDN_HTTP_REQUEST_ID_HEADER = "XDN-Request-ID";

    private final long requestID;
    private final String serviceName;
    private final HttpRequest httpRequest;
    private final HttpContent httpRequestContent;
    private HttpResponse httpResponse;

    public XDNHttpRequest(String serviceName, HttpRequest httpRequest, HttpContent httpRequestContent) {
        assert serviceName != null && httpRequest != null && httpRequestContent != null;
        this.serviceName = serviceName;
        this.httpRequest = httpRequest;
        this.httpRequestContent = httpRequestContent;

        Long inferredRequestID = XDNHttpRequest.inferRequestID(this.httpRequest);
        if (inferredRequestID != null) {
            this.requestID = inferredRequestID;
        } else {
            this.requestID = System.currentTimeMillis();
        }
        this.httpRequest.headers().set(XDN_HTTP_REQUEST_ID_HEADER, this.requestID);
    }

    // The service's name is encoded in the request header.
    // For example, the service name is 'hello' for these cases:
    // - request with "XDN: hello" in the header.
    // - request with "Host: hello.xdnapp.com:80" in the header.
    // return null if service's name cannot be inferred
    public static String inferServiceName(HttpRequest httpRequest) {
        assert httpRequest != null;

        // case-1: encoded in the XDN header (e.g., XDN: alice-book-catalog)
        String xdnHeader = httpRequest.headers().get("XDN");
        if (xdnHeader != null && !xdnHeader.isEmpty()) {
            return xdnHeader;
        }

        // case-2: encoded in the required Host header (e.g., Host: alice-book-catalog.xdnapp.com)
        String hostStr = httpRequest.headers().get(HttpHeaderNames.HOST);
        if (hostStr == null || hostStr.isEmpty()) {
            return null;
        }
        String reqServiceName = hostStr.split("\\.")[0];
        if (!reqServiceName.isEmpty()) {
            return reqServiceName;
        }

        return null;
    }

    // In general, we infer the HTTP request ID based on these headers:
    // (1) `ETag`, (2) `X-Request-ID`, or (3) `XDN-Request-ID`, in that order.
    // If the request does not contain those header, null will be returned.
    // Later, the newly generated request ID should be stored in the header
    // with `XDN-Request-ID` key (check the constructor of XDNHttpRequest).
    public static Long inferRequestID(HttpRequest httpRequest) {
        assert httpRequest != null;

        // case-1: encoded as Etag header
        String etag = httpRequest.headers().get(HttpHeaderNames.ETAG);
        if (etag != null) {
            return (long) Objects.hashCode(etag);
        }

        // case-2: encoded as X-Request-ID header
        String xRequestID = httpRequest.headers().get("X-Request-ID");
        if (xRequestID != null) {
            return (long) Objects.hashCode(xRequestID);
        }

        // case-3: encoded as XDN-Request-ID header
        String xdnReqID = httpRequest.headers().get("XDN-Request-ID");
        if (xdnReqID != null) {
            return Long.valueOf(xdnReqID);
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
        return requestID;
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
            json.put("content", httpRequestContent.content().toString(StandardCharsets.ISO_8859_1));

            if (httpResponse != null) {
                json.put("response", serializedHttpResponse(httpResponse));
            }

            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    private String serializedHttpResponse(HttpResponse response) {
        assert response != null;
        JSONObject json = new JSONObject();

        try {
            // serialize version
            json.put("version", response.protocolVersion().toString());

            // serialize status
            json.put("status", response.status().code());

            // serialize response header
            JSONArray headerJsonArray = new JSONArray();
            Iterator<Map.Entry<String, String>> it = response.headers().iteratorAsString();
            while (it.hasNext()) {
                Map.Entry<String, String> entry = it.next();
                headerJsonArray.put(String.format("%s:%s", entry.getKey(), entry.getValue()));
            }
            json.put("headers", headerJsonArray);

            // serialize response body
            assert response instanceof DefaultFullHttpResponse;
            DefaultFullHttpResponse fullHttpResponse = (DefaultFullHttpResponse) response;
            json.put("body", fullHttpResponse.content().toString(StandardCharsets.ISO_8859_1));

        } catch (JSONException e) {
            throw new RuntimeException(e);
        }

        return json.toString();
    }

    private static HttpResponse deserializedHttpResponse(String response) {
        assert response != null;

        try {
            JSONObject json = new JSONObject(response);
            String versionString = json.getString("version");
            int statusCode = json.getInt("status");
            JSONArray headerArray = json.getJSONArray("headers");
            String bodyString = json.getString("body");

            // copy http response headers, if any
            HttpHeaders headers = new DefaultHttpHeaders();
            for (int i = 0; i < headerArray.length(); i++) {
                String[] raw = headerArray.getString(i).split(":");
                headers.add(raw[0], raw[1]);
            }

            // by default, we have an empty header trailing for the response
            HttpHeaders trailingHeaders = new DefaultHttpHeaders();

            return new DefaultFullHttpResponse(
                    HttpVersion.valueOf(versionString),
                    HttpResponseStatus.valueOf(statusCode),
                    Unpooled.copiedBuffer(bodyString.getBytes(StandardCharsets.ISO_8859_1)),
                    headers,
                    trailingHeaders
            );
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
            String httpResponse = json.has("response") ?
                    json.getString("response") : null;

            // handle array of header
            JSONArray headerJSONArr = json.getJSONArray("headers");
            HttpHeaders httpHeaders = new DefaultHttpHeaders();
            for (int i = 0; i < headerJSONArr.length(); i++) {
                String headerEntry = headerJSONArr.getString(i);
                String headerKey = headerEntry.split(":")[0];
                String headerVal = headerEntry.substring(headerKey.length() + 1);
                httpHeaders.add(headerKey, headerVal);
            }

            // init request and content, then combine them into XDNHttpRequest
            HttpRequest req = new DefaultHttpRequest(
                    HttpVersion.valueOf(httpProtocolVersion),
                    HttpMethod.valueOf(httpMethod),
                    httpURI,
                    httpHeaders);
            HttpContent reqContent = new DefaultHttpContent(
                    Unpooled.copiedBuffer(httpContent, StandardCharsets.ISO_8859_1));
            String serviceName = XDNHttpRequest.inferServiceName(req);
            XDNHttpRequest xdnHttpRequest = new XDNHttpRequest(serviceName, req, reqContent);

            // handle response, if any
            if (httpResponse != null) {
                xdnHttpRequest.httpResponse = deserializedHttpResponse(httpResponse);
            }

            return xdnHttpRequest;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClientRequest getResponse() {
        return this;
    }

    public static class TestXdnHttpRequest {
        @Test
        public void TestXdnHttpRequestSerializationDeserialization() {
            XDNHttpRequest dummyXDNHttpRequest = createDummyTestRequest();

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

        @Test
        public void TestXdnHttpRequestSetResponse() {
            XDNHttpRequest dummyXDNHttpRequest = createDummyTestRequest();
            dummyXDNHttpRequest.setHttpResponse(createDummyTestResponse());

            Request requestWithResponse = dummyXDNHttpRequest.getResponse();
            assert requestWithResponse.equals(dummyXDNHttpRequest) :
                    "response must be embedded into the request";
        }

        public static XDNHttpRequest createDummyTestRequest() {
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

        private HttpResponse createDummyTestResponse() {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            sw.write("http request is successfully executed\n");
            return new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(sw.toString().getBytes()));
        }

    }

}
